"""Hourly Binance Alpha market snapshot collector.

Alpha observations intentionally live in their own SQLite database.  This
module does not import or write any trading database tables.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

import db_config

ALPHA_TOKEN_LIST_URL = os.getenv(
    "ALPHA_TOKEN_LIST_URL",
    # Keep this source aligned with collector.get_alpha_symbols(), which is
    # already used to construct the trading/scoring universe.
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
)
REQUEST_TIMEOUT_SECONDS = float(os.getenv("ALPHA_REQUEST_TIMEOUT_SECONDS", "20"))
BACKFILL_HOURS = 24


@dataclass(frozen=True)
class AlphaObservation:
    symbol: str
    name: str
    chain_id: str
    contract_address: str
    icon_url: str
    volume_24h: str | None
    market_cap: str | None
    observed_at: int


@dataclass(frozen=True)
class AlphaDatabaseStatus:
    """Operator-facing facts used to distinguish an empty DB from a wrong DB."""

    path: str
    size_bytes: int
    total_rows: int
    snapshot_count: int


def init_db(db_path: str = db_config.ALPHA_DB_PATH) -> None:
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                chain_id TEXT NOT NULL DEFAULT '',
                contract_address TEXT NOT NULL DEFAULT '',
                icon_url TEXT NOT NULL DEFAULT '',
                volume_24h TEXT,
                market_cap TEXT,
                observed_at INTEGER NOT NULL,
                UNIQUE(observed_at, chain_id, contract_address, symbol)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alpha_snapshot_latest "
            "ON alpha_market_snapshots(observed_at DESC, symbol)"
        )


def _decimal_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return format(number, "f") if number.is_finite() else None


def _payload_tokens(payload: Any) -> list[dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        data = data.get("tokens") or data.get("list")
    if not isinstance(data, list):
        raise ValueError("Binance Alpha response does not contain a token list")
    return [item for item in data if isinstance(item, dict)]


def fetch_tokens(session=requests) -> list[dict[str, Any]]:
    db_config.assert_no_active_sqlite_transaction("Binance Alpha token request")
    response = session.get(
        ALPHA_TOKEN_LIST_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return _payload_tokens(response.json())


def collect_snapshot(
    db_path: str = db_config.ALPHA_DB_PATH,
    *,
    session=requests,
    observed_at: int | None = None,
) -> int:
    """Fetch and atomically persist one complete Alpha-token snapshot."""
    tokens = fetch_tokens(session)
    timestamp = int(time.time() * 1000) if observed_at is None else int(observed_at)
    rows = []
    for token in tokens:
        symbol = str(token.get("symbol") or token.get("ticker") or "").strip()
        if not symbol:
            continue
        rows.append(
            (
                symbol,
                str(token.get("name") or "").strip(),
                str(token.get("chainId") or token.get("chain_id") or ""),
                str(token.get("contractAddress") or token.get("contract_address") or ""),
                str(token.get("iconUrl") or token.get("icon_url") or ""),
                _decimal_text(token.get("volume24h") or token.get("volume_24h")),
                _decimal_text(token.get("marketCap") or token.get("market_cap")),
                timestamp,
            )
        )
    if not rows:
        raise ValueError("Binance Alpha response contains no valid tokens")
    init_db(db_path)
    with db_config.connect_sqlite(db_path) as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO alpha_market_snapshots
               (symbol, name, chain_id, contract_address, icon_url,
                volume_24h, market_cap, observed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
    return len(rows)


def backfill_recent_hours(
    db_path: str = db_config.ALPHA_DB_PATH,
    *,
    session=requests,
    hours: int = BACKFILL_HOURS,
    now_ms: int | None = None,
) -> int:
    """Fill missing hourly buckets in the trailing window.

    The Alpha endpoint exposes the current token list (not historical candles),
    so one successful response is reused for missing buckets.  This keeps the
    observation timeline queryable after downtime without issuing 24 requests.
    Existing buckets are never overwritten.
    """
    if hours <= 0:
        return 0
    tokens = fetch_tokens(session)
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    hour_ms = 60 * 60 * 1000
    current_bucket = now // hour_ms * hour_ms
    init_db(db_path)
    with db_config.connect_sqlite(db_path) as conn:
        existing = {
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT observed_at FROM alpha_market_snapshots WHERE observed_at >= ?",
                (current_bucket - (hours - 1) * hour_ms,),
            )
        }
    inserted = 0
    for offset in range(hours):
        bucket = current_bucket - offset * hour_ms
        if bucket in existing:
            continue
        inserted += collect_snapshot(db_path, session=_StaticTokenSession(tokens), observed_at=bucket)
    return inserted


class _StaticTokenSession:
    """ requests-like adapter used by backfill to avoid repeated network calls. """
    def __init__(self, tokens):
        self.tokens = tokens
    def get(self, *args, **kwargs):
        tokens = self.tokens
        class Response:
            def raise_for_status(self): pass
            def json(inner): return {"data": tokens}
        return Response()


def latest_snapshot(db_path: str = db_config.ALPHA_DB_PATH) -> tuple[int | None, list[sqlite3.Row]]:
    init_db(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        latest = conn.execute(
            "SELECT MAX(observed_at) FROM alpha_market_snapshots"
        ).fetchone()[0]
        if latest is None:
            return None, []
        rows = conn.execute(
            """SELECT symbol, name, chain_id, contract_address, icon_url,
                      volume_24h, market_cap, observed_at,
                      CAST(volume_24h AS REAL) / NULLIF(CAST(market_cap AS REAL), 0)
                        AS activity
               FROM alpha_market_snapshots WHERE observed_at = ?
               ORDER BY activity IS NULL, activity DESC, symbol""",
            (latest,),
        ).fetchall()
    return int(latest), rows


def database_status(db_path: str = db_config.ALPHA_DB_PATH) -> AlphaDatabaseStatus:
    """Return non-sensitive diagnostics for the database actually being read."""
    init_db(db_path)
    path = Path(db_path).resolve()
    with db_config.connect_sqlite(db_path) as conn:
        total_rows, snapshot_count = conn.execute(
            """SELECT COUNT(*), COUNT(DISTINCT observed_at)
               FROM alpha_market_snapshots"""
        ).fetchone()
    return AlphaDatabaseStatus(
        path=str(path),
        size_bytes=path.stat().st_size,
        total_rows=int(total_rows),
        snapshot_count=int(snapshot_count),
    )
