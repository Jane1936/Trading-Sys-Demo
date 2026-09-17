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
ALPHA_KLINE_URL = os.getenv("ALPHA_KLINE_URL", "https://api.binance.com/api/v3/klines")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("ALPHA_REQUEST_TIMEOUT_SECONDS", "20"))
BACKFILL_HOURS = 24
DAY_MS = 24 * 60 * 60 * 1000
# A daily sample normally arrives within a few seconds of the target time.  A
# wider window also accommodates restarts while preventing an old snapshot
# from being presented as current multi-day activity.
DAILY_SAMPLE_TOLERANCE_MS = 2 * 60 * 60 * 1000


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
        conn.execute("""CREATE TABLE IF NOT EXISTS alpha_daily_klines (
            symbol TEXT NOT NULL, open_time INTEGER NOT NULL, open REAL, high REAL,
            low REAL, close REAL, volume REAL, close_time INTEGER,
            PRIMARY KEY(symbol, open_time))""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alpha_snapshot_latest "
            "ON alpha_market_snapshots(observed_at DESC, symbol)"
        )

def collect_daily_klines(db_path: str = db_config.ALPHA_DB_PATH, *, session=requests,
                         symbols: list[str] | None = None, limit: int = 30) -> int:
    """Fetch and persist daily candles for Alpha symbols in the isolated DB."""
    if symbols is None:
        _, rows = latest_snapshot(db_path)
        symbols = [str(r["symbol"]).upper() for r in rows]
    init_db(db_path); inserted = 0
    with db_config.connect_sqlite(db_path) as conn:
        for symbol in symbols:
            market = symbol if symbol.endswith("USDT") else symbol + "USDT"
            # Alpha token lists contain tokens that are not necessarily Binance
            # spot symbols.  One invalid/temporarily unavailable token must not
            # abort the whole batch (the old behaviour left the table empty).
            try:
                response = session.get(ALPHA_KLINE_URL, params={"symbol": market, "interval": "1d", "limit": limit}, timeout=REQUEST_TIMEOUT_SECONDS)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                print(f"⚠️ Alpha daily kline skipped {symbol} ({market}): {exc}")
                continue
            candles = payload.get("data", payload) if isinstance(payload, dict) else payload
            if not isinstance(candles, list): continue
            for c in candles:
                if not isinstance(c, (list, tuple)) or len(c) < 7: continue
                conn.execute("INSERT OR REPLACE INTO alpha_daily_klines VALUES (?,?,?,?,?,?,?,?)",
                             (symbol, int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5]), int(c[6])))
                inserted += 1
    return inserted

def backfill_recent_daily_klines(db_path: str = db_config.ALPHA_DB_PATH, *, session=requests,
                                 days: int = 30) -> int:
    """Backfill the recent daily-candle window immediately.

    Binance returns the newest candles on every request, so this is safe to
    run at startup and hourly; ``INSERT OR REPLACE`` also repairs missed
    midnight runs without waiting for another day boundary.
    """
    return collect_daily_klines(db_path, session=session, limit=max(1, int(days)))

def daily_trends(db_path: str = db_config.ALPHA_DB_PATH, limit: int = 30) -> list[dict[str, Any]]:
    """Return recent price-trend metrics for each token.

    The three-day return covers the newest three available daily candles, from
    the oldest candle's open to the newest candle's close.  Tokens without
    three complete data points retain a ``None`` value and sort after tokens
    that have enough history.
    """
    init_db(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        symbols = [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM alpha_market_snapshots")]
        result = []
        for symbol in symbols:
            rows = conn.execute("SELECT open_time, open, close FROM alpha_daily_klines WHERE symbol=? ORDER BY open_time DESC LIMIT ?", (symbol, limit)).fetchall()
            ups = 0
            for row in rows:
                if row["close"] > row["open"]: ups += 1
                else: break
            ret = None
            if len(rows) >= 2 and rows[-1]["open"]:
                ret = (rows[0]["close"] / rows[-1]["open"]) - 1
            three_day_return = None
            if len(rows) >= 3 and rows[2]["open"]:
                three_day_return = (rows[0]["close"] / rows[2]["open"]) - 1
            result.append({
                "symbol": symbol,
                "consecutive_up_days": ups,
                "trend_return": ret,
                "three_day_return": three_day_return,
                "kline_count": len(rows),
            })
    return sorted(
        result,
        key=lambda item: (
            item["three_day_return"] is None,
            -(item["three_day_return"] or 0),
            item["symbol"],
        ),
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


def latest_snapshot(db_path: str = db_config.ALPHA_DB_PATH) -> tuple[int | None, list[dict[str, Any]]]:
    """Return the latest tokens with rolling one-to-five-day activity.

    Binance supplies a rolling 24-hour volume rather than historical daily
    candles.  For an N-day value we therefore add the latest 24-hour volume to
    the nearest snapshot at each preceding 24-hour boundary, then divide by
    the latest market cap.  A value is left unavailable if any required daily
    sample is missing.
    """
    init_db(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        latest = conn.execute(
            "SELECT MAX(observed_at) FROM alpha_market_snapshots"
        ).fetchone()[0]
        if latest is None:
            return None, []
        latest_rows = conn.execute(
            """SELECT symbol, name, chain_id, contract_address, icon_url,
                      volume_24h, market_cap, observed_at
               FROM alpha_market_snapshots WHERE observed_at = ?
               ORDER BY symbol""",
            (latest,),
        ).fetchall()
        history = conn.execute(
            """SELECT symbol, chain_id, contract_address, volume_24h, observed_at
               FROM alpha_market_snapshots
               WHERE observed_at >= ? AND observed_at < ?""",
            (latest - 4 * DAY_MS - DAILY_SAMPLE_TOLERANCE_MS, latest),
        ).fetchall()

    history_by_token: dict[tuple[str, str, str], list[sqlite3.Row]] = {}
    for row in history:
        key = (row["symbol"], row["chain_id"], row["contract_address"])
        history_by_token.setdefault(key, []).append(row)

    tokens = []
    for latest_row in latest_rows:
        token = dict(latest_row)
        try:
            market_cap = float(token["market_cap"])
            volumes: list[float] = [float(token["volume_24h"])]
            if market_cap == 0:
                raise ValueError
        except (TypeError, ValueError):
            market_cap, volumes = 0.0, []
        token["activity_1d"] = volumes[0] / market_cap if volumes else None

        key = (token["symbol"], token["chain_id"], token["contract_address"])
        candidates = history_by_token.get(key, [])
        for day in range(1, 5):
            target = latest - day * DAY_MS
            sample = min(
                candidates,
                key=lambda row: abs(row["observed_at"] - target),
                default=None,
            )
            if (
                sample is None
                or abs(sample["observed_at"] - target) > DAILY_SAMPLE_TOLERANCE_MS
            ):
                volumes = []
            if volumes:
                try:
                    volumes.append(float(sample["volume_24h"]))
                except (TypeError, ValueError):
                    volumes = []
            token[f"activity_{day + 1}d"] = (
                sum(volumes) / market_cap if len(volumes) == day + 1 else None
            )
        # Retain the old key for callers that still consume it.
        token["activity"] = token["activity_1d"]
        tokens.append(token)

    tokens.sort(
        key=lambda token: (
            token["activity_1d"] is None,
            -(token["activity_1d"] or 0),
            token["symbol"],
        )
    )
    return int(latest), tokens


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
