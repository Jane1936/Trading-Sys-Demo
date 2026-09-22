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
ALPHA_KLINE_URL = os.getenv(
    "ALPHA_KLINE_URL",
    "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines",
)
SPOT_KLINE_URL = os.getenv("ALPHA_SPOT_KLINE_URL", "https://api.binance.com/api/v3/klines")
FUTURES_KLINE_URL = os.getenv("ALPHA_FUTURES_KLINE_URL", "https://fapi.binance.com/fapi/v1/klines")
FUTURES_OI_URL = os.getenv("ALPHA_FUTURES_OI_URL", "https://fapi.binance.com/fapi/v1/openInterest")
FUTURES_PREMIUM_URL = os.getenv("ALPHA_FUTURES_PREMIUM_URL", "https://fapi.binance.com/fapi/v1/premiumIndex")
FUTURES_EXCHANGE_INFO_URL = os.getenv("ALPHA_FUTURES_EXCHANGE_INFO_URL", "https://fapi.binance.com/fapi/v1/exchangeInfo")
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
                alpha_id TEXT NOT NULL DEFAULT '',
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
        snapshot_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(alpha_market_snapshots)")
        }
        if "alpha_id" not in snapshot_columns:
            conn.execute(
                "ALTER TABLE alpha_market_snapshots "
                "ADD COLUMN alpha_id TEXT NOT NULL DEFAULT ''"
            )
        conn.execute("""CREATE TABLE IF NOT EXISTS alpha_daily_klines (
            symbol TEXT NOT NULL, open_time INTEGER NOT NULL, open REAL, high REAL,
            low REAL, close REAL, volume REAL, close_time INTEGER,
            PRIMARY KEY(symbol, open_time))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS alpha_hourly_market (
            symbol TEXT NOT NULL, open_time INTEGER NOT NULL, close REAL,
            open_interest REAL, funding_rate REAL, PRIMARY KEY(symbol, open_time))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alpha_hourly_market_time ON alpha_hourly_market(open_time)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alpha_snapshot_latest "
            "ON alpha_market_snapshots(observed_at DESC, symbol)"
        )

def collect_daily_klines(db_path: str = db_config.ALPHA_DB_PATH, *, session=requests,
                         symbols: list[str | dict[str, Any]] | None = None,
                         limit: int = 30) -> int:
    """Fetch and persist daily candles for Alpha symbols in the isolated DB."""
    if symbols is None:
        _, rows = latest_snapshot(db_path)
        symbols = rows
    init_db(db_path); inserted = 0
    with db_config.connect_sqlite(db_path) as conn:
        for item in symbols:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").strip()
                alpha_id = str(item.get("alpha_id") or item.get("alphaId") or "").strip()
            else:
                symbol = str(item).strip().upper()
                alpha_id = ""
            if not symbol:
                continue
            # The Alpha endpoint requires the token-list ``alphaId`` rather
            # than the display ticker (for example ``APPon``). Keep the spot
            # fallback only for legacy rows that predate alpha_id persistence.
            identifier = alpha_id or symbol.upper()
            market = identifier if identifier.upper().endswith("USDT") else identifier + "USDT"
            url = ALPHA_KLINE_URL if alpha_id else SPOT_KLINE_URL
            try:
                response = session.get(url, params={"symbol": market, "interval": "1d", "limit": limit}, timeout=REQUEST_TIMEOUT_SECONDS)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                print(f"⚠️ Alpha daily kline skipped {symbol} ({market}): {exc}")
                continue
            candles = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(candles, list): continue
            for c in candles:
                if not isinstance(c, (list, tuple)) or len(c) < 7: continue
                try:
                    values = (symbol, int(c[0]), float(c[1]), float(c[2]),
                              float(c[3]), float(c[4]), float(c[5]), int(c[6]))
                except (TypeError, ValueError, OverflowError):
                    # A malformed candle must not prevent other candles (or
                    # other Alpha tokens) from being persisted.
                    continue
                conn.execute("INSERT OR REPLACE INTO alpha_daily_klines VALUES (?,?,?,?,?,?,?,?)", values)
                inserted += 1
    return inserted

def collect_hourly_market_data(db_path: str = db_config.ALPHA_DB_PATH, *, session=requests) -> int:
    """Collect one-hour futures OI, funding and close data in alpha.db."""
    init_db(db_path)
    _, rows = latest_snapshot(db_path)
    alpha_symbols = {str(r["symbol"]).upper().removesuffix("USDT") for r in rows}
    # Restrict collection to the true Alpha/U-margined intersection.  The
    # Alpha endpoint contains spot-only tokens, so attempting every symbol
    # would pollute alpha_hourly_market with unrelated records.
    try:
        payload = session.get(FUTURES_EXCHANGE_INFO_URL, timeout=REQUEST_TIMEOUT_SECONDS).json()
        contracts = payload.get("symbols", []) if isinstance(payload, dict) else []
        um_symbols = {str(item.get("symbol", "")).upper().removesuffix("USDT")
                      for item in contracts
                      if item.get("status") == "TRADING" and str(item.get("symbol", "")).upper().endswith("USDT")
                      and item.get("contractType", "PERPETUAL") in ("PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER")}
        symbols = sorted(alpha_symbols & um_symbols) if um_symbols else sorted(alpha_symbols)
    except Exception as exc:
        print(f"⚠️ Futures exchange info unavailable; using Alpha symbols: {exc}")
        symbols = sorted(alpha_symbols)
    hour = (int(time.time() * 1000) // 3_600_000) * 3_600_000
    saved = 0
    with db_config.connect_sqlite(db_path) as conn:
        for symbol in symbols:
            market = symbol + "USDT"
            try:
                oi = session.get(FUTURES_OI_URL, params={"symbol": market}, timeout=REQUEST_TIMEOUT_SECONDS).json().get("openInterest")
                premium = session.get(FUTURES_PREMIUM_URL, params={"symbol": market}, timeout=REQUEST_TIMEOUT_SECONDS).json()
                funding = premium.get("lastFundingRate") if isinstance(premium, dict) else None
                candles = session.get(FUTURES_KLINE_URL, params={"symbol": market, "interval": "1h", "limit": 2}, timeout=REQUEST_TIMEOUT_SECONDS).json()
                candle = candles[-1] if isinstance(candles, list) and candles else None
                close = float(candle[4]) if candle and len(candle) > 4 else None
                open_time = int(candle[0]) if candle and len(candle) > 0 else hour
                conn.execute("INSERT OR REPLACE INTO alpha_hourly_market VALUES (?,?,?,?,?)", (symbol, open_time, close, float(oi) if oi is not None else None, float(funding) if funding is not None else None))
                saved += 1
            except Exception as exc:
                print(f"⚠️ Alpha hourly market skipped {market}: {exc}")
    return saved

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

    The three- and five-day returns cover the newest three or five available
    daily candles, respectively, from the oldest candle's open to the newest
    candle's close. Tokens without enough complete data points retain a
    ``None`` value for that metric.
    """
    init_db(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        # Use the latest market snapshot as the source of truth for the
        # returned universe.  Previously this query was driven by
        # ``alpha_daily_klines`` implicitly (symbols with no valid Binance
        # spot pair simply disappeared), which made the trend module look as
        # if those tokens had been lost even though they were present in the
        # market snapshot.  Keep them with a zero-length data set so the UI
        # can distinguish "no kline data" from "not in the Alpha universe".
        latest_snapshot_at = conn.execute(
            "SELECT MAX(observed_at) FROM alpha_market_snapshots"
        ).fetchone()[0]
        if latest_snapshot_at is None:
            return []
        symbols = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT symbol FROM alpha_market_snapshots WHERE observed_at = ?",
                (latest_snapshot_at,),
            )
        ]
        result = []
        for symbol in symbols:
            rows = conn.execute("SELECT open_time, open, close FROM alpha_daily_klines WHERE symbol=? ORDER BY open_time DESC LIMIT ?", (symbol, limit)).fetchall()
            ups = 0
            for row in rows:
                if row["close"] > row["open"]: ups += 1
                else: break
            # Unlike the consecutive-day metric above, this counts every
            # rising candle in the latest five available daily candles.
            recent_five_rows = rows[:5]
            recent_five_up_days = sum(
                1 for row in recent_five_rows if row["close"] > row["open"]
            )
            three_day_return = None
            if len(rows) >= 3 and rows[2]["open"]:
                three_day_return = (rows[0]["close"] / rows[2]["open"]) - 1
            five_day_return = None
            if len(rows) >= 5 and rows[4]["open"]:
                five_day_return = (rows[0]["close"] / rows[4]["open"]) - 1
            result.append({
                "symbol": symbol,
                "consecutive_up_days": ups,
                "recent_five_up_days": recent_five_up_days,
                "three_day_return": three_day_return,
                "five_day_return": five_day_return,
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
                str(token.get("alphaId") or token.get("alpha_id") or "").strip(),
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
               (symbol, alpha_id, name, chain_id, contract_address, icon_url,
                volume_24h, market_cap, observed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
    """Return the latest tokens with one-to-five-day activity.

    Multi-day columns represent individual natural days, rather than a
    cumulative rolling window.  A snapshot at a day's midnight contains the
    preceding 24-hour volume and that midnight's market cap, which are used
    directly for the corresponding activity ratio.
    """
    init_db(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        latest = conn.execute(
            "SELECT MAX(observed_at) FROM alpha_market_snapshots"
        ).fetchone()[0]
        if latest is None:
            return None, []
        latest_rows = conn.execute(
            """SELECT symbol, alpha_id, name, chain_id, contract_address, icon_url,
                      volume_24h, market_cap, observed_at
               FROM alpha_market_snapshots WHERE observed_at = ?
               ORDER BY symbol""",
            (latest,),
        ).fetchall()
        history = conn.execute(
            """SELECT symbol, chain_id, contract_address, volume_24h, market_cap, observed_at
               FROM alpha_market_snapshots
               WHERE observed_at >= ? AND observed_at <= ?""",
            (latest - 4 * DAY_MS - DAILY_SAMPLE_TOLERANCE_MS, latest + DAILY_SAMPLE_TOLERANCE_MS),
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
        # The latest snapshot may be taken at any time during today.  Natural
        # day boundaries are UTC midnights (the timestamp convention used by
        # the collector); day 2 is the day ending at today's midnight.
        today_midnight = (latest // DAY_MS) * DAY_MS
        for day in range(1, 5):
            target = today_midnight - (day - 1) * DAY_MS
            sample = min(
                candidates,
                key=lambda row: abs(row["observed_at"] - target),
                default=None,
            )
            if sample is None or abs(sample["observed_at"] - target) > DAILY_SAMPLE_TOLERANCE_MS:
                token[f"activity_{day + 1}d"] = None
                continue
            try:
                daily_volume = float(sample["volume_24h"])
                boundary_market_cap = float(sample["market_cap"])
                token[f"activity_{day + 1}d"] = (
                    daily_volume / boundary_market_cap
                    if boundary_market_cap != 0
                    else None
                )
            except (TypeError, ValueError):
                token[f"activity_{day + 1}d"] = None
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
