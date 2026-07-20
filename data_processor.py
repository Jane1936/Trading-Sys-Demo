"""Data processing module for technical indicators.

This module is intentionally decoupled from ``collector.py`` and only depends on
SQLite data shape (``klines_<interval>`` tables).
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import db_config


@dataclass
class MACalcResult:
    symbol: str
    interval: str
    open_time: int
    close_time: int
    close: float
    ma20: Optional[float]
    ema12: Optional[float] = None
    ema16: Optional[float] = None
    ema21: Optional[float] = None
    ema26: Optional[float] = None
    macd_dif: Optional[float] = None
    macd_dea: Optional[float] = None
    macd_histogram: Optional[float] = None


def _interval_to_ms(interval: str) -> int:
    mapping = {"5m": 5 * 60_000, "15m": 15 * 60_000, "1h": 60 * 60_000}
    if interval not in mapping:
        raise ValueError(f"Unsupported interval: {interval}")
    return mapping[interval]


def _is_closed_bar_for_interval(result: MACalcResult) -> bool:
    interval_ms = _interval_to_ms(result.interval)
    expected_close_time = result.open_time + interval_ms - 1
    return result.close_time == expected_close_time


class MA20Processor:
    """Calculate MA20 for multiple intervals based on stored kline close prices.

    MA20 definition:
        average close of latest 20 candles

    15m EMA definition:
        pandas ewm(span=N, adjust=False).mean() on close prices, for N=12, 16, 21, and 26

    15m MACD definition:
        DIF = EMA12 - EMA26, DEA = EMA9(DIF), MACD histogram = 2 * (DIF - DEA).
        All EMA calculations use adjust=False semantics.
    """

    SUPPORTED_INTERVALS = ("5m", "15m", "1h")

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = db_config.connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        return conn

    @staticmethod
    def _table_name(interval: str) -> str:
        return f"klines_{interval}"

    def get_ma20_series(
        self, symbol: str, interval: str, limit: int = 200
    ) -> List[MACalcResult]:
        if interval not in self.SUPPORTED_INTERVALS:
            raise ValueError(f"interval must be one of {self.SUPPORTED_INTERVALS}")

        table = self._table_name(interval)
        query_limit = max(20, limit + 19)
        if interval == "15m":
            # EMA/MACD(adjust=False) are recursive; keep a wider warm-up window than
            # MA20 while preserving the existing bounded-query processing pattern.
            query_limit = max(200, limit + 199)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, open_time, close_time, close
                FROM {table}
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT ?
                """,
                (symbol, query_limit),
            ).fetchall()

        if not rows:
            return []

        ordered = list(reversed(rows))
        closes: List[float] = []
        series: List[MACalcResult] = []
        ema12: Optional[float] = None
        ema16: Optional[float] = None
        ema21: Optional[float] = None
        ema26: Optional[float] = None
        macd_dea: Optional[float] = None
        macd_dif: Optional[float] = None
        macd_histogram: Optional[float] = None
        ema12_alpha = 2 / (12 + 1)
        ema16_alpha = 2 / (16 + 1)
        ema21_alpha = 2 / (21 + 1)
        ema26_alpha = 2 / (26 + 1)
        macd_dea_alpha = 2 / (9 + 1)

        for row in ordered:
            close_price = float(row["close"])
            closes.append(close_price)
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None

            if interval == "15m":
                ema12 = (
                    close_price
                    if ema12 is None
                    else (close_price * ema12_alpha) + (ema12 * (1 - ema12_alpha))
                )
                ema16 = (
                    close_price
                    if ema16 is None
                    else (close_price * ema16_alpha) + (ema16 * (1 - ema16_alpha))
                )
                ema21 = (
                    close_price
                    if ema21 is None
                    else (close_price * ema21_alpha) + (ema21 * (1 - ema21_alpha))
                )
                ema26 = (
                    close_price
                    if ema26 is None
                    else (close_price * ema26_alpha) + (ema26 * (1 - ema26_alpha))
                )
                macd_dif = ema12 - ema26
                macd_dea = (
                    macd_dif
                    if macd_dea is None
                    else (macd_dif * macd_dea_alpha) + (macd_dea * (1 - macd_dea_alpha))
                )
                macd_histogram = 2 * (macd_dif - macd_dea)

            series.append(
                MACalcResult(
                    symbol=row["symbol"],
                    interval=interval,
                    open_time=int(row["open_time"]),
                    close_time=int(row["close_time"]),
                    close=close_price,
                    ma20=ma20,
                    ema12=ema12 if interval == "15m" else None,
                    ema16=ema16 if interval == "15m" else None,
                    ema21=ema21 if interval == "15m" else None,
                    ema26=ema26 if interval == "15m" else None,
                    macd_dif=macd_dif if interval == "15m" else None,
                    macd_dea=macd_dea if interval == "15m" else None,
                    macd_histogram=macd_histogram if interval == "15m" else None,
                )
            )

        return list(reversed(series[-limit:]))

    def get_latest_ma20(self, symbol: str, interval: str) -> Optional[MACalcResult]:
        series = self.get_ma20_series(symbol=symbol, interval=interval, limit=1)
        return series[0] if series else None

    def get_latest_multi_interval_ma20(
        self, symbol: str
    ) -> Dict[str, Optional[MACalcResult]]:
        return {
            interval: self.get_latest_ma20(symbol, interval)
            for interval in self.SUPPORTED_INTERVALS
        }


class MA20Scheduler:
    """Simple timer for MA20 processing jobs.

    Trigger rule:
    - 5m  MA20: run at hh:mm where mm % 5 == 0 + ``grace_seconds``
    - 15m MA20: run at hh:00/15/30/45 + ``grace_seconds``
    - 1h  MA20: run at hh:00 + ``grace_seconds``
    """

    INTERVAL_SECONDS = {"5m": 5 * 60, "15m": 15 * 60, "1h": 60 * 60}

    def __init__(self, grace_seconds: int = 5) -> None:
        self.grace_seconds = grace_seconds

    @staticmethod
    def _latest_closed_bar_open_ts(now_ts: int, interval_seconds: int) -> int:
        return (now_ts // interval_seconds) * interval_seconds - interval_seconds

    def due_intervals(self, now_ts: Optional[int] = None) -> List[str]:
        """Compatibility API: return all managed intervals.

        The run loop is data-driven (checks whether a new aggregated candle exists),
        so we do not hard-rely on wall-clock windows to avoid conflicts with
        collector write latency.
        """
        _ = now_ts
        return list(self.INTERVAL_SECONDS.keys())

    def next_run_at(self, interval: str, now_ts: Optional[int] = None) -> datetime:
        if interval not in self.INTERVAL_SECONDS:
            raise ValueError(
                f"interval must be one of {tuple(self.INTERVAL_SECONDS.keys())}"
            )
        if now_ts is None:
            now_ts = int(time.time())
        sec = self.INTERVAL_SECONDS[interval]
        next_boundary = ((now_ts // sec) + 1) * sec
        return datetime.fromtimestamp(
            next_boundary + self.grace_seconds, tz=timezone.utc
        )


def run_loop(
    symbols: List[str],
    processor: MA20Processor,
    scheduler: MA20Scheduler,
    on_result: Callable[[MACalcResult], None],
    symbol_provider: Optional[Callable[[], List[str]]] = None,
    poll_seconds: int = 20,
    on_interval_complete: Optional[Callable[[str, List[MACalcResult]], None]] = None,
) -> None:
    """Run MA20/EMA calculation and dispatch MACD only after each interval round."""
    last_bar_open_time: Dict[str, Dict[str, int]] = {s: {} for s in symbols}

    while True:
        active_symbols = symbol_provider() if symbol_provider is not None else symbols
        for s in active_symbols:
            if s not in last_bar_open_time:
                last_bar_open_time[s] = {}

        due_intervals = scheduler.due_intervals()
        for interval in due_intervals:
            interval_results: List[MACalcResult] = []
            for symbol in active_symbols:
                latest = processor.get_latest_ma20(symbol, interval)
                if latest is None or latest.ma20 is None:
                    continue

                # Explicit trigger rule for 5m/15m/1h: new and closed aggregated candle only.
                if not _is_closed_bar_for_interval(latest):
                    continue

                last_done = last_bar_open_time[symbol].get(interval)
                if last_done == latest.open_time:
                    continue

                on_result(latest)
                interval_results.append(latest)
                last_bar_open_time[symbol][interval] = latest.open_time

            if interval_results and on_interval_complete is not None:
                try:
                    on_interval_complete(interval, interval_results)
                except Exception as exc:
                    print(
                        "⚠️ indicator interval-complete callback failed "
                        f"interval={interval}: {exc}"
                    )

        time.sleep(poll_seconds)


def init_ma20_table(db_path: str = "data/klines.db") -> None:
    """Create table for persisted MA20 values."""
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ma20_indicators (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER NOT NULL,
                    close REAL NOT NULL,
                    ma20 REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, interval, open_time)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ma20_symbol_interval_time "
                "ON ma20_indicators(symbol, interval, open_time)"
            )


def init_ema_table(db_path: str = "data/klines.db") -> None:
    """Create table for persisted EMA12/EMA16/EMA21/EMA26 15m values."""
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ema_indicators (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER NOT NULL,
                    close REAL NOT NULL,
                    ema12 REAL,
                    ema16 REAL NOT NULL,
                    ema21 REAL NOT NULL,
                    ema26 REAL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, interval, open_time)
                )
            """)
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(ema_indicators)").fetchall()
            }
            if "ema12" not in columns:
                conn.execute("ALTER TABLE ema_indicators ADD COLUMN ema12 REAL")
            if "ema26" not in columns:
                conn.execute("ALTER TABLE ema_indicators ADD COLUMN ema26 REAL")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ema_symbol_interval_time "
                "ON ema_indicators(symbol, interval, open_time)"
            )


def save_ma20_result(db_path: str, result: MACalcResult) -> None:
    """Upsert one MA20 record to ma20_indicators table."""
    if result.ma20 is None:
        return

    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute(
            """
            INSERT INTO ma20_indicators
            (symbol, interval, open_time, close_time, close, ma20, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
                close_time=excluded.close_time,
                close=excluded.close,
                ma20=excluded.ma20,
                updated_at=excluded.updated_at
            """,
            (
                result.symbol,
                result.interval,
                result.open_time,
                result.close_time,
                result.close,
                result.ma20,
                now_ms,
            ),
        )


def save_ema_result(db_path: str, result: MACalcResult) -> None:
    """Upsert one 15m EMA12/EMA16/EMA21/EMA26 record to ema_indicators table."""
    if (
        result.interval != "15m"
        or result.ema12 is None
        or result.ema16 is None
        or result.ema21 is None
        or result.ema26 is None
    ):
        return

    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute(
            """
            INSERT INTO ema_indicators
            (symbol, interval, open_time, close_time, close, ema12, ema16, ema21, ema26, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
                close_time=excluded.close_time,
                close=excluded.close,
                ema12=excluded.ema12,
                ema16=excluded.ema16,
                ema21=excluded.ema21,
                ema26=excluded.ema26,
                updated_at=excluded.updated_at
            """,
            (
                result.symbol,
                result.interval,
                result.open_time,
                result.close_time,
                result.close,
                result.ema12,
                result.ema16,
                result.ema21,
                result.ema26,
                now_ms,
            ),
        )


def init_macd_table(db_path: str = "data/klines.db") -> None:
    """Create table for persisted MACD 15m values."""
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS macd_indicators (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER NOT NULL,
                    close REAL NOT NULL,
                    dif REAL NOT NULL,
                    dea REAL NOT NULL,
                    macd REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, interval, open_time)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_macd_symbol_interval_time "
                "ON macd_indicators(symbol, interval, open_time)"
            )


def save_macd_result(db_path: str, result: MACalcResult) -> None:
    """Upsert one 15m MACD record to macd_indicators table."""
    if (
        result.interval != "15m"
        or result.macd_dif is None
        or result.macd_dea is None
        or result.macd_histogram is None
    ):
        return

    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute(
            """
            INSERT INTO macd_indicators
            (symbol, interval, open_time, close_time, close, dif, dea, macd, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
                close_time=excluded.close_time,
                close=excluded.close,
                dif=excluded.dif,
                dea=excluded.dea,
                macd=excluded.macd,
                updated_at=excluded.updated_at
            """,
            (
                result.symbol,
                result.interval,
                result.open_time,
                result.close_time,
                result.close,
                result.macd_dif,
                result.macd_dea,
                result.macd_histogram,
                now_ms,
            ),
        )
