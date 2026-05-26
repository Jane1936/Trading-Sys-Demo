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


@dataclass
class MACalcResult:
    symbol: str
    interval: str
    open_time: int
    close_time: int
    close: float
    ma20: Optional[float]


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
    """

    SUPPORTED_INTERVALS = ("5m", "15m", "1h")

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _table_name(interval: str) -> str:
        return f"klines_{interval}"

    def get_ma20_series(self, symbol: str, interval: str, limit: int = 200) -> List[MACalcResult]:
        if interval not in self.SUPPORTED_INTERVALS:
            raise ValueError(f"interval must be one of {self.SUPPORTED_INTERVALS}")

        table = self._table_name(interval)
        query_limit = max(20, limit + 19)

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

        for row in ordered:
            close_price = float(row["close"])
            closes.append(close_price)
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None

            series.append(
                MACalcResult(
                    symbol=row["symbol"],
                    interval=interval,
                    open_time=int(row["open_time"]),
                    close_time=int(row["close_time"]),
                    close=close_price,
                    ma20=ma20,
                )
            )

        return list(reversed(series[-limit:]))

    def get_latest_ma20(self, symbol: str, interval: str) -> Optional[MACalcResult]:
        series = self.get_ma20_series(symbol=symbol, interval=interval, limit=1)
        return series[0] if series else None

    def get_latest_multi_interval_ma20(self, symbol: str) -> Dict[str, Optional[MACalcResult]]:
        return {interval: self.get_latest_ma20(symbol, interval) for interval in self.SUPPORTED_INTERVALS}


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
            raise ValueError(f"interval must be one of {tuple(self.INTERVAL_SECONDS.keys())}")
        if now_ts is None:
            now_ts = int(time.time())
        sec = self.INTERVAL_SECONDS[interval]
        next_boundary = ((now_ts // sec) + 1) * sec
        return datetime.fromtimestamp(next_boundary + self.grace_seconds, tz=timezone.utc)


def run_loop(
    symbols: List[str],
    processor: MA20Processor,
    scheduler: MA20Scheduler,
    on_result: Callable[[MACalcResult], None],
    symbol_provider: Optional[Callable[[], List[str]]] = None,
    poll_seconds: int = 20,
) -> None:
    """Run MA20 calculation on schedule and dispatch latest results."""
    last_bar_open_time: Dict[str, Dict[str, int]] = {s: {} for s in symbols}

    while True:
        active_symbols = symbol_provider() if symbol_provider is not None else symbols
        for s in active_symbols:
            if s not in last_bar_open_time:
                last_bar_open_time[s] = {}

        due_intervals = scheduler.due_intervals()
        for symbol in active_symbols:
            for interval in due_intervals:
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
                last_bar_open_time[symbol][interval] = latest.open_time

        time.sleep(poll_seconds)


def init_ma20_table(db_path: str = "data/klines.db") -> None:
    """Create table for persisted MA20 values."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
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
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ma20_symbol_interval_time "
            "ON ma20_indicators(symbol, interval, open_time)"
        )


def save_ma20_result(db_path: str, result: MACalcResult) -> None:
    """Upsert one MA20 record to ma20_indicators table."""
    if result.ma20 is None:
        return

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(db_path) as conn:
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
