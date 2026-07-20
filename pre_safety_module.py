"""Pre-trade safety module for abnormal wick (pin) detection.

Scan rule (per 15-minute decision round):
- Read the latest 3 closed 5m candles for each symbol in the current 15-minute round.
- For each candle among the first/second/third 5m candle (oldest -> newest),
  if any candle satisfies all of the following:
  1) upper or lower wick length / (high - low) >= 0.7
  2) (high - low) / open >= 0.015
  3) the same upper or lower wick length >= abs(close - open) * 2.5
  then record the symbol as an abnormal wick symbol for this round.
"""

from __future__ import annotations

import math
import os
import sqlite3
import db_config
import time
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Candle5m:
    symbol: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float


@dataclass
class AbnormalWickEvent:
    symbol: str
    decision_round_ts: int
    candle_index: int
    candle_index_open_time: int
    candle_index_close_time: int
    open: float
    high: float
    low: float
    close: float
    cond1_ratio: float
    cond2_ratio: float
    cond3_ratio: float
    detected_at: int

    @property
    def first_candle_open_time(self) -> int:
        return self.candle_index_open_time

    @property
    def first_candle_close_time(self) -> int:
        return self.candle_index_close_time


class PreSafetyModule:
    """Detect and persist abnormal wick events for each 15m decision round."""

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = db_config.connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH)])
        return conn

    def init_table(self) -> None:
        with db_config.sqlite_schema_lock(self.db_path):
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS abnormal_wick_events (
                        symbol TEXT NOT NULL,
                        decision_round_ts INTEGER NOT NULL,
                        candle_index INTEGER NOT NULL,
                        first_candle_open_time INTEGER NOT NULL,
                        first_candle_close_time INTEGER NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        cond1_ratio REAL NOT NULL,
                        cond2_ratio REAL NOT NULL,
                        cond3_ratio REAL NOT NULL DEFAULT 0,
                        detected_at INTEGER NOT NULL,
                        PRIMARY KEY (symbol, decision_round_ts, candle_index)
                    )
                    """
                )
                self._migrate_table_if_needed(conn)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_abnormal_wick_detected_at "
                    "ON abnormal_wick_events(detected_at DESC)"
                )

    @staticmethod
    def _migrate_table_if_needed(conn: sqlite3.Connection) -> None:
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(abnormal_wick_events)").fetchall()
        }
        if "candle_index" in cols:
            if "cond3_ratio" not in cols:
                conn.execute("ALTER TABLE abnormal_wick_events ADD COLUMN cond3_ratio REAL NOT NULL DEFAULT 0")
            return

        conn.execute("ALTER TABLE abnormal_wick_events RENAME TO abnormal_wick_events_old")
        conn.execute(
            """
            CREATE TABLE abnormal_wick_events (
                symbol TEXT NOT NULL,
                decision_round_ts INTEGER NOT NULL,
                candle_index INTEGER NOT NULL,
                first_candle_open_time INTEGER NOT NULL,
                first_candle_close_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                cond1_ratio REAL NOT NULL,
                cond2_ratio REAL NOT NULL,
                cond3_ratio REAL NOT NULL DEFAULT 0,
                detected_at INTEGER NOT NULL,
                PRIMARY KEY (symbol, decision_round_ts, candle_index)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO abnormal_wick_events (
                symbol, decision_round_ts, candle_index,
                first_candle_open_time, first_candle_close_time,
                open, high, low, close,
                cond1_ratio, cond2_ratio, cond3_ratio,
                detected_at
            )
            SELECT
                symbol, decision_round_ts, 1,
                first_candle_open_time, first_candle_close_time,
                open, high, low, close,
                cond1_ratio, cond2_ratio, 0,
                detected_at
            FROM abnormal_wick_events_old
            """
        )
        conn.execute("DROP TABLE abnormal_wick_events_old")

    def _get_latest_3_closed_5m(self, symbol: str) -> List[Candle5m]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, open_time, close_time, open, high, low, close
                FROM klines_5m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 3
                """,
                (symbol,),
            ).fetchall()

        candles = [
            Candle5m(
                symbol=row["symbol"],
                open_time=int(row["open_time"]),
                close_time=int(row["close_time"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
            )
            for row in rows
        ]
        return list(reversed(candles))

    @staticmethod
    def _decision_round_ts_ms(now_ms: Optional[int] = None) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        round_ms = 15 * 60_000
        return (now_ms // round_ms) * round_ms

    @staticmethod
    def _is_abnormal(candle: Candle5m) -> tuple[bool, float, float, float]:
        span = candle.high - candle.low
        if span <= 0 or candle.open <= 0:
            return False, math.inf, math.inf, math.inf

        upper_wick = candle.high - max(candle.open, candle.close)
        lower_wick = min(candle.open, candle.close) - candle.low
        upper_wick_ratio = upper_wick / span
        lower_wick_ratio = lower_wick / span
        cond1_ratio = max(upper_wick_ratio, lower_wick_ratio)

        cond2_ratio = span / candle.open
        body = abs(candle.close - candle.open)
        same_direction_wick = upper_wick if upper_wick_ratio >= lower_wick_ratio else lower_wick
        cond3_ratio = math.inf if body == 0 and same_direction_wick > 0 else (same_direction_wick / body if body > 0 else 0.0)
        upper_hit = upper_wick_ratio >= 0.7 and upper_wick >= body * 2.5
        lower_hit = lower_wick_ratio >= 0.7 and lower_wick >= body * 2.5
        is_hit = (upper_hit or lower_hit) and cond2_ratio >= 0.015
        return is_hit, cond1_ratio, cond2_ratio, cond3_ratio

    def detect_for_symbol(self, symbol: str, now_ms: Optional[int] = None) -> List[AbnormalWickEvent]:
        candles = self._get_latest_3_closed_5m(symbol)
        if len(candles) < 3:
            return []

        events: List[AbnormalWickEvent] = []
        decision_round_ts = self._decision_round_ts_ms(now_ms=now_ms)
        detected_at = int(time.time() * 1000)

        for idx, candle in enumerate(candles, start=1):
            hit, cond1_ratio, cond2_ratio, cond3_ratio = self._is_abnormal(candle)
            if not hit:
                continue

            event = AbnormalWickEvent(
                symbol=symbol,
                decision_round_ts=decision_round_ts,
                candle_index=idx,
                candle_index_open_time=candle.open_time,
                candle_index_close_time=candle.close_time,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                cond1_ratio=cond1_ratio,
                cond2_ratio=cond2_ratio,
                cond3_ratio=cond3_ratio,
                detected_at=detected_at,
            )
            self._save_event(event)
            events.append(event)

        return events

    def _save_event(self, event: AbnormalWickEvent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO abnormal_wick_events (
                    symbol, decision_round_ts,
                    candle_index,
                    first_candle_open_time, first_candle_close_time,
                    open, high, low, close,
                    cond1_ratio, cond2_ratio, cond3_ratio,
                    detected_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts, candle_index) DO UPDATE SET
                    first_candle_open_time=excluded.first_candle_open_time,
                    first_candle_close_time=excluded.first_candle_close_time,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    cond1_ratio=excluded.cond1_ratio,
                    cond2_ratio=excluded.cond2_ratio,
                    cond3_ratio=excluded.cond3_ratio,
                    detected_at=excluded.detected_at
                """,
                (
                    event.symbol,
                    event.decision_round_ts,
                    event.candle_index,
                    event.candle_index_open_time,
                    event.candle_index_close_time,
                    event.open,
                    event.high,
                    event.low,
                    event.close,
                    event.cond1_ratio,
                    event.cond2_ratio,
                    event.cond3_ratio,
                    event.detected_at,
                ),
            )

    def get_recent_events(self, limit: int = 50, since_ms: Optional[int] = None) -> List[AbnormalWickEvent]:
        where_clause = "" if since_ms is None else "WHERE detected_at >= ?"
        params = (limit,) if since_ms is None else (int(since_ms), limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, decision_round_ts,
                       candle_index,
                       first_candle_open_time, first_candle_close_time,
                       open, high, low, close,
                       cond1_ratio, cond2_ratio, cond3_ratio,
                       detected_at
                FROM abnormal_wick_events
                {where_clause}
                ORDER BY detected_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [
            AbnormalWickEvent(
                symbol=row["symbol"],
                decision_round_ts=int(row["decision_round_ts"]),
                candle_index=int(row["candle_index"]),
                candle_index_open_time=int(row["first_candle_open_time"]),
                candle_index_close_time=int(row["first_candle_close_time"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                cond1_ratio=float(row["cond1_ratio"]),
                cond2_ratio=float(row["cond2_ratio"]),
                cond3_ratio=float(row["cond3_ratio"]),
                detected_at=int(row["detected_at"]),
            )
            for row in rows
        ]

    def get_recent_events_by_symbol(
        self, symbol: str, limit: int = 50, since_ms: Optional[int] = None
    ) -> List[AbnormalWickEvent]:
        time_clause = "" if since_ms is None else "AND detected_at >= ?"
        params = (symbol, limit) if since_ms is None else (symbol, int(since_ms), limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, decision_round_ts,
                       candle_index,
                       first_candle_open_time, first_candle_close_time,
                       open, high, low, close,
                       cond1_ratio, cond2_ratio, cond3_ratio,
                       detected_at
                FROM abnormal_wick_events
                WHERE symbol = ?
                {time_clause}
                ORDER BY detected_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [
            AbnormalWickEvent(
                symbol=row["symbol"],
                decision_round_ts=int(row["decision_round_ts"]),
                candle_index=int(row["candle_index"]),
                candle_index_open_time=int(row["first_candle_open_time"]),
                candle_index_close_time=int(row["first_candle_close_time"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                cond1_ratio=float(row["cond1_ratio"]),
                cond2_ratio=float(row["cond2_ratio"]),
                cond3_ratio=float(row["cond3_ratio"]),
                detected_at=int(row["detected_at"]),
            )
            for row in rows
        ]

    def get_event_symbols(self, since_ms: Optional[int] = None) -> List[str]:
        where_clause = "" if since_ms is None else "WHERE detected_at >= ?"
        params = () if since_ms is None else (int(since_ms),)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT symbol
                FROM abnormal_wick_events
                {where_clause}
                ORDER BY symbol ASC
                """,
                params,
            ).fetchall()
        return [str(row["symbol"]) for row in rows]

    def get_latest_round_abnormal_symbols(self, decision_round_ts: int | None = None) -> tuple[int | None, List[str]]:
        """Return symbols detected in a decision round.

        If ``decision_round_ts`` is None, use the latest round that exists in DB.
        """
        with self._connect() as conn:
            latest_round_ts = decision_round_ts
            if latest_round_ts is None:
                latest_round_row = conn.execute(
                    """
                    SELECT MAX(decision_round_ts) AS latest_round_ts
                    FROM abnormal_wick_events
                    """
                ).fetchone()
                latest_round_ts = latest_round_row["latest_round_ts"]
            if latest_round_ts is None:
                return None, []

            rows = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM abnormal_wick_events
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (int(latest_round_ts),),
            ).fetchall()
        return int(latest_round_ts), [str(row["symbol"]) for row in rows]


def render_events_html(events: List[AbnormalWickEvent]) -> str:
    """Render a simple HTML table for web display."""
    header = (
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<thead><tr>"
        "<th>symbol</th><th>decision_round_ts</th><th>candle_index</th><th>candle_index_open_time</th>"
        "<th>candle_index_open</th><th>candle_index_high</th><th>candle_index_low</th><th>candle_index_close</th>"
        "<th>长上/下影占比</th><th>振幅度大小</th><th>同方向长影/实体 ratio</th><th>是否≥2.5倍</th><th>detected_at</th>"
        "</tr></thead><tbody>"
    )
    rows = "".join(
        "<tr>"
        f"<td>{e.symbol}</td>"
        f"<td>{e.decision_round_ts}</td>"
        f"<td>{e.candle_index}</td>"
        f"<td>{e.first_candle_open_time}</td>"
        f"<td>{e.open:.6f}</td>"
        f"<td>{e.high:.6f}</td>"
        f"<td>{e.low:.6f}</td>"
        f"<td>{e.close:.6f}</td>"
        f"<td>{e.cond1_ratio:.6f}</td>"
        f"<td>{e.cond2_ratio:.6f}</td>"
        f"<td>{e.cond3_ratio:.6f}</td>"
        f"<td>{'是' if e.cond3_ratio >= 2.5 else '否'}</td>"
        f"<td>{e.detected_at}</td>"
        "</tr>"
        for e in events
    )
    return f"{header}{rows}</tbody></table>"
