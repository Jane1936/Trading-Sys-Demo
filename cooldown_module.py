"""Current decision-round cooldown symbol detector.

Cooldown rule (per 15-minute decision round):
- Symbol has an abnormal wick event in ``abnormal_wick_events`` within the
  latest 30 minutes; OR
- The latest two closed 15m candles both satisfy
  ``(high - close) / (high - low) >= 0.7`` and ``close > open``.

Only symbols that hit at least one cooldown condition are persisted.
"""

from __future__ import annotations

import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional


@dataclass
class CooldownSymbol:
    symbol: str
    decision_round_ts: int
    abnormal_wick_hit: bool
    upper_wick_2bar_hit: bool
    latest_15m_open_time: int | None
    latest_15m_ratio: float | None
    prev_15m_open_time: int | None
    prev_15m_ratio: float | None
    reason: str
    detected_at: int


class CooldownModule:
    """Detect and persist current-round cooldown symbols."""

    TABLE_NAME = "current_round_cooldown_symbols"
    ROUND_MS = 15 * 60_000
    ABNORMAL_LOOKBACK_MS = 30 * 60_000
    UPPER_WICK_RATIO_THRESHOLD = 0.7

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    abnormal_wick_hit INTEGER NOT NULL,
                    upper_wick_2bar_hit INTEGER NOT NULL,
                    latest_15m_open_time INTEGER,
                    latest_15m_ratio REAL,
                    prev_15m_open_time INTEGER,
                    prev_15m_ratio REAL,
                    reason TEXT NOT NULL,
                    detected_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_round "
                f"ON {self.TABLE_NAME}(decision_round_ts DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_detected_at "
                f"ON {self.TABLE_NAME}(detected_at DESC)"
            )

    @staticmethod
    def decision_round_ts_ms(now_ms: Optional[int] = None) -> int:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        round_ms = CooldownModule.ROUND_MS
        return (now_ms // round_ms) * round_ms

    def run_round(
        self,
        symbols: Iterable[str],
        decision_round_ts: int | None = None,
        now_ms: int | None = None,
    ) -> List[CooldownSymbol]:
        """Evaluate all symbols for one decision round and persist hits."""
        if decision_round_ts is None:
            decision_round_ts = self.decision_round_ts_ms(now_ms=now_ms)
        detected_at = int(time.time() * 1000) if now_ms is None else now_ms

        rows: List[CooldownSymbol] = []
        with self._connect() as conn:
            for symbol in symbols:
                cooldown = self._detect_for_symbol(conn, symbol, int(decision_round_ts), int(detected_at))
                if cooldown is None:
                    conn.execute(
                        f"DELETE FROM {self.TABLE_NAME} WHERE symbol = ? AND decision_round_ts = ?",
                        (symbol, int(decision_round_ts)),
                    )
                    continue
                self._save_row(conn, cooldown)
                rows.append(cooldown)

        return sorted(rows, key=lambda row: row.symbol)

    def _detect_for_symbol(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        decision_round_ts: int,
        detected_at: int,
    ) -> CooldownSymbol | None:
        abnormal_hit = self._has_recent_abnormal_wick(conn, symbol, decision_round_ts)
        upper_hit, latest_open_time, latest_ratio, prev_open_time, prev_ratio = self._has_latest_two_15m_upper_wicks(
            conn, symbol, decision_round_ts
        )

        if not abnormal_hit and not upper_hit:
            return None

        reasons = []
        if abnormal_hit:
            reasons.append("30分钟内异常插针")
        if upper_hit:
            reasons.append("最近两根15m上影占比≥0.7且close>open")

        return CooldownSymbol(
            symbol=symbol,
            decision_round_ts=decision_round_ts,
            abnormal_wick_hit=abnormal_hit,
            upper_wick_2bar_hit=upper_hit,
            latest_15m_open_time=latest_open_time,
            latest_15m_ratio=latest_ratio,
            prev_15m_open_time=prev_open_time,
            prev_15m_ratio=prev_ratio,
            reason="；".join(reasons),
            detected_at=detected_at,
        )

    def _has_recent_abnormal_wick(self, conn: sqlite3.Connection, symbol: str, decision_round_ts: int) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM abnormal_wick_events
            WHERE symbol = ?
              AND decision_round_ts BETWEEN ? AND ?
            LIMIT 1
            """,
            (symbol, decision_round_ts - self.ABNORMAL_LOOKBACK_MS, decision_round_ts),
        ).fetchone()
        return row is not None

    def _has_latest_two_15m_upper_wicks(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        decision_round_ts: int,
    ) -> tuple[bool, int | None, float | None, int | None, float | None]:
        rows = conn.execute(
            """
            SELECT open_time, open, high, low, close
            FROM klines_15m
            WHERE symbol = ?
              AND open_time < ?
            ORDER BY open_time DESC
            LIMIT 2
            """,
            (symbol, decision_round_ts),
        ).fetchall()
        if len(rows) < 2:
            return False, None, None, None, None

        ratios: list[float] = []
        bullish_hits: list[bool] = []
        for row in rows:
            open_price = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
            span = high - low
            if span <= 0:
                return False, int(rows[0]["open_time"]), None, int(rows[1]["open_time"]), None
            ratios.append((high - close) / span)
            bullish_hits.append(close > open_price)

        latest_ratio = ratios[0]
        prev_ratio = ratios[1]
        is_hit = (
            latest_ratio >= self.UPPER_WICK_RATIO_THRESHOLD
            and prev_ratio >= self.UPPER_WICK_RATIO_THRESHOLD
            and all(bullish_hits)
        )
        return (
            is_hit,
            int(rows[0]["open_time"]),
            latest_ratio if math.isfinite(latest_ratio) else None,
            int(rows[1]["open_time"]),
            prev_ratio if math.isfinite(prev_ratio) else None,
        )

    def _save_row(self, conn: sqlite3.Connection, row: CooldownSymbol) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (
                symbol, decision_round_ts, abnormal_wick_hit, upper_wick_2bar_hit,
                latest_15m_open_time, latest_15m_ratio,
                prev_15m_open_time, prev_15m_ratio,
                reason, detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                abnormal_wick_hit=excluded.abnormal_wick_hit,
                upper_wick_2bar_hit=excluded.upper_wick_2bar_hit,
                latest_15m_open_time=excluded.latest_15m_open_time,
                latest_15m_ratio=excluded.latest_15m_ratio,
                prev_15m_open_time=excluded.prev_15m_open_time,
                prev_15m_ratio=excluded.prev_15m_ratio,
                reason=excluded.reason,
                detected_at=excluded.detected_at
            """,
            (
                row.symbol,
                row.decision_round_ts,
                int(row.abnormal_wick_hit),
                int(row.upper_wick_2bar_hit),
                row.latest_15m_open_time,
                row.latest_15m_ratio,
                row.prev_15m_open_time,
                row.prev_15m_ratio,
                row.reason,
                row.detected_at,
            ),
        )

    def get_latest_round_symbols(self, decision_round_ts: int | None = None) -> tuple[int | None, List[CooldownSymbol]]:
        with self._connect() as conn:
            latest_round_ts = decision_round_ts
            if latest_round_ts is None:
                latest_round_row = conn.execute(
                    f"SELECT MAX(decision_round_ts) AS latest_round_ts FROM {self.TABLE_NAME}"
                ).fetchone()
                latest_round_ts = latest_round_row["latest_round_ts"]
            if latest_round_ts is None:
                return None, []

            rows = conn.execute(
                f"""
                SELECT symbol, decision_round_ts, abnormal_wick_hit, upper_wick_2bar_hit,
                       latest_15m_open_time, latest_15m_ratio,
                       prev_15m_open_time, prev_15m_ratio,
                       reason, detected_at
                FROM {self.TABLE_NAME}
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (int(latest_round_ts),),
            ).fetchall()

        return int(latest_round_ts), [self._row_to_dataclass(row) for row in rows]

    @staticmethod
    def _row_to_dataclass(row: sqlite3.Row) -> CooldownSymbol:
        return CooldownSymbol(
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]),
            abnormal_wick_hit=bool(row["abnormal_wick_hit"]),
            upper_wick_2bar_hit=bool(row["upper_wick_2bar_hit"]),
            latest_15m_open_time=int(row["latest_15m_open_time"]) if row["latest_15m_open_time"] is not None else None,
            latest_15m_ratio=float(row["latest_15m_ratio"]) if row["latest_15m_ratio"] is not None else None,
            prev_15m_open_time=int(row["prev_15m_open_time"]) if row["prev_15m_open_time"] is not None else None,
            prev_15m_ratio=float(row["prev_15m_ratio"]) if row["prev_15m_ratio"] is not None else None,
            reason=str(row["reason"]),
            detected_at=int(row["detected_at"]),
        )
