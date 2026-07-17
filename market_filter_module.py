"""Market-wide 15m filter for blocking new entries.

The filter is independent from symbol scoring.  It compares the last completed
hour (latest 4 closed 15m candles) for ALLUSDT and BTCUSDT and records whether
new positions are allowed for the decision round.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import allusdt_15m_ma20
import collector


@dataclass(frozen=True)
class MarketFilterResult:
    decision_round_ts: int
    allusdt_first_open_time: Optional[int]
    allusdt_latest_open_time: Optional[int]
    allusdt_open: Optional[float]
    allusdt_close: Optional[float]
    allusdt_delta: Optional[float]
    btc_first_open_time: Optional[int]
    btc_latest_open_time: Optional[int]
    btc_open: Optional[float]
    btc_close: Optional[float]
    btc_delta: Optional[float]
    btc_siphon: bool
    market_crash: bool
    allow_new_positions: bool
    reason: str
    evaluated_at: int


class MarketFilterModule:
    TABLE_NAME = "market_filter_rounds"
    BTC_SIPHON_THRESHOLD = 0.05
    MARKET_CRASH_THRESHOLD = -0.03
    ROUND_MS = 15 * 60_000

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                    decision_round_ts INTEGER PRIMARY KEY,
                    allusdt_first_open_time INTEGER,
                    allusdt_latest_open_time INTEGER,
                    allusdt_open REAL,
                    allusdt_close REAL,
                    allusdt_delta REAL,
                    btc_first_open_time INTEGER,
                    btc_latest_open_time INTEGER,
                    btc_open REAL,
                    btc_close REAL,
                    btc_delta REAL,
                    btc_siphon INTEGER NOT NULL,
                    market_crash INTEGER NOT NULL,
                    allow_new_positions INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    evaluated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_evaluated "
                f"ON {self.TABLE_NAME}(evaluated_at DESC)"
            )

    @staticmethod
    def decision_round_ts(now_ms: int | None = None) -> int:
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return (now_ms // MarketFilterModule.ROUND_MS) * MarketFilterModule.ROUND_MS

    def _latest_four(self, conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
        return conn.execute(
            f"""
            SELECT open_time, open, close
            FROM {table_name}
            ORDER BY open_time DESC
            LIMIT 4
            """
        ).fetchall()

    @staticmethod
    def _delta(rows_desc: list[sqlite3.Row]) -> tuple[Optional[int], Optional[int], Optional[float], Optional[float], Optional[float]]:
        if len(rows_desc) < 4:
            return None, None, None, None, None
        latest = rows_desc[0]
        fourth = rows_desc[3]
        open_price = float(fourth["open"])
        close_price = float(latest["close"])
        if open_price == 0:
            return int(fourth["open_time"]), int(latest["open_time"]), open_price, close_price, None
        return (
            int(fourth["open_time"]),
            int(latest["open_time"]),
            open_price,
            close_price,
            (close_price - open_price) / open_price,
        )

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> MarketFilterResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated_ms = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        with self._connect() as conn:
            all_rows = self._latest_four(conn, allusdt_15m_ma20.KLINE_TABLE)
            btc_rows = self._latest_four(conn, collector.BTC_15M_TABLE)
            all_first, all_latest, all_open, all_close, all_delta = self._delta(all_rows)
            btc_first, btc_latest, btc_open, btc_close, btc_delta = self._delta(btc_rows)
            if all_delta is None or btc_delta is None:
                btc_siphon = False
                market_crash = False
                allow = True
                reason = "insufficient_market_data_allow_open"
            else:
                btc_siphon = (btc_delta - all_delta) > self.BTC_SIPHON_THRESHOLD
                market_crash = all_delta < self.MARKET_CRASH_THRESHOLD
                allow = not (btc_siphon or market_crash)
                reasons = []
                if btc_siphon:
                    reasons.append("btc_siphon")
                if market_crash:
                    reasons.append("market_crash")
                reason = ",".join(reasons) if reasons else "market_filter_passed"
            result = MarketFilterResult(round_ts, all_first, all_latest, all_open, all_close, all_delta, btc_first, btc_latest, btc_open, btc_close, btc_delta, btc_siphon, market_crash, allow, reason, evaluated_ms)
            self._save(conn, result)
            return result

    def _save(self, conn: sqlite3.Connection, r: MarketFilterResult) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (decision_round_ts, allusdt_first_open_time, allusdt_latest_open_time, allusdt_open, allusdt_close, allusdt_delta,
             btc_first_open_time, btc_latest_open_time, btc_open, btc_close, btc_delta, btc_siphon, market_crash,
             allow_new_positions, reason, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_round_ts) DO UPDATE SET
                allusdt_first_open_time=excluded.allusdt_first_open_time,
                allusdt_latest_open_time=excluded.allusdt_latest_open_time,
                allusdt_open=excluded.allusdt_open,
                allusdt_close=excluded.allusdt_close,
                allusdt_delta=excluded.allusdt_delta,
                btc_first_open_time=excluded.btc_first_open_time,
                btc_latest_open_time=excluded.btc_latest_open_time,
                btc_open=excluded.btc_open,
                btc_close=excluded.btc_close,
                btc_delta=excluded.btc_delta,
                btc_siphon=excluded.btc_siphon,
                market_crash=excluded.market_crash,
                allow_new_positions=excluded.allow_new_positions,
                reason=excluded.reason,
                evaluated_at=excluded.evaluated_at
            """,
            (r.decision_round_ts, r.allusdt_first_open_time, r.allusdt_latest_open_time, r.allusdt_open, r.allusdt_close, r.allusdt_delta,
             r.btc_first_open_time, r.btc_latest_open_time, r.btc_open, r.btc_close, r.btc_delta, int(r.btc_siphon), int(r.market_crash), int(r.allow_new_positions), r.reason, r.evaluated_at),
        )

    def recent_results(self, limit: int = 100) -> list[MarketFilterResult]:
        self.init_table()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.TABLE_NAME} ORDER BY decision_round_ts DESC LIMIT ?", (int(limit),)).fetchall()
            return [self._from_row(row) for row in rows]

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> MarketFilterResult:
        return MarketFilterResult(
            decision_round_ts=int(row["decision_round_ts"]),
            allusdt_first_open_time=row["allusdt_first_open_time"], allusdt_latest_open_time=row["allusdt_latest_open_time"],
            allusdt_open=row["allusdt_open"], allusdt_close=row["allusdt_close"], allusdt_delta=row["allusdt_delta"],
            btc_first_open_time=row["btc_first_open_time"], btc_latest_open_time=row["btc_latest_open_time"],
            btc_open=row["btc_open"], btc_close=row["btc_close"], btc_delta=row["btc_delta"],
            btc_siphon=bool(row["btc_siphon"]), market_crash=bool(row["market_crash"]),
            allow_new_positions=bool(row["allow_new_positions"]), reason=str(row["reason"]), evaluated_at=int(row["evaluated_at"]),
        )
