"""Dynamic add-position threshold statistics for 2R success rate.

Every 15-minute round samples the latest 40 newly opened experiment trades and
counts how many of them have submitted partial take-profit records.  The ratio
is recorded as the 2R success rate for display in the market filter page.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass

import db_config
from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment


@dataclass(frozen=True)
class DynamicAddPositionThresholdResult:
    decision_round_ts: int
    sample_size: int
    success_count: int
    success_rate: float | None
    latest_trade_created_at: int | None
    earliest_trade_created_at: int | None
    evaluated_at: int
    reason: str


class DynamicAddPositionThresholdModule:
    TABLE_NAME = "dynamic_add_position_threshold_rounds"
    ROUND_MS = 15 * 60_000
    SAMPLE_LIMIT = 40

    def __init__(self, db_path: str = db_config.TRADING_DB_PATH) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = db_config.connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH), ("scoring", db_config.SCORING_DB_PATH), ("market", db_config.MARKET_DB_PATH)])
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def init_table(self) -> None:
        TradingExperiment(self.db_path).init_tables()
        PartialTakeProfitStrategy(self.db_path).init_tables()
        with db_config.sqlite_schema_lock(self.db_path):
            with self._connect() as conn:
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                        decision_round_ts INTEGER PRIMARY KEY,
                        sample_size INTEGER NOT NULL,
                        success_count INTEGER NOT NULL,
                        success_rate REAL,
                        latest_trade_created_at INTEGER,
                        earliest_trade_created_at INTEGER,
                        evaluated_at INTEGER NOT NULL,
                        reason TEXT NOT NULL
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
        return (now_ms // DynamicAddPositionThresholdModule.ROUND_MS) * DynamicAddPositionThresholdModule.ROUND_MS

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> DynamicAddPositionThresholdResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated_ms = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                WITH recent_open_trades AS (
                    SELECT id, symbol, entry_price, created_at
                    FROM {TradingExperiment.TRADES_TABLE}
                    WHERE status = 'opened'
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                )
                SELECT
                    COUNT(*) AS sample_size,
                    COALESCE(SUM(CASE WHEN EXISTS (
                        SELECT 1
                        FROM {PartialTakeProfitStrategy.RECORDS_TABLE} AS p
                        WHERE p.status = 'submitted'
                          AND UPPER(p.symbol) = UPPER(t.symbol)
                          AND p.entry_price = t.entry_price
                          AND p.checked_at >= t.created_at
                        LIMIT 1
                    ) THEN 1 ELSE 0 END), 0) AS success_count,
                    MAX(created_at) AS latest_trade_created_at,
                    MIN(created_at) AS earliest_trade_created_at
                FROM recent_open_trades AS t
                """,
                (self.SAMPLE_LIMIT,),
            ).fetchone()
            sample_size = int(rows["sample_size"] or 0)
            success_count = int(rows["success_count"] or 0)
            success_rate = (success_count / sample_size) if sample_size else None
            reason = "sampled_latest_40_opened_trades" if sample_size else "no_opened_trades"
            result = DynamicAddPositionThresholdResult(
                round_ts,
                sample_size,
                success_count,
                success_rate,
                int(rows["latest_trade_created_at"]) if rows["latest_trade_created_at"] is not None else None,
                int(rows["earliest_trade_created_at"]) if rows["earliest_trade_created_at"] is not None else None,
                evaluated_ms,
                reason,
            )
            self._save(conn, result)
            return result

    def _save(self, conn: sqlite3.Connection, r: DynamicAddPositionThresholdResult) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (decision_round_ts, sample_size, success_count, success_rate, latest_trade_created_at, earliest_trade_created_at, evaluated_at, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_round_ts) DO UPDATE SET
                sample_size=excluded.sample_size,
                success_count=excluded.success_count,
                success_rate=excluded.success_rate,
                latest_trade_created_at=excluded.latest_trade_created_at,
                earliest_trade_created_at=excluded.earliest_trade_created_at,
                evaluated_at=excluded.evaluated_at,
                reason=excluded.reason
            """,
            (r.decision_round_ts, r.sample_size, r.success_count, r.success_rate, r.latest_trade_created_at, r.earliest_trade_created_at, r.evaluated_at, r.reason),
        )

    def recent_results(self, limit: int = 100, days: int | None = None, now_ms: int | None = None) -> list[DynamicAddPositionThresholdResult]:
        self.init_table()
        with self._connect() as conn:
            params: list[int] = []
            where_clause = ""
            if days is not None:
                current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
                where_clause = "WHERE evaluated_at >= ?"
                params.append(current_ms - int(days) * 24 * 60 * 60_000)
            params.append(int(limit))
            rows = conn.execute(
                f"SELECT * FROM {self.TABLE_NAME} {where_clause} ORDER BY decision_round_ts DESC LIMIT ?",
                tuple(params),
            ).fetchall()
            return [self._from_row(row) for row in rows]

    @staticmethod
    def _from_row(row: sqlite3.Row) -> DynamicAddPositionThresholdResult:
        return DynamicAddPositionThresholdResult(
            decision_round_ts=int(row["decision_round_ts"]),
            sample_size=int(row["sample_size"]),
            success_count=int(row["success_count"]),
            success_rate=float(row["success_rate"]) if row["success_rate"] is not None else None,
            latest_trade_created_at=int(row["latest_trade_created_at"]) if row["latest_trade_created_at"] is not None else None,
            earliest_trade_created_at=int(row["earliest_trade_created_at"]) if row["earliest_trade_created_at"] is not None else None,
            evaluated_at=int(row["evaluated_at"]),
            reason=str(row["reason"]),
        )
