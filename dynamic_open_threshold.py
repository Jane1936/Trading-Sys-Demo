"""Dynamic opening threshold evaluated after each scoring round.

The module records the highest total score observed in the last 12 hours and
turns it into an opening threshold for the current 15m decision round.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DynamicOpenThresholdResult:
    decision_round_ts: int
    window_start_ts: int
    window_end_ts: int
    highest_total_score: Optional[int]
    highest_symbol: Optional[str]
    highest_score_round_ts: Optional[int]
    min_open_total_score: Optional[int]
    allow_new_positions: bool
    policy: str
    reason: str
    evaluated_at: int


class DynamicOpenThresholdModule:
    TABLE_NAME = "dynamic_open_threshold_rounds"
    ROUND_MS = 15 * 60_000
    WINDOW_MS = 12 * 60 * 60_000
    NO_THRESHOLD_SCORE = 85
    TREND_STANDARD_MIN_SCORE = 81
    STANDARD_TRIAL_MIN_SCORE = 73

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
                    window_start_ts INTEGER NOT NULL,
                    window_end_ts INTEGER NOT NULL,
                    highest_total_score INTEGER,
                    highest_symbol TEXT,
                    highest_score_round_ts INTEGER,
                    min_open_total_score INTEGER,
                    allow_new_positions INTEGER NOT NULL,
                    policy TEXT NOT NULL,
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
        return (now_ms // DynamicOpenThresholdModule.ROUND_MS) * DynamicOpenThresholdModule.ROUND_MS

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> DynamicOpenThresholdResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated_ms = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        window_start = round_ts - self.WINDOW_MS
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, decision_round_ts, total_score
                FROM symbol_total_scores
                WHERE decision_round_ts > ?
                  AND decision_round_ts <= ?
                ORDER BY total_score DESC, decision_round_ts DESC, symbol ASC
                LIMIT 1
                """,
                (window_start, round_ts),
            ).fetchone()
            highest_score = int(row["total_score"]) if row is not None else None
            highest_symbol = str(row["symbol"]) if row is not None else None
            highest_round = int(row["decision_round_ts"]) if row is not None else None
            min_open_score, allow, policy, reason = self._policy_for_score(highest_score)
            result = DynamicOpenThresholdResult(
                decision_round_ts=round_ts,
                window_start_ts=window_start,
                window_end_ts=round_ts,
                highest_total_score=highest_score,
                highest_symbol=highest_symbol,
                highest_score_round_ts=highest_round,
                min_open_total_score=min_open_score,
                allow_new_positions=allow,
                policy=policy,
                reason=reason,
                evaluated_at=evaluated_ms,
            )
            self._save(conn, result)
            return result

    @classmethod
    def _policy_for_score(cls, score: Optional[int]) -> tuple[Optional[int], bool, str, str]:
        if score is None:
            return None, False, "no_new_positions", "no_scores_in_last_12h"
        if score >= cls.NO_THRESHOLD_SCORE:
            return None, True, "no_min_open_threshold", "highest_score_gte_85"
        if cls.STANDARD_TRIAL_MIN_SCORE <= score < cls.NO_THRESHOLD_SCORE:
            return cls.TREND_STANDARD_MIN_SCORE, True, "trend_standard_or_above_only", "highest_score_73_to_84"
        return None, False, "no_new_positions", "highest_score_lt_73"

    def _save(self, conn: sqlite3.Connection, r: DynamicOpenThresholdResult) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (decision_round_ts, window_start_ts, window_end_ts, highest_total_score, highest_symbol,
             highest_score_round_ts, min_open_total_score, allow_new_positions, policy, reason, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_round_ts) DO UPDATE SET
                window_start_ts=excluded.window_start_ts,
                window_end_ts=excluded.window_end_ts,
                highest_total_score=excluded.highest_total_score,
                highest_symbol=excluded.highest_symbol,
                highest_score_round_ts=excluded.highest_score_round_ts,
                min_open_total_score=excluded.min_open_total_score,
                allow_new_positions=excluded.allow_new_positions,
                policy=excluded.policy,
                reason=excluded.reason,
                evaluated_at=excluded.evaluated_at
            """,
            (r.decision_round_ts, r.window_start_ts, r.window_end_ts, r.highest_total_score, r.highest_symbol,
             r.highest_score_round_ts, r.min_open_total_score, int(r.allow_new_positions), r.policy, r.reason, r.evaluated_at),
        )

    def recent_results(self, limit: int = 100, days: int | None = None, now_ms: int | None = None) -> list[DynamicOpenThresholdResult]:
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
    def _from_row(row: sqlite3.Row) -> DynamicOpenThresholdResult:
        return DynamicOpenThresholdResult(
            decision_round_ts=int(row["decision_round_ts"]),
            window_start_ts=int(row["window_start_ts"]),
            window_end_ts=int(row["window_end_ts"]),
            highest_total_score=int(row["highest_total_score"]) if row["highest_total_score"] is not None else None,
            highest_symbol=str(row["highest_symbol"]) if row["highest_symbol"] is not None else None,
            highest_score_round_ts=int(row["highest_score_round_ts"]) if row["highest_score_round_ts"] is not None else None,
            min_open_total_score=int(row["min_open_total_score"]) if row["min_open_total_score"] is not None else None,
            allow_new_positions=bool(row["allow_new_positions"]),
            policy=str(row["policy"]),
            reason=str(row["reason"]),
            evaluated_at=int(row["evaluated_at"]),
        )
