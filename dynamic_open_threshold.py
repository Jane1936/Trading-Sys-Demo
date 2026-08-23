"""Dynamic opening threshold evaluated after each scoring round.

The module records the highest total score observed in a configurable lookback
window and turns it into an opening threshold for the current 15m round.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import db_config


DEFAULT_SETTINGS = {
    "window_hours": 12,
    "unrestricted_score": 85,
    "restricted_score_floor": 73,
    "min_open_total_score": 81,
}
SETTINGS_TABLE_NAME = "dynamic_open_threshold_settings"


def _validate_settings(payload: dict) -> dict[str, int]:
    try:
        values = {key: float(payload[key]) for key in DEFAULT_SETTINGS}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("统计窗口和三个评分门槛均为必填数字") from exc
    if any(not math.isfinite(value) or not value.is_integer() for value in values.values()):
        raise ValueError("统计窗口和三个评分门槛必须是整数")
    settings = {key: int(value) for key, value in values.items()}
    if not 1 <= settings["window_hours"] <= 168:
        raise ValueError("统计窗口必须在 1–168 小时之间")
    if not 0 <= settings["restricted_score_floor"] < settings["unrestricted_score"] <= 100:
        raise ValueError("限制区最低分必须小于放开门槛分，且均在 0–100 之间")
    if not 0 <= settings["min_open_total_score"] <= 100:
        raise ValueError("限制区开仓最低总分必须在 0–100 之间")
    return settings


def get_settings(db_path: str | None = None) -> dict[str, int]:
    """Return the persisted policy used by the next 15-minute round."""
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
            id INTEGER PRIMARY KEY CHECK (id = 1), window_hours INTEGER NOT NULL,
            unrestricted_score INTEGER NOT NULL, restricted_score_floor INTEGER NOT NULL,
            min_open_total_score INTEGER NOT NULL, updated_at INTEGER NOT NULL)""")
        conn.execute(f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} VALUES (1, ?, ?, ?, ?, ?)",
                     (*DEFAULT_SETTINGS.values(), int(time.time() * 1000)))
        row = conn.execute(f"SELECT * FROM {SETTINGS_TABLE_NAME} WHERE id = 1").fetchone()
    return _validate_settings(dict(row))


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, int]:
    """Validate and persist a complete dynamic-opening policy."""
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(f"""UPDATE {SETTINGS_TABLE_NAME} SET window_hours=?, unrestricted_score=?,
            restricted_score_floor=?, min_open_total_score=?, updated_at=? WHERE id=1""",
            (*settings.values(), int(time.time() * 1000)))
    return get_settings(path)


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


@dataclass(frozen=True)
class DynamicOpenThresholdError:
    decision_round_ts: int
    error: str
    created_at: int


class DynamicOpenThresholdModule:
    TABLE_NAME = "dynamic_open_threshold_rounds"
    ERROR_TABLE_NAME = "dynamic_open_threshold_error_records"
    ROUND_MS = 15 * 60_000
    WINDOW_MS = 12 * 60 * 60_000
    NO_THRESHOLD_SCORE = 85
    TREND_STANDARD_MIN_SCORE = 81
    STANDARD_TRIAL_MIN_SCORE = 73

    def __init__(self, db_path: str = "data/klines.db", settings_db_path: str | None = None) -> None:
        self.db_path = db_path
        self.settings_db_path = settings_db_path

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = db_config.connect_sqlite(self.db_path)
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

    @classmethod
    def record_error(
        cls,
        error_db_path: str,
        decision_round_ts: int,
        error: str,
        created_at: int | None = None,
    ) -> None:
        """Persist worker errors outside the scoring DB for dashboard diagnostics."""
        with db_config.connect_sqlite(error_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {cls.ERROR_TABLE_NAME} (
                    decision_round_ts INTEGER PRIMARY KEY,
                    error TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                INSERT INTO {cls.ERROR_TABLE_NAME} (decision_round_ts, error, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(decision_round_ts) DO UPDATE SET
                    error=excluded.error,
                    created_at=excluded.created_at
                """,
                (
                    int(decision_round_ts),
                    str(error),
                    int(time.time() * 1000) if created_at is None else int(created_at),
                ),
            )

    @classmethod
    def recent_errors(
        cls,
        error_db_path: str,
        limit: int = 20,
        days: int = 7,
        now_ms: int | None = None,
    ) -> list[DynamicOpenThresholdError]:
        """Return recent worker errors; an uninitialized store has no errors."""
        with db_config.connect_sqlite(error_db_path, row_factory=sqlite3.Row) as conn:
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (cls.ERROR_TABLE_NAME,),
            ).fetchone()
            if table_exists is None:
                return []
            current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
            rows = conn.execute(
                f"""
                SELECT decision_round_ts, error, created_at
                FROM {cls.ERROR_TABLE_NAME}
                WHERE created_at >= ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (current_ms - int(days) * 24 * 60 * 60_000, int(limit)),
            ).fetchall()
        return [
            DynamicOpenThresholdError(
                decision_round_ts=int(row["decision_round_ts"]),
                error=str(row["error"]),
                created_at=int(row["created_at"]),
            )
            for row in rows
        ]

    @staticmethod
    def decision_round_ts(now_ms: int | None = None) -> int:
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return (now_ms // DynamicOpenThresholdModule.ROUND_MS) * DynamicOpenThresholdModule.ROUND_MS

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> DynamicOpenThresholdResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated_ms = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        settings = get_settings(self.settings_db_path)
        window_start = round_ts - settings["window_hours"] * 60 * 60_000
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
            min_open_score, allow, policy, reason = self._policy_for_score(highest_score, settings)
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
    def _policy_for_score(cls, score: Optional[int], settings: dict[str, int] | None = None) -> tuple[Optional[int], bool, str, str]:
        settings = _validate_settings(settings or DEFAULT_SETTINGS)
        unrestricted = settings["unrestricted_score"]
        floor = settings["restricted_score_floor"]
        if score is None:
            return None, False, "no_new_positions", f"no_scores_in_last_{settings['window_hours']}h"
        if score >= unrestricted:
            return None, True, "no_min_open_threshold", f"highest_score_gte_{unrestricted}"
        if floor <= score < unrestricted:
            return settings["min_open_total_score"], True, "trend_standard_or_above_only", f"highest_score_{floor}_to_{unrestricted - 1}"
        return None, False, "no_new_positions", f"highest_score_lt_{floor}"

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
