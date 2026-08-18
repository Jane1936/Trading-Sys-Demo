"""Adjust partial take-profit parameters when ALLUSDT is below its 1h MA20."""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import allusdt_15m_ma20
import db_config


DEFAULT_WEAK_MARKET_TRIGGER_R_MULTIPLE = 1.4
DEFAULT_WEAK_MARKET_TAKE_PROFIT_FRACTION = 0.5
SETTINGS_TABLE_NAME = "weak_market_profit_adjustment_settings"


def get_settings(db_path: str | None = None) -> dict[str, float | int]:
    """Return the persisted weak-market take-profit parameters."""
    settings_path = db_path or db_config.BASE_DB_PATH
    with db_config.sqlite_schema_lock(settings_path):
        with db_config.connect_sqlite(settings_path, row_factory=sqlite3.Row) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    trigger_r_multiple REAL NOT NULL,
                    take_profit_fraction REAL NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            conn.execute(
                f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} VALUES (1, ?, ?, ?)",
                (DEFAULT_WEAK_MARKET_TRIGGER_R_MULTIPLE,
                 DEFAULT_WEAK_MARKET_TAKE_PROFIT_FRACTION, int(time.time() * 1000)),
            )
            row = conn.execute(f"SELECT * FROM {SETTINGS_TABLE_NAME} WHERE id = 1").fetchone()
    return {
        "trigger_r_multiple": float(row["trigger_r_multiple"]),
        "take_profit_fraction": float(row["take_profit_fraction"]),
        "updated_at": int(row["updated_at"]),
    }


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, float | int]:
    """Validate and persist weak-market take-profit parameters."""
    try:
        trigger = float(payload["trigger_r_multiple"])
        fraction = float(payload["take_profit_fraction"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("止盈阈值和卖出比例必须是数字") from exc
    if not math.isfinite(trigger) or not 0 < trigger <= 100:
        raise ValueError("止盈阈值必须大于 0 且不超过 100R")
    if not math.isfinite(fraction) or not 0 < fraction <= 1:
        raise ValueError("卖出比例必须大于 0 且不超过 100%")

    settings_path = db_path or db_config.BASE_DB_PATH
    get_settings(settings_path)
    updated_at = int(time.time() * 1000)
    with db_config.connect_sqlite(settings_path) as conn:
        conn.execute(
            f"""UPDATE {SETTINGS_TABLE_NAME}
                SET trigger_r_multiple = ?, take_profit_fraction = ?, updated_at = ?
                WHERE id = 1""",
            (trigger, fraction, updated_at),
        )
    return get_settings(settings_path)


@dataclass(frozen=True)
class WeakMarketProfitAdjustmentResult:
    decision_round_ts: int
    allusdt_15m_open_time: Optional[int]
    allusdt_15m_close: Optional[float]
    allusdt_1h_ma20_open_time: Optional[int]
    allusdt_1h_ma20: Optional[float]
    weak_market: bool
    trigger_r_multiple: float
    take_profit_fraction: float
    reason: str
    evaluated_at: int


class WeakMarketProfitAdjustmentModule:
    """Persist the market regime used by the five-minute partial-profit scan."""

    TABLE_NAME = "weak_market_profit_adjustment_rounds"
    ROUND_MS = 15 * 60_000

    def __init__(self, db_path: str = db_config.MARKET_DB_PATH) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        conn = db_config.connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH)])
        return conn

    def init_table(self) -> None:
        with db_config.sqlite_schema_lock(self.db_path):
            with self._connect() as conn:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                        decision_round_ts INTEGER PRIMARY KEY,
                        allusdt_15m_open_time INTEGER,
                        allusdt_15m_close REAL,
                        allusdt_1h_ma20_open_time INTEGER,
                        allusdt_1h_ma20 REAL,
                        weak_market INTEGER NOT NULL,
                        trigger_r_multiple REAL NOT NULL,
                        take_profit_fraction REAL NOT NULL,
                        reason TEXT NOT NULL,
                        evaluated_at INTEGER NOT NULL
                    )
                """)
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_evaluated ON {self.TABLE_NAME}(evaluated_at DESC)")

    @classmethod
    def decision_round_ts(cls, now_ms: int | None = None) -> int:
        value = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return value // cls.ROUND_MS * cls.ROUND_MS

    @staticmethod
    def _latest_close_time(conn: sqlite3.Connection, table: str) -> int | None:
        row = conn.execute(f"SELECT close_time FROM {table} ORDER BY close_time DESC LIMIT 1").fetchone()
        return int(row["close_time"]) if row else None

    def is_data_converged_for_round(self, decision_round_ts: int) -> tuple[bool, str]:
        expected_close_time = int(decision_round_ts) - 1
        with self._connect() as conn:
            close_time = self._latest_close_time(conn, allusdt_15m_ma20.KLINE_TABLE)
            if close_time is None or close_time < expected_close_time:
                return False, "waiting_allusdt_15m_convergence"
            if int(decision_round_ts) % (60 * 60_000) == 0:
                ma_close_time = self._latest_close_time(conn, allusdt_15m_ma20.H1_MA20_TABLE)
                if ma_close_time is None or ma_close_time < expected_close_time:
                    return False, "waiting_allusdt_1h_ma20_convergence"
        return True, "data_converged"

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> WeakMarketProfitAdjustmentResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        with self._connect() as conn:
            candle = conn.execute(f"SELECT open_time, close FROM {allusdt_15m_ma20.KLINE_TABLE} ORDER BY open_time DESC LIMIT 1").fetchone()
            ma20 = conn.execute(f"SELECT open_time, ma20 FROM {allusdt_15m_ma20.H1_MA20_TABLE} ORDER BY open_time DESC LIMIT 1").fetchone()
            close = float(candle["close"]) if candle else None
            ma_value = float(ma20["ma20"]) if ma20 else None
            weak = close is not None and ma_value is not None and close < ma_value
            if close is None or ma_value is None:
                reason = "insufficient_market_data_use_normal_partial_take_profit"
            elif weak:
                reason = "allusdt_close_below_1h_ma20_weak_market"
            else:
                reason = "allusdt_close_not_below_1h_ma20_normal_market"
            settings = get_settings()
            result = WeakMarketProfitAdjustmentResult(
                round_ts, int(candle["open_time"]) if candle else None, close,
                int(ma20["open_time"]) if ma20 else None, ma_value, weak,
                float(settings["trigger_r_multiple"]) if weak else 2.0,
                float(settings["take_profit_fraction"]) if weak else 0.3,
                reason, evaluated,
            )
            conn.execute(f"""INSERT INTO {self.TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_round_ts) DO UPDATE SET
                allusdt_15m_open_time=excluded.allusdt_15m_open_time,
                allusdt_15m_close=excluded.allusdt_15m_close,
                allusdt_1h_ma20_open_time=excluded.allusdt_1h_ma20_open_time,
                allusdt_1h_ma20=excluded.allusdt_1h_ma20, weak_market=excluded.weak_market,
                trigger_r_multiple=excluded.trigger_r_multiple,
                take_profit_fraction=excluded.take_profit_fraction, reason=excluded.reason,
                evaluated_at=excluded.evaluated_at""", (
                result.decision_round_ts, result.allusdt_15m_open_time, result.allusdt_15m_close,
                result.allusdt_1h_ma20_open_time, result.allusdt_1h_ma20, int(result.weak_market),
                result.trigger_r_multiple, result.take_profit_fraction, result.reason, result.evaluated_at,
            ))
            return result

    def latest_result_for_round(self, decision_round_ts: int) -> WeakMarketProfitAdjustmentResult | None:
        self.init_table()
        with self._connect() as conn:
            row = conn.execute(f"SELECT * FROM {self.TABLE_NAME} WHERE decision_round_ts = ?", (int(decision_round_ts),)).fetchone()
        return self._from_row(row) if row else None

    def recent_results(self, limit: int = 100, days: int | None = None, now_ms: int | None = None) -> list[WeakMarketProfitAdjustmentResult]:
        self.init_table()
        cutoff = None if days is None else (int(time.time() * 1000) if now_ms is None else int(now_ms)) - days * 86_400_000
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self.TABLE_NAME} {('WHERE evaluated_at >= ?' if cutoff is not None else '')} ORDER BY decision_round_ts DESC LIMIT ?",
                ((cutoff, int(limit)) if cutoff is not None else (int(limit),)),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _from_row(row: sqlite3.Row) -> WeakMarketProfitAdjustmentResult:
        return WeakMarketProfitAdjustmentResult(
            int(row["decision_round_ts"]), row["allusdt_15m_open_time"], row["allusdt_15m_close"],
            row["allusdt_1h_ma20_open_time"], row["allusdt_1h_ma20"], bool(row["weak_market"]),
            float(row["trigger_r_multiple"]), float(row["take_profit_fraction"]), row["reason"], int(row["evaluated_at"]),
        )
