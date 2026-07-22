"""Market-wide 15m permission gate for add-position actions."""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import allusdt_15m_ma20
import collector
import db_config
from market_filter_module import MarketFilterModule


@dataclass(frozen=True)
class AddPositionPermissionResult:
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
    alt_outperform_btc: bool
    allusdt_above_1h_ma20: bool
    allusdt_1h_ma20_open_time: Optional[int]
    allusdt_1h_ma20: Optional[float]
    allow_add_positions: bool
    reason: str
    evaluated_at: int


class AddPositionPermissionModule:
    TABLE_NAME = "add_position_permission_rounds"
    ALT_OUTPERFORM_THRESHOLD = -0.003
    ROUND_MS = 15 * 60_000

    def __init__(self, db_path: str = "data/market.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = db_config.connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH)])
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def init_table(self) -> None:
        with db_config.sqlite_schema_lock(self.db_path):
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
                        alt_outperform_btc INTEGER NOT NULL,
                        allusdt_above_1h_ma20 INTEGER NOT NULL DEFAULT 0,
                        allusdt_1h_ma20_open_time INTEGER,
                        allusdt_1h_ma20 REAL,
                        allow_add_positions INTEGER NOT NULL,
                        reason TEXT NOT NULL,
                        evaluated_at INTEGER NOT NULL
                    )
                    """
                )
                self._ensure_column(conn, "allusdt_above_1h_ma20", "INTEGER NOT NULL DEFAULT 0")
                self._ensure_column(conn, "allusdt_1h_ma20_open_time", "INTEGER")
                self._ensure_column(conn, "allusdt_1h_ma20", "REAL")
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_evaluated "
                    f"ON {self.TABLE_NAME}(evaluated_at DESC)"
                )

    @staticmethod
    def decision_round_ts(now_ms: int | None = None) -> int:
        return MarketFilterModule.decision_round_ts(now_ms)

    @classmethod
    def _ensure_column(cls, conn: sqlite3.Connection, column_name: str, column_def: str) -> None:
        existing_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({cls.TABLE_NAME})")}
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE {cls.TABLE_NAME} ADD COLUMN {column_name} {column_def}")

    def _latest_four(self, conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
        return conn.execute(
            f"""
            SELECT open_time, open, close
            FROM {table_name}
            ORDER BY open_time DESC
            LIMIT 4
            """
        ).fetchall()

    def _latest_1h_ma20(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            f"""
            SELECT open_time, ma20
            FROM {allusdt_15m_ma20.H1_MA20_TABLE}
            ORDER BY open_time DESC
            LIMIT 1
            """
        ).fetchone()

    def run_round(self, decision_round_ts: int | None = None, evaluated_at: int | None = None) -> AddPositionPermissionResult:
        self.init_table()
        round_ts = self.decision_round_ts() if decision_round_ts is None else int(decision_round_ts)
        evaluated_ms = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        with self._connect() as conn:
            all_rows = self._latest_four(conn, allusdt_15m_ma20.KLINE_TABLE)
            btc_rows = self._latest_four(conn, collector.BTC_15M_TABLE)
            all_first, all_latest, all_open, all_close, all_delta = MarketFilterModule._delta(all_rows)
            btc_first, btc_latest, btc_open, btc_close, btc_delta = MarketFilterModule._delta(btc_rows)
            h1_ma20_row = self._latest_1h_ma20(conn)
            h1_ma20_open_time = int(h1_ma20_row["open_time"]) if h1_ma20_row else None
            h1_ma20 = float(h1_ma20_row["ma20"]) if h1_ma20_row else None
            allusdt_above_1h_ma20 = all_close is not None and h1_ma20 is not None and all_close > h1_ma20
            if all_delta is None or btc_delta is None or all_close is None:
                alt_outperform = False
                allow = False
                reason = "insufficient_market_data_block_add_position"
            elif h1_ma20 is None:
                alt_outperform = False
                allow = False
                reason = "insufficient_allusdt_1h_ma20_block_add_position"
            else:
                diff = btc_delta - all_delta
                alt_outperform = diff < self.ALT_OUTPERFORM_THRESHOLD
                allow = alt_outperform and allusdt_above_1h_ma20
                if allow:
                    reason = "alt_outperform_btc_and_above_1h_ma20_allow_add_position"
                elif not alt_outperform:
                    reason = "alt_not_outperform_btc_block_add_position"
                else:
                    reason = "allusdt_not_above_1h_ma20_block_add_position"
            result = AddPositionPermissionResult(round_ts, all_first, all_latest, all_open, all_close, all_delta, btc_first, btc_latest, btc_open, btc_close, btc_delta, alt_outperform, allusdt_above_1h_ma20, h1_ma20_open_time, h1_ma20, allow, reason, evaluated_ms)
            self._save(conn, result)
            return result

    def latest_result_for_round(self, decision_round_ts: int) -> AddPositionPermissionResult | None:
        self.init_table()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {self.TABLE_NAME} WHERE decision_round_ts = ?",
                (int(decision_round_ts),),
            ).fetchone()
            return self._from_row(row) if row else None

    def ensure_round_result(self, decision_round_ts: int, evaluated_at: int | None = None) -> AddPositionPermissionResult:
        existing = self.latest_result_for_round(decision_round_ts)
        if existing is not None:
            return existing
        return self.run_round(decision_round_ts=decision_round_ts, evaluated_at=evaluated_at)

    def recent_results(self, limit: int = 100, days: int | None = None, now_ms: int | None = None) -> list[AddPositionPermissionResult]:
        self.init_table()
        with self._connect() as conn:
            params: list[int] = []
            where_clause = ""
            if days is not None:
                current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
                cutoff_ms = current_ms - int(days) * 24 * 60 * 60_000
                where_clause = "WHERE evaluated_at >= ?"
                params.append(cutoff_ms)
            params.append(int(limit))
            rows = conn.execute(
                f"SELECT * FROM {self.TABLE_NAME} {where_clause} ORDER BY decision_round_ts DESC LIMIT ?",
                tuple(params),
            ).fetchall()
            return [self._from_row(row) for row in rows]

    def _save(self, conn: sqlite3.Connection, r: AddPositionPermissionResult) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (decision_round_ts, allusdt_first_open_time, allusdt_latest_open_time, allusdt_open, allusdt_close, allusdt_delta,
             btc_first_open_time, btc_latest_open_time, btc_open, btc_close, btc_delta, alt_outperform_btc,
             allusdt_above_1h_ma20, allusdt_1h_ma20_open_time, allusdt_1h_ma20, allow_add_positions, reason, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                alt_outperform_btc=excluded.alt_outperform_btc,
                allusdt_above_1h_ma20=excluded.allusdt_above_1h_ma20,
                allusdt_1h_ma20_open_time=excluded.allusdt_1h_ma20_open_time,
                allusdt_1h_ma20=excluded.allusdt_1h_ma20,
                allow_add_positions=excluded.allow_add_positions,
                reason=excluded.reason,
                evaluated_at=excluded.evaluated_at
            """,
            (r.decision_round_ts, r.allusdt_first_open_time, r.allusdt_latest_open_time, r.allusdt_open, r.allusdt_close, r.allusdt_delta,
             r.btc_first_open_time, r.btc_latest_open_time, r.btc_open, r.btc_close, r.btc_delta, int(r.alt_outperform_btc),
             int(r.allusdt_above_1h_ma20), r.allusdt_1h_ma20_open_time, r.allusdt_1h_ma20, int(r.allow_add_positions), r.reason, r.evaluated_at),
        )

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> AddPositionPermissionResult:
        return AddPositionPermissionResult(
            decision_round_ts=int(row["decision_round_ts"]),
            allusdt_first_open_time=row["allusdt_first_open_time"], allusdt_latest_open_time=row["allusdt_latest_open_time"],
            allusdt_open=row["allusdt_open"], allusdt_close=row["allusdt_close"], allusdt_delta=row["allusdt_delta"],
            btc_first_open_time=row["btc_first_open_time"], btc_latest_open_time=row["btc_latest_open_time"],
            btc_open=row["btc_open"], btc_close=row["btc_close"], btc_delta=row["btc_delta"],
            alt_outperform_btc=bool(row["alt_outperform_btc"]),
            allusdt_above_1h_ma20=bool(row["allusdt_above_1h_ma20"]),
            allusdt_1h_ma20_open_time=row["allusdt_1h_ma20_open_time"],
            allusdt_1h_ma20=row["allusdt_1h_ma20"],
            allow_add_positions=bool(row["allow_add_positions"]),
            reason=str(row["reason"]), evaluated_at=int(row["evaluated_at"]),
        )
