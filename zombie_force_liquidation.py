"""Zombie-position force liquidation for stale experiment positions.

Every 15-minute custom execution round scans existing Binance Futures positions.
If a position has been held for more than 48 hours and no submitted break-even
stop-loss record exists for the current position lifecycle, the module cancels
all open stop-loss/take-profit orders for that symbol and closes the full
position with a reduce-only market order.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from binance_account_manager import BinanceAccountManager
from break_even_take_profit import BreakEvenTakeProfitStrategy
from trading_experiment import ExperimentConfig, TradingExperiment


@dataclass(frozen=True)
class ZombieForceLiquidationCheck:
    id: int
    symbol: str
    checked_at: int
    opened_at: int | None
    holding_hours: str
    position_amt: str
    entry_price: str
    break_even_record_found: bool
    triggered: bool
    reason: str


@dataclass(frozen=True)
class ZombieForceLiquidationRecord:
    id: int
    symbol: str
    checked_at: int
    opened_at: int | None
    side: str
    position_amt: str
    quantity: str
    entry_price: str
    status: str
    order_id: str
    reason: str
    raw_response: str


class ZombieForceLiquidationModule:
    """Force-close positions older than 48h without lifecycle break-even protection."""

    CHECKS_TABLE = "zombie_force_liquidation_checks"
    RECORDS_TABLE = "zombie_force_liquidation_records"
    HOLDING_THRESHOLD_MS = 48 * 60 * 60 * 1000

    def __init__(
        self,
        db_path: str = "data/klines.db",
        account_manager: BinanceAccountManager | None = None,
        config: ExperimentConfig | None = None,
    ) -> None:
        self.db_path = db_path
        self.account_manager = account_manager or BinanceAccountManager()
        self.config = config or ExperimentConfig()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def init_tables(self) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.CHECKS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    opened_at INTEGER,
                    holding_hours TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    break_even_record_found INTEGER NOT NULL,
                    triggered INTEGER NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.RECORDS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    opened_at INTEGER,
                    side TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    status TEXT NOT NULL,
                    order_id TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT ''
                )
                """
            )
            record_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.RECORDS_TABLE})").fetchall()}
            if "order_id" not in record_columns:
                conn.execute(f"ALTER TABLE {self.RECORDS_TABLE} ADD COLUMN order_id TEXT NOT NULL DEFAULT ''")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_checked ON {self.RECORDS_TABLE}(checked_at DESC, symbol ASC)")

    def run_round(self, checked_at: int | None = None) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        break_even = BreakEvenTakeProfitStrategy(db_path=self.db_path, account_manager=self.account_manager, config=self.config)
        break_even.init_tables()
        helper = TradingExperiment(db_path=self.db_path, account_manager=self.account_manager, config=self.config)
        helper.init_tables()
        positions = helper._fetch_and_store_positions()
        now = int(time.time() * 1000) if checked_at is None else int(checked_at)
        checked = triggered = records = 0
        for position in positions:
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            if amount == 0:
                continue
            checked += 1
            if self._evaluate_position(helper, position, now):
                triggered += 1
                records += 1
        return {"checked": checked, "triggered": triggered, "records": records, "reason": "completed"}

    def _evaluate_position(self, helper: TradingExperiment, position: dict[str, Any], now: int) -> bool:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = TradingExperiment._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        opened_at = self._latest_opened_at(symbol, now)
        holding_ms = max(0, now - opened_at) if opened_at is not None else 0
        holding_hours = Decimal(holding_ms) / Decimal(60 * 60 * 1000)
        has_break_even = self._has_lifecycle_break_even_record(symbol, opened_at)
        if opened_at is None:
            self._insert_check(symbol, now, None, holding_hours, amount, entry_price, has_break_even, False, "opened_at_missing")
            return False
        if holding_ms <= self.HOLDING_THRESHOLD_MS:
            self._insert_check(symbol, now, opened_at, holding_hours, amount, entry_price, has_break_even, False, "holding_time_lte_48h")
            return False
        if has_break_even:
            self._insert_check(symbol, now, opened_at, holding_hours, amount, entry_price, True, False, "break_even_record_found_in_lifecycle")
            return False

        self._insert_check(symbol, now, opened_at, holding_hours, amount, entry_price, False, True, "holding_time_gt_48h_without_break_even_record")
        self._force_close(helper, exchange_symbol, symbol, amount, entry_price, opened_at, now)
        return True

    def _force_close(self, helper: TradingExperiment, exchange_symbol: str, symbol: str, amount: Decimal, entry_price: Decimal, opened_at: int | None, now: int) -> None:
        side = "SELL" if amount > 0 else "BUY"
        status = "submitted"
        reason_parts = ["zombie_position_force_liquidation"]
        raw_parts: list[str] = []
        order_id = ""
        quantity = abs(amount)
        try:
            for endpoint, label in (("/fapi/v1/allOpenOrders", "open_orders_cancel"), ("/fapi/v1/algoOpenOrders", "algo_orders_cancel")):
                response = self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})
                raw_parts.append(str({label: response}))
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            quantity = TradingExperiment._floor_to_step(abs(amount), exchange_info["step_size"])
            if quantity <= 0:
                raise RuntimeError("zombie_liquidation_quantity_rounded_to_zero")
            response = helper._signed_post_order_with_ioc_retry(
                "/fapi/v1/order",
                {
                    "symbol": exchange_symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": TradingExperiment._fmt_decimal(quantity),
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
                trading_symbol=exchange_symbol,
                tick_size=exchange_info["tick_size"],
            )
            order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            raw_parts.append(str({"market_close": response}))
        except Exception as exc:
            status = "failed"
            reason_parts.append(f"zombie_force_liquidation_failed: {type(exc).__name__}: {exc}")
        self._insert_record(symbol, now, opened_at, side, amount, quantity, entry_price, status, order_id, "; ".join(reason_parts), " | ".join(raw_parts))

    def _latest_opened_at(self, symbol: str, now: int) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT created_at FROM {TradingExperiment.TRADES_TABLE}
                WHERE symbol = ? AND status = 'opened' AND created_at <= ?
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (symbol, now),
            ).fetchone()
        return int(row["created_at"]) if row else None

    def _has_lifecycle_break_even_record(self, symbol: str, opened_at: int | None) -> bool:
        if opened_at is None:
            return False
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1 FROM {BreakEvenTakeProfitStrategy.RECORDS_TABLE}
                WHERE symbol = ? AND status = 'submitted' AND checked_at >= ?
                LIMIT 1
                """,
                (symbol, opened_at),
            ).fetchone()
        return row is not None

    def _insert_check(self, symbol: str, checked_at: int, opened_at: int | None, holding_hours: Decimal, amount: Decimal, entry_price: Decimal, has_break_even: bool, triggered: bool, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, opened_at, holding_hours, position_amt, entry_price, break_even_record_found, triggered, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (symbol, checked_at, opened_at, self._fmt_decimal(holding_hours), self._fmt_decimal(amount), self._fmt_decimal(entry_price), int(has_break_even), int(triggered), reason),
            )

    def _insert_record(self, symbol: str, checked_at: int, opened_at: int | None, side: str, amount: Decimal, quantity: Decimal, entry_price: Decimal, status: str, order_id: str, reason: str, raw: str) -> None:
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO {self.RECORDS_TABLE} (symbol, checked_at, opened_at, side, position_amt, quantity, entry_price, status, order_id, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (symbol, checked_at, opened_at, side, self._fmt_decimal(amount), self._fmt_decimal(quantity), self._fmt_decimal(entry_price), status, order_id, reason, raw),
            )

    def get_latest_round_checks(self) -> tuple[int | None, list[ZombieForceLiquidationCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS latest_checked_at FROM {self.CHECKS_TABLE}").fetchone()
            latest_checked_at = latest["latest_checked_at"] if latest else None
            if latest_checked_at is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at = ? ORDER BY symbol ASC, id DESC", (int(latest_checked_at),)).fetchall()
        return int(latest_checked_at), [ZombieForceLiquidationCheck(**{**dict(row), "break_even_record_found": bool(row["break_even_record_found"]), "triggered": bool(row["triggered"])}) for row in rows]

    def recent_records(self, limit: int = 100, since_ms: int | None = None) -> list[ZombieForceLiquidationRecord]:
        self.init_tables()
        if since_ms is not None:
            query = f"""
                SELECT * FROM {self.RECORDS_TABLE}
                WHERE checked_at >= ?
                ORDER BY checked_at DESC, id DESC
                LIMIT ?
            """
            params = (int(since_ms), int(limit))
        else:
            query = f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?"
            params = (int(limit),)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [ZombieForceLiquidationRecord(**dict(row)) for row in rows]

    @staticmethod
    def _decimal_from(value: Any, default: Decimal) -> Decimal:
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return default
            return Decimal(str(value))
        except Exception:
            return default

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.normalize(), "f")
