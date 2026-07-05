"""Partial take-profit protection for existing experiment positions.

Every run scans current Binance Futures positions and the current experiment USDT
net equity. R is defined as ``experiment_equity * 1%``. When a position's
unrealized profit reaches ``2 * R``, the module submits a reduce-only MARKET
order for 30% of the current position size and records both the scan and the
submitted take-profit operation.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance_account_manager import BinanceAccountManager
from trade_action_lock import TradeActionLockManager, acquire_trade_action_lock
from trading_experiment import ExperimentConfig, TradingExperiment


@dataclass(frozen=True)
class PartialTakeProfitCheck:
    id: int
    symbol: str
    checked_at: int
    account_equity_usdt: str
    r_usdt: str
    trigger_r_usdt: str
    unrealized_pnl: str
    entry_price: str
    position_amt: str
    triggered: bool
    reason: str


@dataclass(frozen=True)
class PartialTakeProfitRecord:
    id: int
    symbol: str
    checked_at: int
    side: str
    position_amt: str
    take_profit_quantity: str
    entry_price: str
    account_equity_usdt: str
    r_usdt: str
    trigger_r_usdt: str
    unrealized_pnl: str
    take_profit_order_id: str
    status: str
    reason: str
    raw_response: str


class PartialTakeProfitStrategy:
    """Sell 30% of profitable experiment positions after unrealized PnL reaches 2R."""

    CHECKS_TABLE = "partial_take_profit_checks"
    RECORDS_TABLE = "partial_take_profit_records"
    TAKE_PROFIT_FRACTION = Decimal("0.3")
    TRIGGER_R_MULTIPLE = Decimal("2")

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
        TradeActionLockManager(self.db_path).init_table()
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.CHECKS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    r_usdt TEXT NOT NULL,
                    trigger_r_usdt TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
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
                    side TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    take_profit_quantity TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    r_usdt TEXT NOT NULL,
                    trigger_r_usdt TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    take_profit_order_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_checked ON {self.RECORDS_TABLE}(checked_at DESC, symbol ASC)")

    def run_round(self) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
        equity = helper._fetch_experiment_usdt_equity()
        r_value = equity * self.config.risk_fraction
        trigger_r = r_value * self.TRIGGER_R_MULTIPLE
        positions = helper._fetch_and_store_positions()
        active_positions = [row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0]
        now = int(time.time() * 1000)
        checked = triggered = records = 0
        for position in active_positions:
            checked += 1
            result = self._evaluate_position(position, equity, r_value, trigger_r, now)
            if result:
                triggered += 1
                records += 1
        return {"checked": checked, "triggered": triggered, "records": records, "r_usdt": self._fmt_decimal(r_value), "trigger_r_usdt": self._fmt_decimal(trigger_r)}

    def _evaluate_position(self, position: dict[str, Any], equity: Decimal, r_value: Decimal, trigger_r: Decimal, now: int) -> bool:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        unrealized_pnl = self._decimal_from(position.get("unRealizedProfit", position.get("unrealizedProfit")), Decimal("0"))
        triggered = amount != 0 and entry_price > 0 and trigger_r > 0 and unrealized_pnl >= trigger_r
        if not triggered:
            self._insert_check(symbol, now, equity, r_value, trigger_r, unrealized_pnl, entry_price, amount, False, "unrealized_pnl_lt_2r")
            return False
        if self._has_success_record(symbol, entry_price):
            self._insert_check(symbol, now, equity, r_value, trigger_r, unrealized_pnl, entry_price, amount, True, "partial_take_profit_already_completed")
            return False
        self._insert_check(symbol, now, equity, r_value, trigger_r, unrealized_pnl, entry_price, amount, True, "unrealized_pnl_ge_2r")
        self._execute_take_profit(position, equity, r_value, trigger_r, unrealized_pnl, now)
        return True

    def _execute_take_profit(self, position: dict[str, Any], equity: Decimal, r_value: Decimal, trigger_r: Decimal, unrealized_pnl: Decimal, now: int) -> None:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        side = "SELL" if amount > 0 else "BUY"
        status = "submitted"
        order_id = ""
        raw_response = ""
        reason = "unrealized_pnl_ge_2r_take_profit_30_percent"
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(
            self.db_path, symbol, "partial_take_profit", "partial_take_profit", now
        )
        if lock_handle is None:
            self._insert_record(symbol, now, side, amount, Decimal("0"), entry_price, equity, r_value, trigger_r, unrealized_pnl, "", "failed", f"{reason}; {lock_reason}", raw_response)
            return
        try:
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            step = exchange_info["step_size"]
            quantity = self._floor_to_step(abs(amount) * self.TAKE_PROFIT_FRACTION, step)
            if quantity <= 0:
                raise RuntimeError("take_profit_quantity_rounded_to_zero")
            remaining_quantity = self._floor_to_step(abs(amount) - quantity, step)
            if remaining_quantity <= 0:
                raise RuntimeError("remaining_position_quantity_rounded_to_zero")
            raw_parts: list[str] = []
            cancel_reason = self._cancel_existing_exit_orders(exchange_symbol, raw_parts)
            stop_loss_order_id, stop_loss_reason = self._replace_remaining_stop_loss(
                helper,
                exchange_symbol,
                symbol,
                side,
                remaining_quantity,
                raw_parts,
            )
            take_profit_order_id, take_profit_reason = self._replace_remaining_hard_take_profit(
                helper,
                exchange_symbol,
                symbol,
                side,
                entry_price,
                remaining_quantity,
                exchange_info["tick_size"],
                raw_parts,
            )
            response = self.account_manager._signed_post(
                "/fapi/v1/order",
                {
                    "symbol": exchange_symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": self._fmt_decimal(quantity),
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
            )
            raw_parts.append(str({"partial_take_profit": response}))
            raw_response = " | ".join(raw_parts)
            order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
            self._update_latest_open_trade_exit_orders(symbol, take_profit_order_id, stop_loss_order_id)
            reason = f"{reason}; {cancel_reason}; {stop_loss_reason}; {take_profit_reason}"
        except Exception as exc:
            status = "failed"
            quantity = Decimal("0")
            reason = f"partial_take_profit_failed: {type(exc).__name__}: {exc}"
        finally:
            lock_manager.release(lock_handle)
        self._insert_record(symbol, now, side, amount, quantity, entry_price, equity, r_value, trigger_r, unrealized_pnl, order_id, status, reason, raw_response)

    def _cancel_existing_exit_orders(self, exchange_symbol: str, raw_parts: list[str]) -> str:
        for endpoint, label in (
            ("/fapi/v1/allOpenOrders", "open_orders_cancel"),
            ("/fapi/v1/algoOpenOrders", "algo_orders_cancel"),
        ):
            response = self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})
            raw_parts.append(str({label: response}))
        return "pre_partial_take_profit_cancelled_existing_open_orders"

    def _replace_remaining_stop_loss(
        self,
        helper: TradingExperiment,
        exchange_symbol: str,
        symbol: str,
        side: str,
        quantity: Decimal,
        raw_parts: list[str],
    ) -> tuple[str, str]:
        stop_loss_price = self._latest_open_stop_loss_price(symbol)
        if stop_loss_price <= 0:
            raise RuntimeError("remaining_stop_loss_not_recreated_missing_price")
        endpoint, params = TradingExperiment._exit_order_request(
            {
                "symbol": exchange_symbol,
                "side": side,
                "type": "STOP",
                "quantity": self._fmt_decimal(quantity),
                "price": self._fmt_decimal(stop_loss_price),
                "stopPrice": self._fmt_decimal(stop_loss_price),
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
            }
        )
        response = helper._signed_post_order_with_ioc_retry(endpoint, params, trading_symbol=exchange_symbol)
        raw_parts.append(str({"remaining_stop_loss": response}))
        order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
        return order_id, f"remaining_stop_loss_recreated; quantity={self._fmt_decimal(quantity)}; order_id={order_id}"

    def _replace_remaining_hard_take_profit(
        self,
        helper: TradingExperiment,
        exchange_symbol: str,
        symbol: str,
        side: str,
        entry_price: Decimal,
        quantity: Decimal,
        tick_size: Decimal,
        raw_parts: list[str],
    ) -> tuple[str, str]:
        take_profit_price = helper._hard_take_profit_price(
            entry_price=entry_price,
            quantity=quantity,
            profit_usdt=self.config.hard_take_profit_usdt,
            tick_size=tick_size,
        )
        if take_profit_price <= 0:
            raise RuntimeError("remaining_take_profit_not_recreated_invalid_price")
        endpoint, params = TradingExperiment._exit_order_request(
            {
                "symbol": exchange_symbol,
                "side": side,
                "type": "TAKE_PROFIT",
                "quantity": self._fmt_decimal(quantity),
                "price": self._fmt_decimal(take_profit_price),
                "stopPrice": self._fmt_decimal(take_profit_price),
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
            }
        )
        response = helper._signed_post_order_with_ioc_retry(endpoint, params, trading_symbol=exchange_symbol)
        raw_parts.append(str({"remaining_take_profit": response}))
        order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE {TradingExperiment.TRADES_TABLE}
                SET take_profit_price = ?, updated_at = ?
                WHERE id = (
                    SELECT id FROM {TradingExperiment.TRADES_TABLE}
                    WHERE symbol = ? AND status = 'opened'
                    ORDER BY created_at DESC, id DESC LIMIT 1
                )
                """,
                (self._fmt_decimal(take_profit_price), int(time.time() * 1000), symbol),
            )
        return order_id, f"remaining_hard_take_profit_recreated; target_profit_usdt={self._fmt_decimal(self.config.hard_take_profit_usdt)}; quantity={self._fmt_decimal(quantity)}; price={self._fmt_decimal(take_profit_price)}; order_id={order_id}"

    def _update_latest_open_trade_exit_orders(self, symbol: str, take_profit_order_id: str, stop_loss_order_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE {TradingExperiment.TRADES_TABLE}
                SET take_profit_order_id = ?, stop_loss_order_id = ?, updated_at = ?
                WHERE id = (
                    SELECT id FROM {TradingExperiment.TRADES_TABLE}
                    WHERE symbol = ? AND status = 'opened'
                    ORDER BY created_at DESC, id DESC LIMIT 1
                )
                """,
                (take_profit_order_id, stop_loss_order_id, int(time.time() * 1000), symbol),
            )

    def _latest_open_stop_loss_price(self, symbol: str) -> Decimal:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    f"""
                    SELECT stop_loss_price FROM {TradingExperiment.TRADES_TABLE}
                    WHERE symbol = ? AND status = 'opened' AND stop_loss_price != '0'
                    ORDER BY created_at DESC, id DESC LIMIT 1
                    """,
                    (symbol,),
                ).fetchone()
        except sqlite3.OperationalError:
            return Decimal("0")
        return self._decimal_from(row["stop_loss_price"], Decimal("0")) if row else Decimal("0")

    def _has_success_record(self, symbol: str, entry_price: Decimal) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.RECORDS_TABLE} WHERE symbol = ? AND entry_price = ? AND status = 'submitted' LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return row is not None

    def _insert_check(self, symbol: str, checked_at: int, equity: Decimal, r_value: Decimal, trigger_r: Decimal, pnl: Decimal, entry: Decimal, amount: Decimal, triggered: bool, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl, entry_price, position_amt, triggered, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(trigger_r), self._fmt_decimal(pnl), self._fmt_decimal(entry), self._fmt_decimal(amount), int(triggered), reason))

    def _insert_record(self, symbol: str, checked_at: int, side: str, amount: Decimal, quantity: Decimal, entry: Decimal, equity: Decimal, r_value: Decimal, trigger_r: Decimal, pnl: Decimal, order_id: str, status: str, reason: str, raw: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.RECORDS_TABLE} (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price, account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl, take_profit_order_id, status, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, side, self._fmt_decimal(amount), self._fmt_decimal(quantity), self._fmt_decimal(entry), self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(trigger_r), self._fmt_decimal(pnl), order_id, status, reason, raw))

    def get_latest_round_checks(self) -> tuple[int | None, list[PartialTakeProfitCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS latest_checked_at FROM {self.CHECKS_TABLE}").fetchone()
            latest_checked_at = latest["latest_checked_at"] if latest else None
            if latest_checked_at is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at = ? ORDER BY symbol ASC, id DESC", (int(latest_checked_at),)).fetchall()
        return int(latest_checked_at), [PartialTakeProfitCheck(**{**dict(row), "triggered": bool(row["triggered"])}) for row in rows]

    def recent_records(self, limit: int = 100) -> list[PartialTakeProfitRecord]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?", (int(limit),)).fetchall()
        return [PartialTakeProfitRecord(**dict(row)) for row in rows]

    @staticmethod
    def _base_symbol(symbol: Any) -> str:
        normalized = str(symbol).strip().upper()
        return normalized[:-4] if normalized.endswith("USDT") else normalized

    @staticmethod
    def _decimal_from(value: Any, default: Decimal) -> Decimal:
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return default
            return Decimal(str(value))
        except Exception:
            return default

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.normalize(), "f")
