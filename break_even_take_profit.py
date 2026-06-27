"""Break-even take-profit protection for existing experiment positions.

Every run scans current Binance Futures positions and the current experiment USDT
net equity.  R is defined as ``experiment_equity * 1%``.  When a position's
unrealized profit reaches R, the module cancels the original stop-loss order and
creates a new stop-loss order at the position entry price, so the remaining
position is protected at break-even.
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
from trading_experiment import ExperimentConfig, TradingExperiment


@dataclass(frozen=True)
class BreakEvenTakeProfitCheck:
    id: int
    symbol: str
    checked_at: int
    account_equity_usdt: str
    r_usdt: str
    unrealized_pnl: str
    entry_price: str
    position_amt: str
    triggered: bool
    reason: str


@dataclass(frozen=True)
class BreakEvenStopLossRecord:
    id: int
    symbol: str
    checked_at: int
    side: str
    position_amt: str
    entry_price: str
    account_equity_usdt: str
    r_usdt: str
    unrealized_pnl: str
    old_stop_loss_order_id: str
    new_stop_loss_order_id: str
    stop_loss_price: str
    status: str
    reason: str
    raw_response: str


class BreakEvenTakeProfitStrategy:
    """Move profitable experiment positions' stop loss to entry price."""

    CHECKS_TABLE = "break_even_take_profit_checks"
    RECORDS_TABLE = "break_even_stop_loss_records"

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
                    account_equity_usdt TEXT NOT NULL,
                    r_usdt TEXT NOT NULL,
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
                    entry_price TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    r_usdt TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    old_stop_loss_order_id TEXT NOT NULL DEFAULT '',
                    new_stop_loss_order_id TEXT NOT NULL DEFAULT '',
                    stop_loss_price TEXT NOT NULL,
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
        positions = helper._fetch_and_store_positions()
        active_positions = [row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0]
        now = int(time.time() * 1000)
        checked = triggered = records = 0
        for position in active_positions:
            checked += 1
            if self._evaluate_position(position, equity, r_value, now):
                triggered += 1
                records += 1
        return {"checked": checked, "triggered": triggered, "records": records, "r_usdt": self._fmt_decimal(r_value)}

    def _evaluate_position(self, position: dict[str, Any], equity: Decimal, r_value: Decimal, now: int) -> bool:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        unrealized_pnl = self._decimal_from(position.get("unRealizedProfit", position.get("unrealizedProfit")), Decimal("0"))
        triggered = amount != 0 and entry_price > 0 and r_value > 0 and unrealized_pnl >= r_value
        if not triggered:
            self._insert_check(symbol, now, equity, r_value, unrealized_pnl, entry_price, amount, False, "unrealized_pnl_lt_r")
            return False

        current_stop_losses = self._current_stop_loss_orders(exchange_symbol, amount)
        target_stop_price = self._break_even_stop_price(exchange_symbol, entry_price, "SELL" if amount > 0 else "BUY")
        matching_stop_losses = [order for order in current_stop_losses if self._is_limit_order(order) and self._stop_loss_order_price(order) == target_stop_price]
        stale_stop_losses = [order for order in current_stop_losses if not (self._is_limit_order(order) and self._stop_loss_order_price(order) == target_stop_price)]
        if matching_stop_losses and not stale_stop_losses:
            self._insert_check(symbol, now, equity, r_value, unrealized_pnl, entry_price, amount, True, "break_even_already_completed")
            return False

        self._insert_check(symbol, now, equity, r_value, unrealized_pnl, entry_price, amount, True, "unrealized_pnl_ge_r")
        self._move_stop_loss_to_break_even(
            position,
            equity,
            r_value,
            unrealized_pnl,
            now,
            current_stop_losses=current_stop_losses,
            stop_loss_price=target_stop_price,
        )
        return True

    def _move_stop_loss_to_break_even(
        self,
        position: dict[str, Any],
        equity: Decimal,
        r_value: Decimal,
        unrealized_pnl: Decimal,
        now: int,
        current_stop_losses: list[dict[str, Any]] | None = None,
        stop_loss_price: Decimal | None = None,
    ) -> None:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        side = "SELL" if amount > 0 else "BUY"
        current_stop_losses = current_stop_losses if current_stop_losses is not None else self._current_stop_loss_orders(exchange_symbol, amount)
        target_stop_loss = next((order for order in current_stop_losses if self._is_limit_order(order) and self._stop_loss_order_price(order) == stop_loss_price), None)
        stale_stop_losses = [order for order in current_stop_losses if not (self._is_limit_order(order) and self._stop_loss_order_price(order) == stop_loss_price)]
        current_stop_loss_order_ids = [self._stop_loss_order_id(order) for order in current_stop_losses if self._stop_loss_order_id(order)]
        db_stop_loss_order_id = self._latest_stop_loss_order_id(symbol)
        old_order_id = ",".join(current_stop_loss_order_ids) or db_stop_loss_order_id
        status = "submitted"
        reason_parts = ["unrealized_pnl_ge_r_move_stop_loss_to_entry"]
        raw_parts: list[str] = []
        try:
            for stale_order in stale_stop_losses:
                stale_order_id = self._stop_loss_order_id(stale_order)
                if stale_order_id:
                    cancel_response = self._cancel_stop_loss_order(exchange_symbol, stale_order_id, stale_order)
                    raw_parts.append(str({"cancel_stop_loss": cancel_response}))
            if stale_stop_losses:
                reason_parts.append("old_stop_loss_cancelled")
            elif target_stop_loss:
                reason_parts.append("current_stop_loss_already_at_target")
            elif db_stop_loss_order_id:
                reason_parts.append("db_stop_loss_order_id_not_open_skip_cancel")
            else:
                reason_parts.append("old_stop_loss_order_id_missing")
            stop_loss_price = stop_loss_price or self._break_even_stop_price(exchange_symbol, entry_price, side)
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            quantity = self._floor_to_step(abs(amount), exchange_info["step_size"])
            if quantity <= 0:
                raise RuntimeError("break_even_stop_loss_quantity_rounded_to_zero")
            if target_stop_loss:
                response = target_stop_loss
                reason_parts.append("new_stop_loss_not_required")
            else:
                endpoint, request_params = TradingExperiment._exit_order_request(
                    {
                        "symbol": exchange_symbol,
                        "side": side,
                        "type": "LIMIT",
                        "quantity": self._fmt_decimal(quantity),
                        "price": self._fmt_decimal(stop_loss_price),
                        "timeInForce": "GTC",
                        "reduceOnly": "true",
                    }
                )
                response = self.account_manager._signed_post(endpoint, request_params)
            raw_parts.append(str({"new_stop_loss": response}))
            new_order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
            if not new_order_id:
                raise RuntimeError(f"break_even_stop_loss_order_id_missing: response={response}")
        except Exception as exc:
            status = "failed"
            stop_loss_price = entry_price
            new_order_id = ""
            reason_parts.append(f"break_even_stop_loss_failed: {type(exc).__name__}: {exc}")
        self._insert_record(symbol, now, side, amount, entry_price, equity, r_value, unrealized_pnl, old_order_id, new_order_id, stop_loss_price, status, "; ".join(reason_parts), " | ".join(raw_parts))


    def _current_stop_loss_orders(self, exchange_symbol: str, amount: Decimal) -> list[dict[str, Any]]:
        side = "SELL" if amount > 0 else "BUY"
        orders: list[dict[str, Any]] = []
        for endpoint in ("/fapi/v1/openAlgoOrders", "/fapi/v1/openOrders"):
            try:
                rows = self.account_manager._signed_get(endpoint, {"symbol": exchange_symbol})
            except Exception:
                continue
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("symbol", "")).upper() != exchange_symbol:
                    continue
                if str(row.get("side", "")).upper() != side:
                    continue
                if not self._is_stop_loss_order(row):
                    continue
                status = str(row.get("status", "NEW")).upper()
                if status and status != "NEW":
                    continue
                orders.append({**row, "_source_endpoint": endpoint})
        return orders

    @staticmethod
    def _stop_loss_order_id(order: dict[str, Any] | None) -> str:
        if not order:
            return ""
        return str(order.get("algoId") or order.get("orderId") or "")

    @staticmethod
    def _is_stop_loss_order(order: dict[str, Any]) -> bool:
        order_type = str(order.get("type") or order.get("orderType") or "").upper()
        return order_type in {"STOP_MARKET", "LIMIT"}

    @staticmethod
    def _is_limit_order(order: dict[str, Any]) -> bool:
        return str(order.get("type") or order.get("orderType") or "").upper() == "LIMIT"

    def _stop_loss_order_price(self, order: dict[str, Any] | None) -> Decimal | None:
        if not order:
            return None
        for key in ("triggerPrice", "stopPrice", "price"):
            price = self._decimal_from(order.get(key), Decimal("0"))
            if price > 0:
                return price
        return None

    def _cancel_stop_loss_order(self, exchange_symbol: str, order_id: str, order: dict[str, Any] | None = None) -> Any:
        """Cancel the current stop-loss order with the matching Binance endpoint.

        Binance Futures conditional orders created through ``/fapi/v1/algoOrder``
        are not cancelable through the regular ``/fapi/v1/order`` endpoint.  When
        the open-order scan tells us which endpoint returned the order, keep that
        order family and do not fall back to the other cancellation API.
        """
        source_endpoint = str((order or {}).get("_source_endpoint", "")).lower()
        if (order or {}).get("algoId") or source_endpoint.endswith("openalgoorders"):
            algo_id = str((order or {}).get("algoId") or order_id)
            return self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": algo_id})
        if (order or {}).get("orderId") or source_endpoint.endswith("openorders"):
            regular_id = str((order or {}).get("orderId") or order_id)
            return self.account_manager._signed_delete("/fapi/v1/order", {"symbol": exchange_symbol, "orderId": regular_id})

        return self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": order_id})

    def _break_even_stop_price(self, exchange_symbol: str, entry_price: Decimal, side: str) -> Decimal:
        tick = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)._exchange_symbol_info(exchange_symbol)["tick_size"]
        return self._floor_to_step(entry_price, tick) if side == "SELL" else TradingExperiment._ceil_to_tick(entry_price, tick)

    def _latest_stop_loss_order_id(self, symbol: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT stop_loss_order_id FROM {TradingExperiment.TRADES_TABLE}
                WHERE symbol = ? AND status = 'opened' AND stop_loss_order_id != ''
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (symbol,),
            ).fetchone()
        return str(row["stop_loss_order_id"]) if row else ""

    def _has_success_record(self, symbol: str, entry_price: Decimal) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.RECORDS_TABLE} WHERE symbol = ? AND entry_price = ? AND status = 'submitted' LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return row is not None

    def _insert_check(self, symbol: str, checked_at: int, equity: Decimal, r_value: Decimal, pnl: Decimal, entry: Decimal, amount: Decimal, triggered: bool, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, account_equity_usdt, r_usdt, unrealized_pnl, entry_price, position_amt, triggered, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(pnl), self._fmt_decimal(entry), self._fmt_decimal(amount), int(triggered), reason))

    def _insert_record(self, symbol: str, checked_at: int, side: str, amount: Decimal, entry: Decimal, equity: Decimal, r_value: Decimal, pnl: Decimal, old_id: str, new_id: str, stop_price: Decimal, status: str, reason: str, raw: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.RECORDS_TABLE} (symbol, checked_at, side, position_amt, entry_price, account_equity_usdt, r_usdt, unrealized_pnl, old_stop_loss_order_id, new_stop_loss_order_id, stop_loss_price, status, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, side, self._fmt_decimal(amount), self._fmt_decimal(entry), self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(pnl), old_id, new_id, self._fmt_decimal(stop_price), status, reason, raw))

    def get_latest_round_checks(self) -> tuple[int | None, list[BreakEvenTakeProfitCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS latest_checked_at FROM {self.CHECKS_TABLE}").fetchone()
            latest_checked_at = latest["latest_checked_at"] if latest else None
            if latest_checked_at is None:
                return None, []
            rows = conn.execute(
                f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at = ? ORDER BY symbol ASC, id DESC",
                (int(latest_checked_at),),
            ).fetchall()
        return int(latest_checked_at), [BreakEvenTakeProfitCheck(**{**dict(row), "triggered": bool(row["triggered"])}) for row in rows]

    def recent_checks(self, limit: int = 100) -> list[BreakEvenTakeProfitCheck]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?", (int(limit),)).fetchall()
        return [BreakEvenTakeProfitCheck(**{**dict(row), "triggered": bool(row["triggered"])}) for row in rows]

    def recent_records(self, limit: int = 100) -> list[BreakEvenStopLossRecord]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?", (int(limit),)).fetchall()
        return [BreakEvenStopLossRecord(**dict(row)) for row in rows]

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
