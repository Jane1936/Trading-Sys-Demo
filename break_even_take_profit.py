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
        reason = "unrealized_pnl_ge_r" if triggered else "unrealized_pnl_lt_r"
        self._insert_check(symbol, now, equity, r_value, unrealized_pnl, entry_price, amount, triggered, reason)
        if not triggered or self._has_success_record(symbol, entry_price):
            return False
        self._move_stop_loss_to_break_even(position, equity, r_value, unrealized_pnl, now)
        return True

    def _move_stop_loss_to_break_even(self, position: dict[str, Any], equity: Decimal, r_value: Decimal, unrealized_pnl: Decimal, now: int) -> None:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        side = "SELL" if amount > 0 else "BUY"
        old_order_id = self._latest_stop_loss_order_id(symbol)
        status = "submitted"
        reason_parts = ["unrealized_pnl_ge_r_move_stop_loss_to_entry"]
        raw_parts: list[str] = []
        try:
            if old_order_id:
                cancel_response = self._cancel_stop_loss_order(exchange_symbol, old_order_id)
                raw_parts.append(str({"cancel_stop_loss": cancel_response}))
                reason_parts.append("old_stop_loss_cancelled")
            else:
                reason_parts.append("old_stop_loss_order_id_missing")
            stop_loss_price = self._break_even_stop_price(exchange_symbol, entry_price, side)
            endpoint, params = TradingExperiment._exit_order_request({
                "symbol": exchange_symbol,
                "side": side,
                "type": "STOP_MARKET",
                "stopPrice": self._fmt_decimal(stop_loss_price),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            })
            response = self.account_manager._signed_post(endpoint, params)
            raw_parts.append(str({"new_stop_loss": response}))
            new_order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
        except Exception as exc:
            status = "failed"
            stop_loss_price = entry_price
            new_order_id = ""
            reason_parts.append(f"break_even_stop_loss_failed: {type(exc).__name__}: {exc}")
        self._insert_record(symbol, now, side, amount, entry_price, equity, r_value, unrealized_pnl, old_order_id, new_order_id, stop_loss_price, status, "; ".join(reason_parts), " | ".join(raw_parts))

    def _cancel_stop_loss_order(self, exchange_symbol: str, order_id: str) -> Any:
        try:
            return self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": order_id})
        except Exception:
            return self.account_manager._signed_delete("/fapi/v1/order", {"symbol": exchange_symbol, "orderId": order_id})

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
