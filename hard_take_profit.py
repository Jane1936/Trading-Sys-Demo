"""Independent, minute-level hard take-profit for simulated positions."""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

import db_config
from binance_account_manager import BinanceAccountManager
from trade_action_lock import TradeActionLockManager, acquire_trade_action_lock
from trading_experiment import ExperimentConfig, TradingExperiment


DEFAULT_PROFIT_RATIO = Decimal(os.getenv("HARD_TAKE_PROFIT_RATIO", "0.20"))


@dataclass(frozen=True)
class HardTakeProfitCheck:
    id: int
    symbol: str
    checked_at: int
    open_trade_id: int
    opened_at: int
    entry_price: str
    position_amt: str
    unrealized_pnl: str
    position_notional: str
    profit_ratio: str
    profit_threshold: str
    triggered: bool
    close_quantity: str
    close_order_id: str
    close_status: str
    reason: str


class HardTakeProfit:
    """Close an entire long position once its unrealized return reaches 20%."""

    CHECKS_TABLE = "hard_take_profit_checks"
    RECORDS_TABLE = "hard_take_profit_records"

    def __init__(self, db_path: str = db_config.TRADING_DB_PATH,
                 account_manager: BinanceAccountManager | None = None,
                 config: ExperimentConfig | None = None,
                 profit_threshold: Decimal | str | float = DEFAULT_PROFIT_RATIO) -> None:
        self.db_path = db_path
        self.core_db_path = db_config.trading_core_path(db_path)
        self.info_db_path = db_config.trading_info_path(db_path)
        self.account_manager = account_manager or BinanceAccountManager()
        self.config = config or ExperimentConfig()
        self.profit_threshold = Decimal(str(profit_threshold))
        if self.profit_threshold <= 0:
            raise ValueError("profit_threshold must be positive")

    def _core_connect(self) -> sqlite3.Connection:
        return db_config.connect_sqlite(self.core_db_path, row_factory=sqlite3.Row)

    def _info_connect(self) -> sqlite3.Connection:
        return db_config.connect_sqlite(self.info_db_path, row_factory=sqlite3.Row)

    def init_tables(self) -> None:
        for path in (self.core_db_path, self.info_db_path):
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        TradeActionLockManager(self.db_path).init_table()
        schema = """
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            checked_at INTEGER NOT NULL,
            open_trade_id INTEGER NOT NULL DEFAULT 0,
            opened_at INTEGER NOT NULL DEFAULT 0,
            entry_price TEXT NOT NULL,
            position_amt TEXT NOT NULL,
            unrealized_pnl TEXT NOT NULL DEFAULT '0',
            position_notional TEXT NOT NULL DEFAULT '0',
            profit_ratio TEXT NOT NULL DEFAULT '0',
            profit_threshold TEXT NOT NULL DEFAULT '0.2',
            triggered INTEGER NOT NULL DEFAULT 0,
            close_quantity TEXT NOT NULL DEFAULT '0',
            close_order_id TEXT NOT NULL DEFAULT '',
            close_status TEXT NOT NULL DEFAULT 'not_required',
            reason TEXT NOT NULL
        """
        with self._core_connect() as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.CHECKS_TABLE} ({schema})")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
        with self._info_connect() as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.RECORDS_TABLE} (record_key TEXT NOT NULL UNIQUE, {schema})")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_checked ON {self.RECORDS_TABLE}(checked_at DESC, symbol ASC)")

    def run_round(self) -> dict[str, int]:
        self.account_manager.validate_config()
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
        positions = helper._fetch_and_store_positions()
        now = int(time.time() * 1000)
        checked = triggered = 0
        for position in positions:
            if self._decimal(position.get("positionAmt")) == 0:
                continue
            checked += 1
            triggered += int(self._evaluate_position(position, now))
        return {"checked": checked, "triggered": triggered}

    def _evaluate_position(self, position: dict[str, Any], now: int) -> bool:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal(position.get("positionAmt"))
        entry = self._decimal(position.get("entryPrice"))
        pnl = self._decimal(position.get("unRealizedProfit", position.get("unrealizedProfit")))
        notional = abs(entry * amount)
        ratio = pnl / notional if notional > 0 else Decimal("0")
        open_trade_id = opened_at = 0
        quantity = Decimal("0")
        order_id = ""
        status = "not_required"
        triggered = False
        try:
            open_trade_id, opened_at = self._latest_open_trade(symbol)
            if amount < 0:
                reason = "short_position_not_supported"
            elif entry <= 0 or notional <= 0:
                reason = "invalid_position_cost"
            elif ratio < self.profit_threshold:
                reason = "hard_take_profit_not_triggered: profit_ratio_below_threshold"
            elif self._has_submitted_record(symbol, open_trade_id):
                reason = "hard_take_profit_already_completed"
            else:
                quantity, order_id, status, action_reason = self._execute_close(exchange_symbol, symbol, amount, now)
                triggered = status == "submitted"
                reason = (f"hard_take_profit_triggered: profit_ratio={self._fmt(ratio)}; "
                          f"threshold={self._fmt(self.profit_threshold)}; {action_reason}")
                values = (symbol, now, open_trade_id, opened_at, entry, amount, pnl,
                          notional, ratio, self.profit_threshold, triggered, quantity,
                          order_id, status, reason)
                self._insert_check(*values)
                self._upsert_record(*values)
                return triggered
        except Exception as exc:
            reason = f"hard_take_profit_failed: {type(exc).__name__}: {exc}"
        self._insert_check(symbol, now, open_trade_id, opened_at, entry, amount, pnl,
                           notional, ratio, self.profit_threshold, False, quantity,
                           order_id, status, reason)
        return False

    def _latest_open_trade(self, symbol: str) -> tuple[int, int]:
        with self._core_connect() as conn:
            row = conn.execute(
                f"SELECT COALESCE(id, rowid) AS trade_id, created_at FROM {TradingExperiment.TRADES_TABLE} "
                "WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row is None:
            raise RuntimeError("missing_latest_open_trade")
        return int(row["trade_id"]), int(row["created_at"])

    def _execute_close(self, exchange_symbol: str, symbol: str, amount: Decimal, now: int) -> tuple[Decimal, str, str, str]:
        manager, handle, lock_reason = acquire_trade_action_lock(
            self.db_path, symbol, "hard_take_profit", "close_position", now
        )
        if handle is None:
            return Decimal("0"), "", "failed", lock_reason
        raw: list[str] = []
        try:
            for endpoint in ("/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"):
                raw.append(str(self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})))
            info = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)._exchange_symbol_info(exchange_symbol)
            quantity = self._floor_to_step(abs(amount), info["step_size"])
            if quantity <= 0:
                raise RuntimeError("close_quantity_rounded_to_zero")
            response = self.account_manager._signed_post("/fapi/v1/order", {
                "symbol": exchange_symbol, "side": "SELL", "type": "MARKET",
                "quantity": self._fmt(quantity), "reduceOnly": "true", "newOrderRespType": "RESULT",
            })
            raw.append(str(response))
            return quantity, TradingExperiment._exit_order_id(response if isinstance(response, dict) else None), "submitted", " | ".join(raw)
        except Exception as exc:
            return Decimal("0"), "", "failed", f"hard_take_profit_close_failed: {type(exc).__name__}: {exc}; " + " | ".join(raw)
        finally:
            manager.release(handle)

    def _has_submitted_record(self, symbol: str, open_trade_id: int) -> bool:
        with self._info_connect() as conn:
            row = conn.execute(f"SELECT 1 FROM {self.RECORDS_TABLE} WHERE symbol=? AND open_trade_id=? AND close_status='submitted' LIMIT 1", (symbol, open_trade_id)).fetchone()
        return row is not None

    def _values(self, symbol: str, checked_at: int, open_trade_id: int, opened_at: int,
                entry: Decimal, amount: Decimal, pnl: Decimal, notional: Decimal,
                ratio: Decimal, threshold: Decimal, triggered: bool, quantity: Decimal,
                order_id: str, status: str, reason: str) -> tuple[Any, ...]:
        return (symbol, checked_at, open_trade_id, opened_at, self._fmt(entry), self._fmt(amount),
                self._fmt(pnl), self._fmt(notional), self._fmt(ratio), self._fmt(threshold),
                int(triggered), self._fmt(quantity), order_id, status, reason)

    _COLUMNS = "symbol, checked_at, open_trade_id, opened_at, entry_price, position_amt, unrealized_pnl, position_notional, profit_ratio, profit_threshold, triggered, close_quantity, close_order_id, close_status, reason"

    def _insert_check(self, *args: Any) -> None:
        values = self._values(*args)
        with self._core_connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} ({self._COLUMNS}) VALUES ({','.join('?' for _ in values)})", values)

    def _upsert_record(self, *args: Any) -> None:
        values = self._values(*args)
        key = f"{values[0]}:{values[2]}:close"
        updates = ", ".join(f"{c}=excluded.{c}" for c in self._COLUMNS.split(", "))
        with self._info_connect() as conn:
            conn.execute(f"INSERT INTO {self.RECORDS_TABLE} (record_key, {self._COLUMNS}) VALUES (?, {','.join('?' for _ in values)}) ON CONFLICT(record_key) DO UPDATE SET {updates} WHERE {self.RECORDS_TABLE}.close_status != 'submitted'", (key, *values))

    def get_latest_round_checks(self) -> tuple[int | None, list[HardTakeProfitCheck]]:
        with self._core_connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS ts FROM {self.CHECKS_TABLE}").fetchone()
            ts = latest["ts"] if latest else None
            rows = [] if ts is None else conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at=? ORDER BY symbol", (ts,)).fetchall()
        return (None if ts is None else int(ts)), [self._from_row(row) for row in rows]

    def recent_action_records(self, limit: int = 100) -> list[HardTakeProfitCheck]:
        with self._info_connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?", (limit,)).fetchall()
        return [self._from_row(row) for row in rows]

    def summary_payload(self) -> dict[str, Any]:
        ts, checks = self.get_latest_round_checks()
        return {"round_ts": ts, "checks": [c.__dict__ for c in checks],
                "records": [r.__dict__ for r in self.recent_action_records()]}

    @staticmethod
    def _from_row(row: sqlite3.Row) -> HardTakeProfitCheck:
        values = dict(row)
        values["triggered"] = bool(values["triggered"])
        return HardTakeProfitCheck(**{k: v for k, v in values.items() if k in HardTakeProfitCheck.__dataclass_fields__})

    @staticmethod
    def _base_symbol(symbol: str) -> str:
        return symbol[:-4] if symbol.endswith("USDT") else symbol

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return Decimal("0")
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step if step > 0 else value

    @staticmethod
    def _fmt(value: Decimal) -> str:
        return format(value.normalize(), "f")
