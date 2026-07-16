"""Dynamic profit protection for open experiment positions.

Runs once per minute before trailing stop. It reads latest 1m kline close,
maintains the highest 1m high since open, and closes the full position by
reduce-only MARKET order when current unrealized PnL is in configured R bands and
latest close gives back enough of the open-to-high profit.
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
class DynamicProfitProtectionCheck:
    id: int
    symbol: str
    checked_at: int
    entry_price: str
    position_amt: str
    account_equity_usdt: str
    r_usdt: str
    unrealized_pnl: str
    profit_r_multiple: str
    latest_1m_high: str
    latest_1m_close: str
    highest_since_open: str
    profit_drawdown_ratio: str
    drawdown_threshold: str
    triggered: bool
    close_quantity: str
    close_order_id: str
    close_status: str
    eligible: bool
    reason: str


class DynamicProfitProtection:
    CHECKS_TABLE = "dynamic_profit_protection_checks"

    def __init__(self, db_path: str = "data/klines.db", account_manager: BinanceAccountManager | None = None, config: ExperimentConfig | None = None) -> None:
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
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.CHECKS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    entry_price TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL DEFAULT '0',
                    r_usdt TEXT NOT NULL DEFAULT '0',
                    unrealized_pnl TEXT NOT NULL DEFAULT '0',
                    profit_r_multiple TEXT NOT NULL DEFAULT '0',
                    latest_1m_high TEXT NOT NULL DEFAULT '0',
                    latest_1m_close TEXT NOT NULL DEFAULT '0',
                    highest_since_open TEXT NOT NULL DEFAULT '0',
                    profit_drawdown_ratio TEXT NOT NULL DEFAULT '0',
                    drawdown_threshold TEXT NOT NULL DEFAULT '0',
                    triggered INTEGER NOT NULL DEFAULT 0,
                    close_quantity TEXT NOT NULL DEFAULT '0',
                    close_order_id TEXT NOT NULL DEFAULT '',
                    close_status TEXT NOT NULL DEFAULT 'not_required',
                    eligible INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL
                )
            """)
            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.CHECKS_TABLE})")}
            migrations = {
                "account_equity_usdt": "TEXT NOT NULL DEFAULT '0'", "r_usdt": "TEXT NOT NULL DEFAULT '0'",
                "unrealized_pnl": "TEXT NOT NULL DEFAULT '0'", "profit_r_multiple": "TEXT NOT NULL DEFAULT '0'",
                "latest_1m_high": "TEXT NOT NULL DEFAULT '0'", "latest_1m_close": "TEXT NOT NULL DEFAULT '0'",
                "highest_since_open": "TEXT NOT NULL DEFAULT '0'", "profit_drawdown_ratio": "TEXT NOT NULL DEFAULT '0'",
                "drawdown_threshold": "TEXT NOT NULL DEFAULT '0'", "triggered": "INTEGER NOT NULL DEFAULT 0",
                "close_quantity": "TEXT NOT NULL DEFAULT '0'", "close_order_id": "TEXT NOT NULL DEFAULT ''",
                "close_status": "TEXT NOT NULL DEFAULT 'not_required'", "eligible": "INTEGER NOT NULL DEFAULT 0",
            }
            for column, definition in migrations.items():
                if column not in columns:
                    conn.execute(f"ALTER TABLE {self.CHECKS_TABLE} ADD COLUMN {column} {definition}")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_position ON {self.CHECKS_TABLE}(symbol ASC, entry_price ASC, checked_at DESC)")

    def run_round(self) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
        equity = helper._fetch_experiment_usdt_equity()
        r_value = equity * self.config.risk_fraction
        positions = helper._fetch_and_store_positions()
        now = int(time.time() * 1000)
        checked = eligible = triggered = 0
        for position in positions:
            if self._decimal_from(position.get("positionAmt"), Decimal("0")) == 0:
                continue
            checked += 1
            is_eligible, did_trigger = self._evaluate_position(position, equity, r_value, now)
            eligible += int(is_eligible)
            triggered += int(did_trigger)
        return {"checked": checked, "eligible": eligible, "triggered": triggered, "r_usdt": self._fmt_decimal(r_value)}

    def _evaluate_position(self, position: dict[str, Any], equity: Decimal, r_value: Decimal, now: int) -> tuple[bool, bool]:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        pnl = self._decimal_from(position.get("unRealizedProfit", position.get("unrealizedProfit")), Decimal("0"))
        r_multiple = pnl / r_value if r_value > 0 else Decimal("0")
        close_quantity = Decimal("0"); close_order_id = ""; close_status = "not_required"
        high = close = highest = drawdown = threshold = Decimal("0")
        eligible = amount > 0 and entry_price > 0 and r_value > 0 and pnl > r_value * Decimal("2")
        try:
            high, close = self._latest_1m_high_close(symbol)
            open_time = self._latest_open_trade_created_at(symbol)
            highest = max(self._previous_highest_since_open(symbol, entry_price), self._highest_1m_high_since(symbol, open_time), high)
            threshold = self._threshold_for_r_multiple(r_multiple)
            if amount <= 0:
                reason = "short_position_not_supported_by_highest_price_rule"
                eligible = False
            elif threshold <= 0:
                reason = "unrealized_pnl_not_in_dynamic_profit_protection_band"
                eligible = False
            elif highest <= entry_price or close <= 0:
                reason = "missing_valid_highest_or_close"
                eligible = False
            else:
                drawdown = max(Decimal("0"), (highest - close) / (highest - entry_price))
                if drawdown >= threshold:
                    if self._has_close_record(symbol, entry_price):
                        reason = "dynamic_profit_protection_already_completed"
                    else:
                        close_quantity, close_order_id, close_status, action_reason = self._execute_close(exchange_symbol, symbol, amount, now)
                        reason = f"dynamic_profit_protection_triggered; profit_r={self._fmt_decimal(r_multiple)}; drawdown={self._fmt_decimal(drawdown)}; threshold={self._fmt_decimal(threshold)}; {action_reason}"
                        self._insert_check(symbol, now, entry_price, amount, equity, r_value, pnl, r_multiple, high, close, highest, drawdown, threshold, close_status == "submitted", close_quantity, close_order_id, close_status, eligible, reason)
                        return eligible, close_status == "submitted"
                else:
                    reason = f"dynamic_profit_protection_not_triggered; profit_r={self._fmt_decimal(r_multiple)}; drawdown_lt_threshold"
        except Exception as exc:
            reason = f"dynamic_profit_protection_failed: {type(exc).__name__}: {exc}"
        self._insert_check(symbol, now, entry_price, amount, equity, r_value, pnl, r_multiple, high, close, highest, drawdown, threshold, False, close_quantity, close_order_id, close_status, eligible, reason)
        return eligible, False

    @staticmethod
    def _threshold_for_r_multiple(r_multiple: Decimal) -> Decimal:
        if Decimal("2") < r_multiple <= Decimal("4"):
            return Decimal("0.40")
        if Decimal("4") < r_multiple <= Decimal("7"):
            return Decimal("0.30")
        if r_multiple > Decimal("7"):
            return Decimal("0.20")
        return Decimal("0")

    def _execute_close(self, exchange_symbol: str, symbol: str, amount: Decimal, now: int) -> tuple[Decimal, str, str, str]:
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(self.db_path, symbol, "dynamic_profit_protection", "close_position", now)
        if lock_handle is None:
            return Decimal("0"), "", "failed", lock_reason
        raw_parts: list[str] = []
        try:
            for endpoint, label in (("/fapi/v1/allOpenOrders", "open_orders_cancel"), ("/fapi/v1/algoOpenOrders", "algo_orders_cancel")):
                raw_parts.append(str({label: self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})}))
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
            info = helper._exchange_symbol_info(exchange_symbol)
            quantity = self._floor_to_step(abs(amount), info["step_size"])
            if quantity <= 0:
                raise RuntimeError("close_quantity_rounded_to_zero")
            response = self.account_manager._signed_post("/fapi/v1/order", {"symbol": exchange_symbol, "side": "SELL", "type": "MARKET", "quantity": self._fmt_decimal(quantity), "reduceOnly": "true", "newOrderRespType": "RESULT"})
            raw_parts.append(str({"close_position": response}))
            order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
            return quantity, order_id, "submitted", " | ".join(raw_parts)
        except Exception as exc:
            return Decimal("0"), "", "failed", f"dynamic_profit_protection_close_failed: {type(exc).__name__}: {exc}; " + " | ".join(raw_parts)
        finally:
            lock_manager.release(lock_handle)

    def _has_close_record(self, symbol: str, entry_price: Decimal) -> bool:
        with self._connect() as conn:
            row = conn.execute(f"SELECT 1 FROM {self.CHECKS_TABLE} WHERE symbol = ? AND entry_price = ? AND triggered = 1 AND close_status = 'submitted' LIMIT 1", (symbol, self._fmt_decimal(entry_price))).fetchone()
        return row is not None

    def _latest_1m_high_close(self, symbol: str) -> tuple[Decimal, Decimal]:
        with self._connect() as conn:
            row = conn.execute("SELECT high, close FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1", (symbol,)).fetchone()
        if row is None:
            raise RuntimeError("missing_1m_kline")
        high = self._decimal_from(row["high"], Decimal("0")); close = self._decimal_from(row["close"], Decimal("0"))
        if high <= 0 or close <= 0:
            raise RuntimeError("invalid_1m_kline")
        return high, close

    def _latest_open_trade_created_at(self, symbol: str) -> int:
        try:
            with self._connect() as conn:
                row = conn.execute(f"SELECT created_at FROM {TradingExperiment.TRADES_TABLE} WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
        except sqlite3.OperationalError:
            return 0
        return int(row["created_at"]) if row and row["created_at"] is not None else 0

    def _highest_1m_high_since(self, symbol: str, since_ms: int) -> Decimal:
        if since_ms <= 0:
            return Decimal("0")
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(CAST(high AS REAL)) AS highest FROM klines_1m WHERE symbol = ? AND open_time >= ?", (symbol, int(since_ms))).fetchone()
        return self._decimal_from(row["highest"], Decimal("0")) if row else Decimal("0")

    def _previous_highest_since_open(self, symbol: str, entry_price: Decimal) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(f"SELECT highest_since_open FROM {self.CHECKS_TABLE} WHERE symbol = ? AND entry_price = ? ORDER BY checked_at DESC, id DESC LIMIT 1", (symbol, self._fmt_decimal(entry_price))).fetchone()
        return self._decimal_from(row["highest_since_open"], Decimal("0")) if row else Decimal("0")

    def _insert_check(self, symbol: str, checked_at: int, entry: Decimal, amount: Decimal, equity: Decimal, r_value: Decimal, pnl: Decimal, r_multiple: Decimal, high: Decimal, close: Decimal, highest: Decimal, drawdown: Decimal, threshold: Decimal, triggered: bool, close_quantity: Decimal, close_order_id: str, close_status: str, eligible: bool, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, entry_price, position_amt, account_equity_usdt, r_usdt, unrealized_pnl, profit_r_multiple, latest_1m_high, latest_1m_close, highest_since_open, profit_drawdown_ratio, drawdown_threshold, triggered, close_quantity, close_order_id, close_status, eligible, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, self._fmt_decimal(entry), self._fmt_decimal(amount), self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(pnl), self._fmt_decimal(r_multiple), self._fmt_decimal(high), self._fmt_decimal(close), self._fmt_decimal(highest), self._fmt_decimal(drawdown), self._fmt_decimal(threshold), int(triggered), self._fmt_decimal(close_quantity), close_order_id, close_status, int(eligible), reason))

    def summary_payload(self) -> dict[str, Any]:
        round_ts, checks = self.get_latest_round_checks()
        records = self.recent_action_records(limit=100)
        return {"round_ts": round_ts, "checks": [check.__dict__ for check in checks], "records": [record.__dict__ for record in records]}

    def get_latest_round_checks(self) -> tuple[int | None, list[DynamicProfitProtectionCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS latest_checked_at FROM {self.CHECKS_TABLE}").fetchone()
            latest_checked_at = latest["latest_checked_at"] if latest else None
            if latest_checked_at is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at = ? ORDER BY symbol ASC, id DESC", (int(latest_checked_at),)).fetchall()
        return int(latest_checked_at), [self._check_from_row(row) for row in rows]

    def recent_action_records(self, limit: int = 100) -> list[DynamicProfitProtectionCheck]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE triggered = 1 OR close_status != 'not_required' ORDER BY checked_at DESC, id DESC LIMIT ?", (int(limit),)).fetchall()
        return [self._check_from_row(row) for row in rows]

    @staticmethod
    def _check_from_row(row: sqlite3.Row) -> DynamicProfitProtectionCheck:
        return DynamicProfitProtectionCheck(**{**dict(row), "triggered": bool(row["triggered"]), "eligible": bool(row["eligible"])})

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
