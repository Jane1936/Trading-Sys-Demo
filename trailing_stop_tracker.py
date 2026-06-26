"""Trailing stop profit tracker for positions that already hit partial take-profit.

The tracker runs once per minute. It only evaluates symbols with a successful
partial take-profit record (meaning unrealized PnL has reached 2R before), reads
the latest stored 1m kline high from SQLite, calculates current high-based
unrealized profit and drawdown, and persists the maximum high-based unrealized
profit seen for the same position.
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
from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment


@dataclass(frozen=True)
class TrailingStopCheck:
    id: int
    symbol: str
    checked_at: int
    entry_price: str
    position_amt: str
    kline_high: str
    unrealized_pnl_at_high: str
    max_unrealized_pnl: str
    current_profit_drawdown: str
    max_unrealized_pnl_at: int
    total_score: int | None
    drawdown_threshold: str
    current_mark_price: str
    latest_15m_low: str
    price_below_latest_15m_low: bool
    trailing_stop_triggered: bool
    cancel_take_profit_order_id: str
    cancel_status: str
    close_quantity: str
    close_order_id: str
    close_status: str
    eligible: bool
    reason: str


class TrailingStopTracker:
    """Maintain max high-based unrealized PnL after partial take-profit."""

    CHECKS_TABLE = "trailing_stop_profit_checks"

    def __init__(self, db_path: str = "data/klines.db", account_manager: BinanceAccountManager | None = None) -> None:
        self.db_path = db_path
        self.account_manager = account_manager or BinanceAccountManager()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def init_tables(self) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        PartialTakeProfitStrategy(db_path=self.db_path, account_manager=self.account_manager).init_tables()
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.CHECKS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    entry_price TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    kline_high TEXT NOT NULL,
                    unrealized_pnl_at_high TEXT NOT NULL,
                    max_unrealized_pnl TEXT NOT NULL,
                    current_profit_drawdown TEXT NOT NULL DEFAULT '0',
                    max_unrealized_pnl_at INTEGER NOT NULL,
                    total_score INTEGER,
                    drawdown_threshold TEXT NOT NULL DEFAULT '0',
                    current_mark_price TEXT NOT NULL DEFAULT '0',
                    latest_15m_low TEXT NOT NULL DEFAULT '0',
                    price_below_latest_15m_low INTEGER NOT NULL DEFAULT 0,
                    trailing_stop_triggered INTEGER NOT NULL DEFAULT 0,
                    cancel_take_profit_order_id TEXT NOT NULL DEFAULT '',
                    cancel_status TEXT NOT NULL DEFAULT 'not_required',
                    close_quantity TEXT NOT NULL DEFAULT '0',
                    close_order_id TEXT NOT NULL DEFAULT '',
                    close_status TEXT NOT NULL DEFAULT 'not_required',
                    eligible INTEGER NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.CHECKS_TABLE})").fetchall()}
            migrations = {
                "current_profit_drawdown": "TEXT NOT NULL DEFAULT '0'",
                "total_score": "INTEGER",
                "drawdown_threshold": "TEXT NOT NULL DEFAULT '0'",
                "current_mark_price": "TEXT NOT NULL DEFAULT '0'",
                "latest_15m_low": "TEXT NOT NULL DEFAULT '0'",
                "price_below_latest_15m_low": "INTEGER NOT NULL DEFAULT 0",
                "trailing_stop_triggered": "INTEGER NOT NULL DEFAULT 0",
                "cancel_take_profit_order_id": "TEXT NOT NULL DEFAULT ''",
                "cancel_status": "TEXT NOT NULL DEFAULT 'not_required'",
                "close_quantity": "TEXT NOT NULL DEFAULT '0'",
                "close_order_id": "TEXT NOT NULL DEFAULT ''",
                "close_status": "TEXT NOT NULL DEFAULT 'not_required'",
            }
            for column, definition in migrations.items():
                if column not in columns:
                    conn.execute(f"ALTER TABLE {self.CHECKS_TABLE} ADD COLUMN {column} {definition}")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_position ON {self.CHECKS_TABLE}(symbol ASC, entry_price ASC, checked_at DESC)")

    def run_round(self) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
        positions = helper._fetch_and_store_positions()
        active_positions = [row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0]
        now = int(time.time() * 1000)
        checked = eligible = updated = 0
        for position in active_positions:
            checked += 1
            result = self._evaluate_position(position, now)
            if result:
                eligible += 1
                updated += 1
        return {"checked": checked, "eligible": eligible, "updated": updated}

    def _evaluate_position(self, position: dict[str, Any], now: int) -> bool:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        if amount == 0 or entry_price <= 0:
            return False
        if not self._has_partial_take_profit_record(symbol, entry_price):
            self._insert_check(symbol, now, entry_price, amount, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), now, None, Decimal("0"), Decimal("0"), Decimal("0"), False, False, "", "not_required", Decimal("0"), "", "not_required", False, "partial_take_profit_not_triggered")
            return False
        try:
            high = self._latest_1m_high(symbol)
            latest_15m_low = self._latest_15m_low(symbol)
            current_price = self._decimal_from(position.get("markPrice"), Decimal("0"))
            if current_price <= 0:
                raise RuntimeError("invalid_current_mark_price")
            pnl = self._unrealized_pnl_at_high(amount, entry_price, high)
            previous = self._previous_max(symbol, entry_price)
            if previous and previous[0] >= pnl:
                max_pnl, max_at = previous
                reason = "max_unrealized_pnl_unchanged"
            else:
                max_pnl, max_at = pnl, now
                reason = "max_unrealized_pnl_updated"
            drawdown = self._current_profit_drawdown(pnl, max_pnl)
            total_score = self._latest_total_score(symbol)
            threshold = self._drawdown_threshold(total_score)
            price_below_latest_15m_low = current_price < latest_15m_low
            should_trigger = threshold > 0 and drawdown >= threshold and price_below_latest_15m_low
            cancel_order_id = ""
            cancel_status = "not_required"
            close_quantity = Decimal("0")
            close_order_id = ""
            close_status = "not_required"
            if should_trigger:
                if self._has_trailing_stop_close_record(symbol, entry_price):
                    reason = f"{reason}; trailing_stop_already_completed"
                    should_trigger = False
                else:
                    cancel_order_id, cancel_status, close_quantity, close_order_id, close_status, action_reason = self._execute_trailing_stop_close(exchange_symbol, symbol, amount, entry_price)
                    reason = f"{reason}; {action_reason}"
            elif total_score is None:
                reason = f"{reason}; missing_total_score"
            elif threshold <= 0 or drawdown < threshold:
                reason = f"{reason}; drawdown_below_threshold"
            else:
                reason = f"{reason}; current_price_not_below_latest_15m_low(current_price={self._fmt_decimal(current_price)}, latest_15m_low={self._fmt_decimal(latest_15m_low)})"
            self._insert_check(symbol, now, entry_price, amount, high, pnl, max_pnl, drawdown, max_at, total_score, threshold, current_price, latest_15m_low, price_below_latest_15m_low, should_trigger, cancel_order_id, cancel_status, close_quantity, close_order_id, close_status, True, reason)
            return True
        except Exception as exc:
            previous = self._previous_max(symbol, entry_price)
            max_pnl, max_at = previous if previous else (Decimal("0"), now)
            drawdown = self._current_profit_drawdown(Decimal("0"), max_pnl)
            self._insert_check(symbol, now, entry_price, amount, Decimal("0"), Decimal("0"), max_pnl, drawdown, max_at, None, Decimal("0"), Decimal("0"), Decimal("0"), False, False, "", "not_required", Decimal("0"), "", "not_required", True, f"trailing_stop_tracker_failed: {type(exc).__name__}: {exc}")
            return False

    def _has_partial_take_profit_record(self, symbol: str, entry_price: Decimal) -> bool:
        table = PartialTakeProfitStrategy.RECORDS_TABLE
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {table} WHERE symbol = ? AND entry_price = ? AND status = 'submitted' LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return row is not None

    def _latest_total_score(self, symbol: str) -> int | None:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT total_score FROM symbol_total_scores WHERE symbol = ? ORDER BY decision_round_ts DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.OperationalError:
            return None
        return int(row["total_score"]) if row and row["total_score"] is not None else None

    @staticmethod
    def _drawdown_threshold(total_score: int | None) -> Decimal:
        if total_score is None:
            return Decimal("0")
        if total_score >= 80:
            return Decimal("0.22")
        if total_score >= 65:
            return Decimal("0.16")
        return Decimal("0.10")

    def _has_trailing_stop_close_record(self, symbol: str, entry_price: Decimal) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.CHECKS_TABLE} WHERE symbol = ? AND entry_price = ? AND trailing_stop_triggered = 1 AND close_status = 'submitted' LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return row is not None

    def _latest_take_profit_order_id(self, symbol: str, entry_price: Decimal) -> str:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT take_profit_order_id FROM {TradingExperiment.TRADES_TABLE}
                WHERE symbol = ? AND entry_price = ? AND status = 'opened' AND take_profit_order_id != ''
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return str(row["take_profit_order_id"]) if row else ""

    def _execute_trailing_stop_close(self, exchange_symbol: str, symbol: str, amount: Decimal, entry_price: Decimal) -> tuple[str, str, Decimal, str, str, str]:
        side = "SELL" if amount > 0 else "BUY"
        cancel_order_id = self._latest_take_profit_order_id(symbol, entry_price)
        cancel_status = "missing"
        raw_parts: list[str] = []
        if cancel_order_id:
            try:
                cancel_response = self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": cancel_order_id})
                cancel_status = "submitted"
                raw_parts.append(str({"cancel_take_profit": cancel_response}))
            except Exception as exc:
                cancel_status = "failed"
                return cancel_order_id, cancel_status, Decimal("0"), "", "failed", f"trailing_stop_cancel_take_profit_failed: {type(exc).__name__}: {exc}"
        try:
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            quantity = self._floor_to_step(abs(amount), exchange_info["step_size"])
            if quantity <= 0:
                raise RuntimeError("close_quantity_rounded_to_zero")
            limit_params = helper._limit_ioc_order_params(
                {
                    "symbol": exchange_symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": self._fmt_decimal(quantity),
                    "reduceOnly": "true",
                },
                trading_symbol=exchange_symbol,
                tick_size=exchange_info["tick_size"],
            )
            response = self.account_manager._signed_post("/fapi/v1/order", limit_params)
            raw_parts.append(str({"close_position": response}))
            close_order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
            return cancel_order_id, cancel_status, quantity, close_order_id, "submitted", "trailing_stop_triggered_close_position; " + " | ".join(raw_parts)
        except Exception as exc:
            return cancel_order_id, cancel_status, Decimal("0"), "", "failed", f"trailing_stop_close_failed: {type(exc).__name__}: {exc}; " + " | ".join(raw_parts)

    def _latest_1m_high(self, symbol: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT high FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row is None:
            raise RuntimeError("missing_1m_kline_high")
        high = self._decimal_from(row["high"], Decimal("0"))
        if high <= 0:
            raise RuntimeError("invalid_1m_kline_high")
        return high

    def _latest_15m_low(self, symbol: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT low FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row is None:
            raise RuntimeError("missing_15m_kline_low")
        low = self._decimal_from(row["low"], Decimal("0"))
        if low <= 0:
            raise RuntimeError("invalid_15m_kline_low")
        return low

    @staticmethod
    def _unrealized_pnl_at_high(amount: Decimal, entry_price: Decimal, high: Decimal) -> Decimal:
        return amount * (high - entry_price) if amount > 0 else abs(amount) * (entry_price - high)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    @staticmethod
    def _current_profit_drawdown(pnl: Decimal, max_pnl: Decimal) -> Decimal:
        if max_pnl <= 0:
            return Decimal("0")
        drawdown = (max_pnl - pnl) / max_pnl
        return max(drawdown, Decimal("0"))

    def _previous_max(self, symbol: str, entry_price: Decimal) -> tuple[Decimal, int] | None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT max_unrealized_pnl, max_unrealized_pnl_at FROM {self.CHECKS_TABLE} WHERE symbol = ? AND entry_price = ? AND eligible = 1 ORDER BY checked_at DESC, id DESC LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        if not row:
            return None
        return self._decimal_from(row["max_unrealized_pnl"], Decimal("0")), int(row["max_unrealized_pnl_at"] or 0)

    def _insert_check(self, symbol: str, checked_at: int, entry: Decimal, amount: Decimal, high: Decimal, pnl: Decimal, max_pnl: Decimal, drawdown: Decimal, max_at: int, total_score: int | None, threshold: Decimal, current_mark_price: Decimal, latest_15m_low: Decimal, price_below_latest_15m_low: bool, trailing_triggered: bool, cancel_order_id: str, cancel_status: str, close_quantity: Decimal, close_order_id: str, close_status: str, eligible: bool, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, entry_price, position_amt, kline_high, unrealized_pnl_at_high, max_unrealized_pnl, current_profit_drawdown, max_unrealized_pnl_at, total_score, drawdown_threshold, current_mark_price, latest_15m_low, price_below_latest_15m_low, trailing_stop_triggered, cancel_take_profit_order_id, cancel_status, close_quantity, close_order_id, close_status, eligible, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, self._fmt_decimal(entry), self._fmt_decimal(amount), self._fmt_decimal(high), self._fmt_decimal(pnl), self._fmt_decimal(max_pnl), self._fmt_decimal(drawdown), int(max_at), total_score, self._fmt_decimal(threshold), self._fmt_decimal(current_mark_price), self._fmt_decimal(latest_15m_low), int(price_below_latest_15m_low), int(trailing_triggered), cancel_order_id, cancel_status, self._fmt_decimal(close_quantity), close_order_id, close_status, int(eligible), reason))

    def get_latest_round_checks(self) -> tuple[int | None, list[TrailingStopCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(checked_at) AS latest_checked_at FROM {self.CHECKS_TABLE}").fetchone()
            latest_checked_at = latest["latest_checked_at"] if latest else None
            if latest_checked_at is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE checked_at = ? ORDER BY symbol ASC, id DESC", (int(latest_checked_at),)).fetchall()
        return int(latest_checked_at), [self._check_from_row(row) for row in rows]

    def recent_action_records(self, limit: int = 100) -> list[TrailingStopCheck]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self.CHECKS_TABLE}
                WHERE trailing_stop_triggered = 1
                   OR cancel_status != 'not_required'
                   OR close_status != 'not_required'
                ORDER BY checked_at DESC, id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [self._check_from_row(row) for row in rows]

    @staticmethod
    def _check_from_row(row: sqlite3.Row) -> TrailingStopCheck:
        return TrailingStopCheck(**{**dict(row), "eligible": bool(row["eligible"]), "price_below_latest_15m_low": bool(row["price_below_latest_15m_low"]), "trailing_stop_triggered": bool(row["trailing_stop_triggered"])})

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
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.normalize(), "f")
