"""Trailing stop profit tracker for positions that already hit partial take-profit.

The tracker runs once per minute. It only evaluates symbols with a successful
partial take-profit record (meaning unrealized PnL has reached 2R before), reads
the latest stored 1m kline high/close from SQLite, maintains the highest price
since the position was opened, and closes the full position when the latest close
draws down by more than a volatility-adjusted ATR(14) multiple after the 15m
pre-trigger is present.
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
from trade_action_lock import TradeActionLockManager, acquire_trade_action_lock
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
    latest_1m_close: str
    highest_since_open: str
    atr14: str
    volatility: str
    price_drawdown: str
    pretriggered: bool
    tag: str
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
        TradeActionLockManager(self.db_path).init_table()
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
                    latest_1m_close TEXT NOT NULL DEFAULT '0',
                    highest_since_open TEXT NOT NULL DEFAULT '0',
                    atr14 TEXT NOT NULL DEFAULT '0',
                    volatility TEXT NOT NULL DEFAULT '0',
                    price_drawdown TEXT NOT NULL DEFAULT '0',
                    pretriggered INTEGER NOT NULL DEFAULT 0,
                    tag TEXT NOT NULL DEFAULT '',
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
                "latest_1m_close": "TEXT NOT NULL DEFAULT '0'",
                "highest_since_open": "TEXT NOT NULL DEFAULT '0'",
                "atr14": "TEXT NOT NULL DEFAULT '0'",
                "volatility": "TEXT NOT NULL DEFAULT '0'",
                "price_drawdown": "TEXT NOT NULL DEFAULT '0'",
                "pretriggered": "INTEGER NOT NULL DEFAULT 0",
                "tag": "TEXT NOT NULL DEFAULT ''",
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
            self._insert_check(symbol, now, entry_price, amount, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), now, None, Decimal("0"), Decimal("0"), Decimal("0"), False, False, "", "not_required", Decimal("0"), "", "not_required", False, "partial_take_profit_not_triggered", latest_1m_close=Decimal("0"), highest_since_open=Decimal("0"), atr14=Decimal("0"), volatility=Decimal("0"), price_drawdown=Decimal("0"), pretriggered=False, tag="")
            return False
        try:
            pretriggered, pretrigger_reason, latest_15m_low = self._is_pretriggered(symbol)
            high, close = self._latest_1m_high_close(symbol)
            pnl = self._unrealized_pnl_at_high(amount, entry_price, high)
            previous = self._previous_max(symbol, entry_price)
            if previous and previous[0] >= pnl:
                max_pnl, max_at = previous
                reason = "max_unrealized_pnl_unchanged"
            else:
                max_pnl, max_at = pnl, now
                reason = "max_unrealized_pnl_updated"
            open_time = self._latest_open_trade_created_at(symbol)
            highest_since_open = max(self._previous_highest_since_open(symbol, entry_price), self._highest_1m_high_since(symbol, open_time), high)
            atr14 = self._latest_atr14(symbol)
            volatility = self._trailing_stop_volatility(symbol, atr14)
            atr_multiple = self._atr_multiple_for_volatility(volatility)
            price_drawdown = highest_since_open - close if highest_since_open > 0 and close > 0 else Decimal("0")
            threshold = atr14 * atr_multiple
            in_profit_at_close = self._in_profit_at_price(amount, entry_price, close)
            should_trigger = pretriggered and atr14 > 0 and price_drawdown > threshold and in_profit_at_close
            cancel_order_id = ""
            cancel_status = "not_required"
            close_quantity = Decimal("0")
            close_order_id = ""
            close_status = "not_required"
            tag = "预触发移动追踪止盈" if pretriggered else ""
            if should_trigger:
                tag = "移动追踪止盈"
                if self._has_trailing_stop_close_record(symbol, entry_price):
                    reason = f"{reason}; trailing_stop_already_completed"
                    should_trigger = False
                else:
                    cancel_order_id, cancel_status, close_quantity, close_order_id, close_status, action_reason = self._execute_trailing_stop_close(exchange_symbol, symbol, amount, entry_price)
                    reason = f"{reason}; {pretrigger_reason}; drawdown_gt_{self._fmt_atr_multiple(atr_multiple)}atr(drawdown={self._fmt_decimal(price_drawdown)}, threshold={self._fmt_decimal(threshold)}, volatility={self._fmt_decimal(volatility)}); tag={tag}; {action_reason}"
            elif not pretriggered:
                reason = f"{reason}; {pretrigger_reason}"
            elif atr14 <= 0:
                reason = f"{reason}; missing_or_invalid_atr14"
            elif not in_profit_at_close:
                reason = f"{reason}; {pretrigger_reason}; latest_1m_close_not_in_profit(close={self._fmt_decimal(close)}, entry={self._fmt_decimal(entry_price)})"
            else:
                reason = f"{reason}; {pretrigger_reason}; drawdown_lte_{self._fmt_atr_multiple(atr_multiple)}atr(drawdown={self._fmt_decimal(price_drawdown)}, threshold={self._fmt_decimal(threshold)}, volatility={self._fmt_decimal(volatility)})"
            drawdown = self._current_profit_drawdown(pnl, max_pnl)
            self._insert_check(symbol, now, entry_price, amount, high, pnl, max_pnl, drawdown, max_at, None, threshold, close, latest_15m_low, False, should_trigger, cancel_order_id, cancel_status, close_quantity, close_order_id, close_status, True, reason, latest_1m_close=close, highest_since_open=highest_since_open, atr14=atr14, volatility=volatility, price_drawdown=price_drawdown, pretriggered=pretriggered, tag=tag)
            return True
        except Exception as exc:
            previous = self._previous_max(symbol, entry_price)
            max_pnl, max_at = previous if previous else (Decimal("0"), now)
            drawdown = self._current_profit_drawdown(Decimal("0"), max_pnl)
            self._insert_check(symbol, now, entry_price, amount, Decimal("0"), Decimal("0"), max_pnl, drawdown, max_at, None, Decimal("0"), Decimal("0"), Decimal("0"), False, False, "", "not_required", Decimal("0"), "", "not_required", True, f"trailing_stop_tracker_failed: {type(exc).__name__}: {exc}", latest_1m_close=Decimal("0"), highest_since_open=Decimal("0"), atr14=Decimal("0"), volatility=Decimal("0"), price_drawdown=Decimal("0"), pretriggered=False, tag="")
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
        cancel_status = "submitted"
        raw_parts: list[str] = []
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(
            self.db_path, symbol, "trailing_stop_tracker", "trailing_stop_close"
        )
        if lock_handle is None:
            return cancel_order_id, "failed", Decimal("0"), "", "failed", lock_reason
        try:
            cancel_failures: list[str] = []
            for endpoint, label in (
                ("/fapi/v1/allOpenOrders", "open_orders_cancel"),
                ("/fapi/v1/algoOpenOrders", "algo_orders_cancel"),
            ):
                try:
                    cancel_response = self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})
                    raw_parts.append(str({label: cancel_response}))
                except Exception as exc:
                    cancel_failures.append(f"{label}_failed: {type(exc).__name__}: {exc}")
            if cancel_failures:
                cancel_status = "failed"
                return cancel_order_id, cancel_status, Decimal("0"), "", "failed", "trailing_stop_cancel_existing_orders_failed: " + " | ".join(cancel_failures)
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            quantity = self._floor_to_step(abs(amount), exchange_info["step_size"])
            if quantity <= 0:
                raise RuntimeError("close_quantity_rounded_to_zero")
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
            raw_parts.append(str({"close_position": response}))
            close_order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
            for endpoint, label in (
                ("/fapi/v1/allOpenOrders", "post_close_open_orders_cancel"),
                ("/fapi/v1/algoOpenOrders", "post_close_algo_orders_cancel"),
            ):
                try:
                    cancel_response = self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})
                    raw_parts.append(str({label: cancel_response}))
                except Exception as exc:
                    return cancel_order_id, cancel_status, quantity, close_order_id, "failed", f"trailing_stop_post_close_cancel_failed: {label}: {type(exc).__name__}: {exc}; " + " | ".join(raw_parts)
            return cancel_order_id, cancel_status, quantity, close_order_id, "submitted", "trailing_stop_triggered_close_position; " + " | ".join(raw_parts)
        except Exception as exc:
            return cancel_order_id, cancel_status, Decimal("0"), "", "failed", f"trailing_stop_close_failed: {type(exc).__name__}: {exc}; " + " | ".join(raw_parts)
        finally:
            lock_manager.release(lock_handle)

    def _latest_1m_high_close(self, symbol: str) -> tuple[Decimal, Decimal]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT high, close FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row is None:
            raise RuntimeError("missing_1m_kline_high_close")
        high = self._decimal_from(row["high"], Decimal("0"))
        close = self._decimal_from(row["close"], Decimal("0"))
        if high <= 0 or close <= 0:
            raise RuntimeError("invalid_1m_kline_high_close")
        return high, close

    def _is_pretriggered(self, symbol: str) -> tuple[bool, str, Decimal]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT low, close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 4",
                (symbol,),
            ).fetchall()
        if len(rows) < 4:
            return False, "missing_recent_four_15m_klines", Decimal("0")
        latest_close = self._decimal_from(rows[0]["close"], Decimal("0"))
        latest_low = self._decimal_from(rows[0]["low"], Decimal("0"))
        reference_lows = [self._decimal_from(row["low"], Decimal("0")) for row in rows[1:4]]
        if latest_close <= 0 or latest_low <= 0 or any(low <= 0 for low in reference_lows):
            return False, "invalid_recent_four_15m_klines", latest_low
        threshold = min(reference_lows)
        if latest_close < threshold:
            return True, f"tag=预触发移动追踪止盈; latest_15m_close_lt_min_prev_three_lows(close={self._fmt_decimal(latest_close)}, min_low={self._fmt_decimal(threshold)})", latest_low
        return False, f"latest_15m_close_gte_min_prev_three_lows(close={self._fmt_decimal(latest_close)}, min_low={self._fmt_decimal(threshold)})", latest_low

    def _latest_atr14(self, symbol: str) -> Decimal:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT atr14 FROM atr_15m_indicators WHERE symbol = ? ORDER BY open_time DESC LIMIT 1", (symbol,)).fetchone()
        except sqlite3.OperationalError:
            return Decimal("0")
        return self._decimal_from(row["atr14"], Decimal("0")) if row else Decimal("0")

    def _second_latest_15m_close(self, symbol: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1 OFFSET 1",
                (symbol,),
            ).fetchone()
        return self._decimal_from(row["close"], Decimal("0")) if row else Decimal("0")

    def _trailing_stop_volatility(self, symbol: str, atr14: Decimal) -> Decimal:
        second_latest_close = self._second_latest_15m_close(symbol)
        if atr14 <= 0 or second_latest_close <= 0:
            return Decimal("0")
        return atr14 / second_latest_close

    @staticmethod
    def _atr_multiple_for_volatility(volatility: Decimal) -> Decimal:
        return Decimal("2") if volatility > Decimal("0.03") else Decimal("2.5")

    def _highest_1m_high_since(self, symbol: str, since_ms: int) -> Decimal:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(high) AS high FROM klines_1m WHERE symbol = ? AND open_time >= ?", (symbol, int(since_ms))).fetchone()
        return self._decimal_from(row["high"], Decimal("0")) if row else Decimal("0")

    def _latest_open_trade_created_at(self, symbol: str) -> int:
        try:
            with self._connect() as conn:
                row = conn.execute(f"SELECT created_at FROM {TradingExperiment.TRADES_TABLE} WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
            return int(row["created_at"]) if row and row["created_at"] is not None else 0
        except Exception:
            return 0

    @staticmethod
    def _unrealized_pnl_at_high(amount: Decimal, entry_price: Decimal, high: Decimal) -> Decimal:
        return amount * (high - entry_price) if amount > 0 else abs(amount) * (entry_price - high)

    @staticmethod
    def _in_profit_at_price(amount: Decimal, entry_price: Decimal, price: Decimal) -> bool:
        if amount > 0:
            return price > entry_price
        if amount < 0:
            return price < entry_price
        return False

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

    def _previous_highest_since_open(self, symbol: str, entry_price: Decimal) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT highest_since_open FROM {self.CHECKS_TABLE} WHERE symbol = ? AND entry_price = ? AND eligible = 1 ORDER BY checked_at DESC, id DESC LIMIT 1",
                (symbol, self._fmt_decimal(entry_price)),
            ).fetchone()
        return self._decimal_from(row["highest_since_open"], Decimal("0")) if row and "highest_since_open" in row.keys() else Decimal("0")

    def _insert_check(self, symbol: str, checked_at: int, entry: Decimal, amount: Decimal, high: Decimal, pnl: Decimal, max_pnl: Decimal, drawdown: Decimal, max_at: int, total_score: int | None, threshold: Decimal, current_mark_price: Decimal, latest_15m_low: Decimal, price_below_latest_15m_low: bool, trailing_triggered: bool, cancel_order_id: str, cancel_status: str, close_quantity: Decimal, close_order_id: str, close_status: str, eligible: bool, reason: str, *, latest_1m_close: Decimal, highest_since_open: Decimal, atr14: Decimal, volatility: Decimal, price_drawdown: Decimal, pretriggered: bool, tag: str) -> None:
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.CHECKS_TABLE} (symbol, checked_at, entry_price, position_amt, kline_high, unrealized_pnl_at_high, max_unrealized_pnl, current_profit_drawdown, max_unrealized_pnl_at, total_score, drawdown_threshold, current_mark_price, latest_15m_low, price_below_latest_15m_low, latest_1m_close, highest_since_open, atr14, volatility, price_drawdown, pretriggered, tag, trailing_stop_triggered, cancel_take_profit_order_id, cancel_status, close_quantity, close_order_id, close_status, eligible, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, checked_at, self._fmt_decimal(entry), self._fmt_decimal(amount), self._fmt_decimal(high), self._fmt_decimal(pnl), self._fmt_decimal(max_pnl), self._fmt_decimal(drawdown), int(max_at), total_score, self._fmt_decimal(threshold), self._fmt_decimal(current_mark_price), self._fmt_decimal(latest_15m_low), int(price_below_latest_15m_low), self._fmt_decimal(latest_1m_close), self._fmt_decimal(highest_since_open), self._fmt_decimal(atr14), self._fmt_decimal(volatility), self._fmt_decimal(price_drawdown), int(pretriggered), tag, int(trailing_triggered), cancel_order_id, cancel_status, self._fmt_decimal(close_quantity), close_order_id, close_status, int(eligible), reason))


    def refresh_pretriggered_symbols(self) -> dict[str, Any]:
        """Refresh latest 1m metrics for the current round's pre-triggered symbols."""
        self.account_manager.validate_config()
        self.init_tables()
        round_ts, checks = self.get_latest_round_checks()
        if round_ts is None:
            return {"round_ts": None, "checks": [], "records": [], "refreshed": 0, "triggered": 0, "created_records": 0}
        positions = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_and_store_positions()
        by_symbol = {self._base_symbol(position.get("symbol")): position for position in positions}
        refreshed = triggered = records = 0
        for check in checks:
            if not check.pretriggered:
                continue
            position = by_symbol.get(check.symbol)
            if not position:
                continue
            refreshed += 1
            did_trigger = self._refresh_pretriggered_check(check, position)
            triggered += int(did_trigger)
            records += int(did_trigger)
        return {**self.summary_payload(), "refreshed": refreshed, "triggered": triggered, "created_records": records}

    def _refresh_pretriggered_check(self, check: TrailingStopCheck, position: dict[str, Any]) -> bool:
        now = int(time.time() * 1000)
        symbol = check.symbol
        exchange_symbol = str(position.get("symbol", f"{symbol}USDT")).upper()
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(check.entry_price, self._decimal_from(position.get("entryPrice"), Decimal("0")))
        high, close = self._latest_1m_high_close(symbol)
        open_time = self._latest_open_trade_created_at(symbol)
        highest_since_open = max(self._decimal_from(check.highest_since_open, Decimal("0")), self._highest_1m_high_since(symbol, open_time), high)
        atr14 = self._latest_atr14(symbol)
        volatility = self._trailing_stop_volatility(symbol, atr14)
        atr_multiple = self._atr_multiple_for_volatility(volatility)
        price_drawdown = highest_since_open - close if highest_since_open > 0 and close > 0 else Decimal("0")
        threshold = atr14 * atr_multiple
        in_profit_at_close = self._in_profit_at_price(amount, entry_price, close)
        should_trigger = amount != 0 and entry_price > 0 and atr14 > 0 and price_drawdown > threshold and in_profit_at_close
        tag = "移动追踪止盈" if should_trigger else "预触发移动追踪止盈"
        cancel_order_id = check.cancel_take_profit_order_id
        cancel_status = check.cancel_status
        close_quantity = self._decimal_from(check.close_quantity, Decimal("0"))
        close_order_id = check.close_order_id
        close_status = check.close_status
        reason = f"manual_refresh_pretriggered; latest_1m_high_close_refreshed; drawdown={self._fmt_decimal(price_drawdown)}; threshold_{self._fmt_atr_multiple(atr_multiple)}atr={self._fmt_decimal(threshold)}; volatility={self._fmt_decimal(volatility)}"
        if should_trigger:
            if self._has_trailing_stop_close_record(symbol, entry_price):
                should_trigger = False
                tag = "预触发移动追踪止盈"
                reason = f"{reason}; trailing_stop_already_completed"
            else:
                cancel_order_id, cancel_status, close_quantity, close_order_id, close_status, action_reason = self._execute_trailing_stop_close(exchange_symbol, symbol, amount, entry_price)
                reason = f"{reason}; tag=移动追踪止盈; {action_reason}"
        elif atr14 <= 0:
            reason = f"{reason}; missing_or_invalid_atr14"
        elif amount == 0 or entry_price <= 0:
            reason = f"{reason}; missing_active_position_or_entry_price"
        elif not in_profit_at_close:
            reason = f"{reason}; latest_1m_close_not_in_profit(close={self._fmt_decimal(close)}, entry={self._fmt_decimal(entry_price)})"
        else:
            reason = f"{reason}; drawdown_lte_{self._fmt_atr_multiple(atr_multiple)}atr"
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE {self.CHECKS_TABLE}
                SET checked_at = ?, kline_high = ?, current_mark_price = ?, latest_1m_close = ?,
                    highest_since_open = ?, atr14 = ?, volatility = ?, price_drawdown = ?, drawdown_threshold = ?,
                    tag = ?, trailing_stop_triggered = ?, cancel_take_profit_order_id = ?, cancel_status = ?,
                    close_quantity = ?, close_order_id = ?, close_status = ?, reason = ?
                WHERE id = ?
                """,
                (
                    now,
                    self._fmt_decimal(high),
                    self._fmt_decimal(close),
                    self._fmt_decimal(close),
                    self._fmt_decimal(highest_since_open),
                    self._fmt_decimal(atr14),
                    self._fmt_decimal(volatility),
                    self._fmt_decimal(price_drawdown),
                    self._fmt_decimal(threshold),
                    tag,
                    int(should_trigger),
                    cancel_order_id,
                    cancel_status,
                    self._fmt_decimal(close_quantity),
                    close_order_id,
                    close_status,
                    reason,
                    check.id,
                ),
            )
        return should_trigger and close_status == "submitted"

    def summary_payload(self) -> dict[str, Any]:
        round_ts, checks = self.get_latest_round_checks()
        records = self.recent_action_records(limit=100)
        return {"round_ts": round_ts, "checks": [check.__dict__ for check in checks], "records": [record.__dict__ for record in records]}

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
        return TrailingStopCheck(**{**dict(row), "eligible": bool(row["eligible"]), "price_below_latest_15m_low": bool(row["price_below_latest_15m_low"]), "pretriggered": bool(row["pretriggered"]), "trailing_stop_triggered": bool(row["trailing_stop_triggered"])})

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

    @staticmethod
    def _fmt_atr_multiple(value: Decimal) -> str:
        return format(value.normalize(), "f").replace(".", "_")
