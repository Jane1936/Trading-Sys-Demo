"""15-minute trailing reduction pre-trigger tracker.

After the normal position-reduction module runs for a closed 15m candle, this
module scans active positions. If a position's unrealized profit is less than
2R, it compares the current mark price with the lower low of the latest two
stored 15m candles. A price break below that level is recorded with the
``预触发结构破位`` tag for web display and downstream visibility.
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
from holding_position_scoring import HoldingPositionScoringSystem
from trade_action_lock import acquire_trade_action_lock
from trading_experiment import ExperimentConfig, TradingExperiment


@dataclass(frozen=True)
class TrailingReductionCheck:
    id: int
    symbol: str
    decision_round_ts: int
    checked_at: int
    account_equity_usdt: str
    r_usdt: str
    trigger_r_usdt: str
    unrealized_pnl: str
    current_price: str
    latest_15m_low: str
    second_15m_low: str
    lowest_15m_low: str
    atr14: str
    latest_1m_high: str
    latest_1m_close: str
    highest_since_open: str
    price_drawdown: str
    structure_break_triggered: bool
    entry_price: str
    position_amt: str
    eligible: bool
    pretriggered: bool
    tag: str
    reason: str


class TrailingReductionTracker:
    """Record structure-break pre-triggers for profitable active holdings."""

    CHECKS_TABLE = "trailing_reduction_checks"
    TAG_PRE_TRIGGER_STRUCTURE_BREAK = "预触发结构破位"
    TRIGGER_R_MULTIPLE = Decimal("2")
    STRUCTURE_REDUCTION_FRACTION = Decimal("0.3")
    TAG_STRUCTURE_BREAK = "结构破位"
    RECORDS_TABLE = "trailing_reduction_records"

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
                    decision_round_ts INTEGER NOT NULL,
                    checked_at INTEGER NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    r_usdt TEXT NOT NULL,
                    trigger_r_usdt TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    latest_15m_low TEXT NOT NULL,
                    second_15m_low TEXT NOT NULL,
                    lowest_15m_low TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    eligible INTEGER NOT NULL,
                    pretriggered INTEGER NOT NULL,
                    tag TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_round ON {self.CHECKS_TABLE}(decision_round_ts DESC, symbol ASC)")
            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.CHECKS_TABLE})").fetchall()}
            for column, ddl in {
                "atr14": "TEXT NOT NULL DEFAULT ''",
                "latest_1m_high": "TEXT NOT NULL DEFAULT ''",
                "latest_1m_close": "TEXT NOT NULL DEFAULT ''",
                "highest_since_open": "TEXT NOT NULL DEFAULT ''",
                "price_drawdown": "TEXT NOT NULL DEFAULT ''",
                "structure_break_triggered": "INTEGER NOT NULL DEFAULT 0",
            }.items():
                if column not in columns:
                    conn.execute(f"ALTER TABLE {self.CHECKS_TABLE} ADD COLUMN {column} {ddl}")
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.RECORDS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    checked_at INTEGER NOT NULL,
                    latest_1m_high TEXT NOT NULL,
                    latest_1m_close TEXT NOT NULL,
                    highest_since_open TEXT NOT NULL,
                    atr14 TEXT NOT NULL,
                    price_drawdown TEXT NOT NULL,
                    reduction_percent TEXT NOT NULL,
                    original_quantity TEXT NOT NULL,
                    reduced_quantity TEXT NOT NULL,
                    remaining_quantity TEXT NOT NULL,
                    market_order_id TEXT NOT NULL DEFAULT '',
                    take_profit_order_id TEXT NOT NULL DEFAULT '',
                    stop_loss_order_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_created ON {self.RECORDS_TABLE}(checked_at DESC, symbol ASC)")

    def run_round(self, decision_round_ts: int | None = None) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
        equity = helper._fetch_experiment_usdt_equity()
        r_value = equity * self.config.risk_fraction
        trigger_r = r_value * self.TRIGGER_R_MULTIPLE
        positions = helper._fetch_and_store_positions()
        active_positions = [row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0]
        now = int(time.time() * 1000)
        round_ts = int(decision_round_ts) if decision_round_ts is not None else now
        checked = eligible = pretriggered = 0
        for position in active_positions:
            checked += 1
            is_eligible, is_pretriggered = self._evaluate_position(position, round_ts, now, equity, r_value, trigger_r)
            eligible += int(is_eligible)
            pretriggered += int(is_pretriggered)
        return {"checked": checked, "eligible": eligible, "pretriggered": pretriggered, "r_usdt": self._fmt_decimal(r_value), "trigger_r_usdt": self._fmt_decimal(trigger_r)}

    def _evaluate_position(self, position: dict[str, Any], round_ts: int, now: int, equity: Decimal, r_value: Decimal, trigger_r: Decimal) -> tuple[bool, bool]:
        exchange_symbol = str(position.get("symbol", "")).upper()
        symbol = self._base_symbol(exchange_symbol)
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
        unrealized_pnl = self._decimal_from(position.get("unRealizedProfit", position.get("unrealizedProfit")), Decimal("0"))
        current_price = self._decimal_from(position.get("markPrice"), Decimal("0"))
        latest_low = second_low = lowest = Decimal("0")
        eligible = amount != 0 and entry_price > 0 and trigger_r > 0 and unrealized_pnl < trigger_r
        pretriggered = False
        tag = ""
        reason = "unrealized_pnl_gte_2r"
        if eligible:
            try:
                latest_low, second_low = self._latest_two_15m_lows(symbol)
                lowest = min(latest_low, second_low)
                if current_price <= 0:
                    reason = "missing_current_price"
                elif current_price < lowest:
                    pretriggered = True
                    tag = self.TAG_PRE_TRIGGER_STRUCTURE_BREAK
                    reason = "current_price_lt_lowest_recent_two_15m_lows"
                else:
                    reason = "current_price_gte_lowest_recent_two_15m_lows"
            except Exception as exc:
                reason = f"trailing_reduction_failed: {type(exc).__name__}: {exc}"
        self._insert_check(symbol, round_ts, now, equity, r_value, trigger_r, unrealized_pnl, current_price, latest_low, second_low, lowest, entry_price, amount, eligible, pretriggered, tag, reason)
        return eligible, pretriggered

    def _latest_two_15m_lows(self, symbol: str) -> tuple[Decimal, Decimal]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT low FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 2",
                (symbol,),
            ).fetchall()
        if len(rows) < 2:
            raise RuntimeError("missing_recent_two_15m_klines")
        return self._decimal_from(rows[0]["low"], Decimal("0")), self._decimal_from(rows[1]["low"], Decimal("0"))

    def _insert_check(self, symbol: str, decision_round_ts: int, checked_at: int, equity: Decimal, r_value: Decimal, trigger_r: Decimal, unrealized_pnl: Decimal, current_price: Decimal, latest_low: Decimal, second_low: Decimal, lowest: Decimal, entry_price: Decimal, amount: Decimal, eligible: bool, pretriggered: bool, tag: str, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(
                f"INSERT INTO {self.CHECKS_TABLE} (symbol, decision_round_ts, checked_at, account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl, current_price, latest_15m_low, second_15m_low, lowest_15m_low, entry_price, position_amt, eligible, pretriggered, tag, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (symbol, int(decision_round_ts), int(checked_at), self._fmt_decimal(equity), self._fmt_decimal(r_value), self._fmt_decimal(trigger_r), self._fmt_decimal(unrealized_pnl), self._fmt_decimal(current_price), self._fmt_decimal(latest_low), self._fmt_decimal(second_low), self._fmt_decimal(lowest), self._fmt_decimal(entry_price), self._fmt_decimal(amount), int(eligible), int(pretriggered), tag, reason),
            )

    def get_latest_round_checks(self) -> tuple[int | None, list[TrailingReductionCheck]]:
        self.init_tables()
        with self._connect() as conn:
            latest = conn.execute(f"SELECT MAX(decision_round_ts) AS latest_round FROM {self.CHECKS_TABLE}").fetchone()
            latest_round = latest["latest_round"] if latest else None
            if latest_round is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.CHECKS_TABLE} WHERE decision_round_ts = ? ORDER BY symbol ASC, id DESC", (int(latest_round),)).fetchall()
        return int(latest_round), [self._check_from_row(row) for row in rows]

    def refresh_pretriggered_symbols(self) -> dict[str, Any]:
        self.account_manager.validate_config()
        self.init_tables()
        round_ts, checks = self.get_latest_round_checks()
        if round_ts is None:
            return {"round_ts": None, "refreshed": 0, "triggered": 0, "records": 0}
        positions = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)._fetch_and_store_positions()
        by_symbol = {self._base_symbol(p.get("symbol")): p for p in positions}
        refreshed = triggered = records = 0
        for check in checks:
            if not check.pretriggered:
                continue
            position = by_symbol.get(check.symbol)
            if not position:
                continue
            refreshed += 1
            did_trigger = self._refresh_one(check, position)
            triggered += int(did_trigger)
            records += int(did_trigger)
        return {**self.summary_payload(), "refreshed": refreshed, "triggered": triggered, "records": records}

    def _refresh_one(self, check: TrailingReductionCheck, position: dict[str, Any]) -> bool:
        now = int(time.time() * 1000)
        symbol = check.symbol
        atr14 = self._latest_atr14(symbol)
        high, close = self._latest_1m_high_close(symbol)
        open_time = self._latest_open_trade_created_at(symbol)
        highest = max(self._decimal_from(check.highest_since_open, Decimal("0")), self._highest_1m_high_since(symbol, open_time), high)
        drawdown = highest - close if highest > 0 and close > 0 else Decimal("0")
        structure_break = atr14 > 0 and drawdown > (atr14 * Decimal("2"))
        tag = self.TAG_STRUCTURE_BREAK if structure_break else check.tag
        reason = f"latest_1m_high_close_refreshed; drawdown={self._fmt_decimal(drawdown)}; threshold_2atr={self._fmt_decimal(atr14 * Decimal('2'))}"
        with self._connect() as conn:
            conn.execute(f"""
                UPDATE {self.CHECKS_TABLE}
                SET checked_at = ?, atr14 = ?, latest_1m_high = ?, latest_1m_close = ?,
                    highest_since_open = ?, price_drawdown = ?, structure_break_triggered = ?, tag = ?, reason = ?
                WHERE id = ?
            """, (now, self._fmt_decimal(atr14), self._fmt_decimal(high), self._fmt_decimal(close), self._fmt_decimal(highest), self._fmt_decimal(drawdown), int(structure_break), tag, reason, check.id))
        if not structure_break or self._has_structure_record(symbol, check.decision_round_ts):
            return False
        self._execute_structure_reduction(check.decision_round_ts, position, now, atr14, high, close, highest, drawdown)
        return True

    def _execute_structure_reduction(self, round_ts: int, position: dict[str, Any], now: int, atr14: Decimal, high: Decimal, close: Decimal, highest: Decimal, drawdown: Decimal) -> None:
        symbol = self._base_symbol(position.get("symbol"))
        exchange_symbol = str(position.get("symbol", f"{symbol}USDT")).upper()
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        side = "SELL" if amount > 0 else "BUY"
        original = abs(amount)
        reduced = remaining = Decimal("0")
        status = "submitted"
        market_order_id = take_profit_order_id = stop_loss_order_id = ""
        raw_parts: list[str] = []
        reason_parts = ["structure_break_drawdown_gt_2atr", "tag=结构破位", "market_reduce_30_percent"]
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(self.db_path, symbol, "trailing_reduction", "structure_reduction", now)
        if lock_handle is None:
            self._insert_record(symbol, round_ts, now, high, close, highest, atr14, drawdown, original, reduced, remaining, market_order_id, take_profit_order_id, stop_loss_order_id, "failed", lock_reason, "")
            return
        try:
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager, config=self.config)
            holding = HoldingPositionScoringSystem(self.db_path, account_manager=self.account_manager)
            info = helper._exchange_symbol_info(exchange_symbol)
            reduced = holding._floor_to_step(original * self.STRUCTURE_REDUCTION_FRACTION, info["step_size"])
            remaining = holding._floor_to_step(original - reduced, info["step_size"])
            if reduced <= 0 or remaining <= 0:
                raise RuntimeError("structure_reduction_quantity_rounded_to_zero")
            cancel_reason = holding._cancel_existing_exit_orders(exchange_symbol)
            reason_parts.append(cancel_reason)
            response = self.account_manager._signed_post("/fapi/v1/order", holding._market_close_order_params(exchange_symbol, side, reduced))
            raw_parts.append(str({"market_order": response}))
            market_order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            actual_qty, actual_entry = holding._current_position_quantity_and_entry(exchange_symbol, remaining, position)
            take_profit_order_id, tp_price, tp_reason = holding._replace_hard_take_profit_for_position(helper, exchange_symbol, symbol, side, actual_entry, actual_qty, info["tick_size"])
            holding._update_latest_open_trade_take_profit(symbol, take_profit_order_id, tp_price)
            reason_parts.append(tp_reason)
            stop_loss_order_id, sl_price, sl_reason = holding._replace_stop_loss_for_position(helper, exchange_symbol, symbol, side, actual_qty, info["tick_size"], close)
            holding._update_latest_open_trade_stop_loss(symbol, stop_loss_order_id, sl_price)
            reason_parts.append(sl_reason)
        except Exception as exc:
            status = "failed"
            reason_parts.append(f"structure_reduction_failed: {type(exc).__name__}: {exc}")
        finally:
            lock_manager.release(lock_handle)
        self._insert_record(symbol, round_ts, now, high, close, highest, atr14, drawdown, original, reduced, remaining, market_order_id, take_profit_order_id, stop_loss_order_id, status, "; ".join(reason_parts), " | ".join(raw_parts))

    def _insert_record(self, symbol, round_ts, now, high, close, highest, atr14, drawdown, original, reduced, remaining, market_order_id, take_profit_order_id, stop_loss_order_id, status, reason, raw):
        with self._connect() as conn:
            conn.execute(f"INSERT INTO {self.RECORDS_TABLE} (symbol, decision_round_ts, checked_at, latest_1m_high, latest_1m_close, highest_since_open, atr14, price_drawdown, reduction_percent, original_quantity, reduced_quantity, remaining_quantity, market_order_id, take_profit_order_id, stop_loss_order_id, status, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (symbol, int(round_ts), int(now), self._fmt_decimal(high), self._fmt_decimal(close), self._fmt_decimal(highest), self._fmt_decimal(atr14), self._fmt_decimal(drawdown), self._fmt_decimal(self.STRUCTURE_REDUCTION_FRACTION), self._fmt_decimal(original), self._fmt_decimal(reduced), self._fmt_decimal(remaining), market_order_id, take_profit_order_id, stop_loss_order_id, status, reason, raw))

    def summary_payload(self) -> dict[str, Any]:
        round_ts, checks = self.get_latest_round_checks()
        latest_pretrigger_rounds = self.latest_pretrigger_rounds()
        annotated_checks = []
        for check in checks:
            item = dict(check.__dict__)
            item["latest_pretrigger_round_ts"] = latest_pretrigger_rounds.get(check.symbol)
            annotated_checks.append(item)
        return {"round_ts": round_ts, "checks": annotated_checks, "records": [dict(r) for r in self.recent_action_records(decision_round_ts=round_ts)]}

    def latest_pretrigger_rounds(self) -> dict[str, int]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, MAX(decision_round_ts) AS latest_round
                FROM {self.CHECKS_TABLE}
                WHERE pretriggered = 1
                GROUP BY symbol
                """
            ).fetchall()
        return {str(row["symbol"]): int(row["latest_round"]) for row in rows if row["latest_round"] is not None}

    def recent_action_records(self, limit: int = 100, decision_round_ts: int | None = None) -> list[sqlite3.Row]:
        self.init_tables()
        with self._connect() as conn:
            if decision_round_ts is None:
                return conn.execute(f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY checked_at DESC, id DESC LIMIT ?", (limit,)).fetchall()
            return conn.execute(
                f"SELECT * FROM {self.RECORDS_TABLE} WHERE decision_round_ts = ? ORDER BY checked_at DESC, id DESC LIMIT ?",
                (int(decision_round_ts), limit),
            ).fetchall()

    def _has_structure_record(self, symbol: str, round_ts: int) -> bool:
        with self._connect() as conn:
            return conn.execute(f"SELECT 1 FROM {self.RECORDS_TABLE} WHERE symbol = ? AND decision_round_ts = ? LIMIT 1", (symbol, int(round_ts))).fetchone() is not None

    def _latest_atr14(self, symbol: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute("SELECT atr14 FROM atr_15m_indicators WHERE symbol = ? ORDER BY open_time DESC LIMIT 1", (symbol,)).fetchone()
        return self._decimal_from(row["atr14"], Decimal("0")) if row else Decimal("0")

    def _latest_1m_high_close(self, symbol: str) -> tuple[Decimal, Decimal]:
        with self._connect() as conn:
            row = conn.execute("SELECT high, close FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1", (symbol,)).fetchone()
        return (self._decimal_from(row["high"], Decimal("0")), self._decimal_from(row["close"], Decimal("0"))) if row else (Decimal("0"), Decimal("0"))

    def _highest_1m_high_since(self, symbol: str, since_ms: int) -> Decimal:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(high) AS high FROM klines_1m WHERE symbol = ? AND open_time >= ?", (symbol, int(since_ms))).fetchone()
        return self._decimal_from(row["high"], Decimal("0")) if row else Decimal("0")

    def _latest_open_trade_created_at(self, symbol: str) -> int:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT created_at FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
            return int(row["created_at"]) if row and row["created_at"] is not None else 0
        except Exception:
            return 0

    @staticmethod
    def _check_from_row(row: sqlite3.Row) -> TrailingReductionCheck:
        return TrailingReductionCheck(**{**dict(row), "eligible": bool(row["eligible"]), "pretriggered": bool(row["pretriggered"]), "structure_break_triggered": bool(row["structure_break_triggered"])})

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
