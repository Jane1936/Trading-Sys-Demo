"""Holding-position scoring system for 15-minute stop-loss decisions.

This module is intentionally independent from ``scoring_system.py``.  It
queries current Binance Futures positions, compares the latest two closed 15m
candles with the latest two structural stop-loss rows, and immediately submits
an opposite MARKET order with ``reduceOnly=true`` when both candles close below
their corresponding structural stop-loss levels.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable

from binance_account_manager import BinanceAccountManager
from openable_symbol_module import OpenableSymbol
from trading_experiment import TradingExperiment
from trade_action_lock import TradeActionLockManager, acquire_trade_action_lock


@dataclass(frozen=True)
class HoldingStopLossCheck:
    symbol: str
    decision_round_ts: int
    triggered: bool
    latest_15m_open_time: int | None
    latest_15m_close: float | None
    latest_structural_stop_loss: float | None
    prev_15m_open_time: int | None
    prev_15m_close: float | None
    prev_structural_stop_loss: float | None
    reason: str
    checked_at: int


@dataclass(frozen=True)
class PortfolioRiskPosition:
    symbol: str
    decision_round_ts: int
    position_amt: str
    latest_15m_close: str
    account_equity_usdt: str
    leverage: str
    risk: str
    total_score: str
    reason: str
    calculated_at: int


@dataclass(frozen=True)
class PortfolioRiskSummary:
    decision_round_ts: int
    total_risk: str
    position_count: int
    account_equity_usdt: str
    calculated_at: int
    positions: list[PortfolioRiskPosition]


@dataclass(frozen=True)
class PositionReductionCheck:
    symbol: str
    decision_round_ts: int
    highest_15m_high: str
    current_price: str
    price_drawdown_ratio: str
    account_equity_usdt: str
    two_r_usdt: str
    one_r_usdt: str
    unrealized_pnl: str
    open_total_score: str
    latest_total_score: str
    score_drawdown: str
    latest_15m_open: str
    latest_15m_close: str
    previous_total_score: str
    recent_score_drawdown: str
    open_entry_price: str
    rule_name: str
    triggered: bool
    tag: str
    reason: str
    checked_at: int


@dataclass(frozen=True)
class PositionIncreaseCheck:
    symbol: str
    decision_round_ts: int
    current_price: str
    account_equity_usdt: str
    one_r_usdt: str
    unrealized_pnl: str
    latest_total_score: str
    previous_total_score: str
    latest_reduction_price: str
    open_trade_created_at: str
    triggered: bool
    tag: str
    reason: str
    checked_at: int


@dataclass(frozen=True)
class PositionIncreaseRecord:
    id: int
    symbol: str
    decision_round_ts: int
    action_name: str
    current_price: str
    unrealized_pnl: str
    one_r_usdt: str
    latest_total_score: str
    previous_total_score: str
    latest_reduction_price: str
    status: str
    reason: str
    created_at: int
    original_quantity: str = ""
    increased_quantity: str = ""
    required_margin_usdt: str = ""
    available_experiment_usdt: str = ""
    market_order_id: str = ""
    raw_response: str = ""


@dataclass(frozen=True)
class HoldingStopLossRecord:
    id: int
    symbol: str
    decision_round_ts: int
    side: str
    quantity: str
    latest_15m_close: float
    latest_structural_stop_loss: float
    prev_15m_close: float
    prev_structural_stop_loss: float
    status: str
    order_id: str
    realized_pnl: str
    reason: str
    raw_response: str
    created_at: int


@dataclass(frozen=True)
class PositionReductionRecord:
    id: int
    symbol: str
    decision_round_ts: int
    side: str
    matched_rule: str
    reduction_percent: str
    original_quantity: str
    reduced_quantity: str
    remaining_quantity: str
    old_limit_order_id: str
    new_limit_order_id: str
    market_order_id: str
    limit_price: str
    status: str
    reason: str
    raw_response: str
    created_at: int


class HoldingPositionScoringSystem:
    """Evaluate held symbols and execute structural stop-loss exits."""

    CHECKS_TABLE = "holding_stop_loss_checks"
    RECORDS_TABLE = "holding_stop_loss_records"
    PORTFOLIO_RISK_TABLE = "holding_portfolio_risk_checks"
    PORTFOLIO_RISK_SUMMARY_TABLE = "holding_portfolio_risk_summaries"
    REDUCTION_CHECKS_TABLE = "holding_position_reduction_checks"
    REDUCTION_RECORDS_TABLE = "holding_position_reduction_records"
    INCREASE_CHECKS_TABLE = "holding_position_increase_checks"
    INCREASE_RECORDS_TABLE = "holding_position_increase_records"
    INCREASE_TAG_FIRST = "第一次加仓"
    INCREASE_TAG_PRE_TRIGGER = "预触发"
    INCREASE_TAG_FIRST_COMPLETED = "已完成第一次加仓"
    REDUCTION_TAG_ABSOLUTE_SCORE_DRAWDOWN = "绝对分数大幅回撤"
    REDUCTION_TAG_MEDIUM_DRAWDOWN_WEAKENING = "中等回撤且趋势连续弱化"
    REDUCTION_TAG_SCORE_DANGER_ZONE = "评分进入危险区"
    REDUCTION_TAG_MEDIUM_DANGER_PRICE_CONFIRMATION = "中危险区+价格确认"
    REDUCTION_TAG_DEEP_WEAKNESS = "深度弱势"

    def __init__(
        self,
        db_path: str = "data/klines.db",
        account_manager: BinanceAccountManager | None = None,
        realized_pnl_retry_delays: Iterable[float] = (1, 3, 5),
    ) -> None:
        self.db_path = db_path
        self.account_manager = account_manager or BinanceAccountManager()
        self.realized_pnl_retry_delays = tuple(realized_pnl_retry_delays)

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
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    triggered INTEGER NOT NULL,
                    latest_15m_open_time INTEGER,
                    latest_15m_close REAL,
                    latest_structural_stop_loss REAL,
                    prev_15m_open_time INTEGER,
                    prev_15m_close REAL,
                    prev_structural_stop_loss REAL,
                    reason TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.RECORDS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    latest_15m_close REAL NOT NULL,
                    latest_structural_stop_loss REAL NOT NULL,
                    prev_15m_close REAL NOT NULL,
                    prev_structural_stop_loss REAL NOT NULL,
                    status TEXT NOT NULL,
                    order_id TEXT NOT NULL DEFAULT '',
                    realized_pnl TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.PORTFOLIO_RISK_TABLE} (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    position_amt TEXT NOT NULL,
                    latest_15m_close TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    leverage TEXT NOT NULL,
                    risk TEXT NOT NULL,
                    total_score TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    calculated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.REDUCTION_CHECKS_TABLE} (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    highest_15m_high TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    price_drawdown_ratio TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    two_r_usdt TEXT NOT NULL,
                    one_r_usdt TEXT NOT NULL DEFAULT '',
                    unrealized_pnl TEXT NOT NULL,
                    open_total_score TEXT NOT NULL,
                    latest_total_score TEXT NOT NULL,
                    score_drawdown TEXT NOT NULL,
                    latest_15m_open TEXT NOT NULL DEFAULT '',
                    latest_15m_close TEXT NOT NULL DEFAULT '',
                    previous_total_score TEXT NOT NULL DEFAULT '',
                    recent_score_drawdown TEXT NOT NULL DEFAULT '',
                    open_entry_price TEXT NOT NULL DEFAULT '',
                    rule_name TEXT NOT NULL DEFAULT '',
                    triggered INTEGER NOT NULL,
                    tag TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.PORTFOLIO_RISK_SUMMARY_TABLE} (
                    decision_round_ts INTEGER PRIMARY KEY,
                    total_risk TEXT NOT NULL,
                    position_count INTEGER NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    calculated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.REDUCTION_RECORDS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    matched_rule TEXT NOT NULL,
                    reduction_percent TEXT NOT NULL,
                    original_quantity TEXT NOT NULL,
                    reduced_quantity TEXT NOT NULL,
                    remaining_quantity TEXT NOT NULL,
                    old_limit_order_id TEXT NOT NULL DEFAULT '',
                    new_limit_order_id TEXT NOT NULL DEFAULT '',
                    market_order_id TEXT NOT NULL DEFAULT '',
                    limit_price TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.INCREASE_CHECKS_TABLE} (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    current_price TEXT NOT NULL,
                    account_equity_usdt TEXT NOT NULL,
                    one_r_usdt TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    latest_total_score TEXT NOT NULL,
                    previous_total_score TEXT NOT NULL,
                    latest_reduction_price TEXT NOT NULL DEFAULT '',
                    open_trade_created_at TEXT NOT NULL DEFAULT '',
                    triggered INTEGER NOT NULL,
                    tag TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.INCREASE_RECORDS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    action_name TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    one_r_usdt TEXT NOT NULL,
                    latest_total_score TEXT NOT NULL,
                    previous_total_score TEXT NOT NULL,
                    latest_reduction_price TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    original_quantity TEXT NOT NULL DEFAULT '',
                    increased_quantity TEXT NOT NULL DEFAULT '',
                    required_margin_usdt TEXT NOT NULL DEFAULT '',
                    available_experiment_usdt TEXT NOT NULL DEFAULT '',
                    market_order_id TEXT NOT NULL DEFAULT '',
                    raw_response TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.REDUCTION_CHECKS_TABLE}_round "
                f"ON {self.REDUCTION_CHECKS_TABLE}(decision_round_ts DESC, triggered DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_round "
                f"ON {self.CHECKS_TABLE}(decision_round_ts DESC, triggered DESC, symbol ASC)"
            )
            record_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.RECORDS_TABLE})").fetchall()}
            if "realized_pnl" not in record_columns:
                conn.execute(f"ALTER TABLE {self.RECORDS_TABLE} ADD COLUMN realized_pnl TEXT NOT NULL DEFAULT ''")
            risk_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.PORTFOLIO_RISK_TABLE})").fetchall()}
            if "total_score" not in risk_columns:
                conn.execute(f"ALTER TABLE {self.PORTFOLIO_RISK_TABLE} ADD COLUMN total_score TEXT NOT NULL DEFAULT ''")
            reduction_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.REDUCTION_CHECKS_TABLE})").fetchall()}
            for column, ddl in {
                "latest_15m_open": "TEXT NOT NULL DEFAULT ''",
                "latest_15m_close": "TEXT NOT NULL DEFAULT ''",
                "previous_total_score": "TEXT NOT NULL DEFAULT ''",
                "recent_score_drawdown": "TEXT NOT NULL DEFAULT ''",
                "rule_name": "TEXT NOT NULL DEFAULT ''",
                "one_r_usdt": "TEXT NOT NULL DEFAULT ''",
                "open_entry_price": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if column not in reduction_columns:
                    conn.execute(f"ALTER TABLE {self.REDUCTION_CHECKS_TABLE} ADD COLUMN {column} {ddl}")
            increase_record_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.INCREASE_RECORDS_TABLE})").fetchall()}
            for column, ddl in {
                "original_quantity": "TEXT NOT NULL DEFAULT ''",
                "increased_quantity": "TEXT NOT NULL DEFAULT ''",
                "required_margin_usdt": "TEXT NOT NULL DEFAULT ''",
                "available_experiment_usdt": "TEXT NOT NULL DEFAULT ''",
                "market_order_id": "TEXT NOT NULL DEFAULT ''",
                "raw_response": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if column not in increase_record_columns:
                    conn.execute(f"ALTER TABLE {self.INCREASE_RECORDS_TABLE} ADD COLUMN {column} {ddl}")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_created "
                f"ON {self.RECORDS_TABLE}(created_at DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.REDUCTION_RECORDS_TABLE}_created "
                f"ON {self.REDUCTION_RECORDS_TABLE}(created_at DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.INCREASE_CHECKS_TABLE}_round "
                f"ON {self.INCREASE_CHECKS_TABLE}(decision_round_ts DESC, triggered DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.INCREASE_RECORDS_TABLE}_created "
                f"ON {self.INCREASE_RECORDS_TABLE}(created_at DESC, symbol ASC)"
            )

    def run_round(self, decision_round_ts: int | None = None) -> dict[str, Any]:
        """Run one 15m holding-position round.

        The round is intentionally sequenced so the position-reduction module
        only starts after the stop-loss rule has finished evaluating and
        persisting the current round's judgement results.
        """
        self.account_manager.validate_config()
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        now_ms = int(time.time() * 1000)
        positions = self._active_positions()
        stop_loss_result = self._run_stop_loss_rule_round(positions=positions, decision_round_ts=round_ts, checked_at=now_ms)
        reduction_positions = self._positions_after_stop_loss_records_are_persisted(
            positions=positions,
            stop_loss_records=stop_loss_result["records"],
        )
        reduction_checks = self.evaluate_reduction_conditions(positions=reduction_positions, decision_round_ts=round_ts, checked_at=now_ms)
        reduction_records = self._execute_reduction_actions(reduction_checks, reduction_positions, now_ms)
        increase_checks = self.evaluate_increase_conditions(positions=reduction_positions, decision_round_ts=round_ts, checked_at=now_ms)
        increase_records = self._record_increase_actions(increase_checks, reduction_positions, now_ms)
        portfolio_risk = self.calculate_portfolio_risk(positions=reduction_positions, decision_round_ts=round_ts, calculated_at=now_ms)
        return {
            "decision_round_ts": round_ts,
            "checked": stop_loss_result["checked"],
            "triggered": stop_loss_result["triggered"],
            "records": stop_loss_result["records"],
            "reduction_checked": len(reduction_checks),
            "reduction_triggered": sum(1 for row in reduction_checks if row.triggered),
            "reduction_records": reduction_records,
            "increase_checked": len(increase_checks),
            "increase_triggered": sum(1 for row in increase_checks if row.triggered),
            "increase_records": increase_records,
            "total_risk": portfolio_risk.total_risk,
            "risk_position_count": portfolio_risk.position_count,
            "portfolio_risk_action": "",
        }

    def _run_stop_loss_rule_round(
        self,
        positions: list[dict[str, Any]],
        decision_round_ts: int,
        checked_at: int,
    ) -> dict[str, int]:
        """Evaluate and persist stop-loss judgement results before dependants run."""
        round_ts = decision_round_ts
        now_ms = checked_at
        checked = 0
        triggered = 0
        records = 0
        for position in positions:
            exchange_symbol = str(position.get("symbol", "")).strip()
            symbol = self._base_symbol(exchange_symbol)
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            if not symbol or amount == 0:
                continue
            check = self.evaluate_symbol(symbol=symbol, decision_round_ts=round_ts, checked_at=now_ms)
            self._save_check(check)
            checked += 1
            if check.triggered:
                triggered += 1
                if self._has_stop_loss_record(symbol, round_ts):
                    continue
                record = self._execute_stop_loss(position, check, now_ms)
                self._save_record(record)
                records += 1
        return {
            "checked": checked,
            "triggered": triggered,
            "records": records,
        }

    def _positions_after_stop_loss_records_are_persisted(
        self,
        positions: list[dict[str, Any]],
        stop_loss_records: int,
    ) -> list[dict[str, Any]]:
        """Return holdings for downstream modules after stop-loss records are durable.

        Stop-loss execution can close or shrink a position.  Re-reading active
        positions after at least one stop-loss record has been saved prevents the
        reduction module from acting on a stale pre-stop-loss snapshot.
        """
        if stop_loss_records <= 0:
            return positions
        return self._active_positions()

    def _has_total_scores_for_round(self, decision_round_ts: int) -> bool:
        """Return True only after the scoring system has persisted this round's totals."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM symbol_total_scores WHERE decision_round_ts = ? LIMIT 1",
                    (int(decision_round_ts),),
                ).fetchone()
        except sqlite3.OperationalError:
            return False
        return row is not None

    def _has_ema_for_round(self, decision_round_ts: int) -> bool:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM ema_indicators WHERE interval = '15m' AND open_time <= ? LIMIT 1",
                    (int(decision_round_ts),),
                ).fetchone()
        except sqlite3.OperationalError:
            return True
        return row is not None

    def _latest_15m_ema21(self, symbol: str, round_ts: int) -> Decimal:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT ema21 FROM ema_indicators WHERE symbol = ? AND interval = '15m' AND open_time <= ? ORDER BY open_time DESC LIMIT 1",
                    (symbol, int(round_ts)),
                ).fetchone()
        except sqlite3.OperationalError:
            return Decimal("Infinity")
        except sqlite3.Error:
            return Decimal("0")
        return self._decimal_from(row["ema21"], Decimal("0")) if row else Decimal("0")

    def evaluate_reduction_conditions(
        self,
        positions: list[dict[str, Any]] | None = None,
        decision_round_ts: int | None = None,
        checked_at: int | None = None,
    ) -> list[PositionReductionCheck]:
        """Evaluate 15m position-reduction condition rules for active holdings."""
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        if not self._has_total_scores_for_round(round_ts) or not self._has_ema_for_round(round_ts):
            return []
        now_ms = checked_at if checked_at is not None else int(time.time() * 1000)
        active_positions = positions if positions is not None else self._active_positions()
        equity = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_experiment_usdt_equity()
        one_r = equity * Decimal("0.01")
        two_r = equity * Decimal("0.02")
        checks: list[PositionReductionCheck] = []
        for position in active_positions:
            symbol = self._base_symbol(str(position.get("symbol", "")))
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            if not symbol or amount == 0:
                continue
            check = self._evaluate_reduction_rules(position, symbol, round_ts, equity, one_r, two_r, now_ms)
            self._save_reduction_check(check)
            checks.append(check)
        return checks

    def _evaluate_reduction_rules(
        self, position: dict[str, Any], symbol: str, round_ts: int, equity: Decimal, one_r: Decimal, two_r: Decimal, now_ms: int
    ) -> PositionReductionCheck:
        highest = self._highest_recent_15m_high(symbol, limit=3)
        recent_15m_open_closes = self._recent_15m_open_closes(symbol, limit=3)
        latest_kline = recent_15m_open_closes[0] if recent_15m_open_closes else None
        exchange_symbol = self._exchange_symbol(position, symbol)
        current_price = self._current_symbol_price(exchange_symbol, position)
        drawdown = (highest - current_price) / highest if highest > 0 and current_price > 0 else Decimal("0")
        unrealized_pnl = self._position_unrealized_pnl(position)
        recent_scores = self._recent_total_scores(symbol, limit=3)
        latest_score = recent_scores[0] if recent_scores else ""
        previous_score = recent_scores[1] if len(recent_scores) > 1 else ""
        previous_previous_score = recent_scores[2] if len(recent_scores) > 2 else ""
        open_score = self._latest_open_trade_total_score(symbol)
        open_entry_price = self._latest_open_trade_entry_price(symbol)
        open_trade_created_at = self._latest_open_trade_created_at(symbol)
        score_drawdown = (
            self._decimal_from(open_score, Decimal("0")) - self._decimal_from(latest_score, Decimal("0"))
            if open_score != "" and latest_score != ""
            else Decimal("0")
        )
        recent_score_drawdown = (
            self._decimal_from(previous_score, Decimal("0")) - self._decimal_from(latest_score, Decimal("0"))
            if latest_score != "" and previous_score != ""
            else Decimal("0")
        )
        triggered = False
        tags: list[str] = []
        matched_rules: list[str] = []
        reasons: list[str] = []

        latest_open = latest_kline[0] if latest_kline else Decimal("0")
        latest_close = latest_kline[1] if latest_kline else Decimal("0")
        if len(recent_15m_open_closes) < 2:
            reasons.append("rule2_missing_recent_two_15m_klines")
        elif any(close >= open_ for open_, close in recent_15m_open_closes[:2]):
            reasons.append("rule2_recent_two_15m_not_all_bearish")
        elif unrealized_pnl >= two_r:
            reasons.append("rule2_unrealized_pnl_ge_2r")
        elif latest_score == "" or previous_score == "" or previous_previous_score == "":
            reasons.append("rule2_missing_recent_three_total_scores")
        else:
            rule2_score_drawdown = recent_score_drawdown
            if self._decimal_from(latest_score, Decimal("0")) >= self._decimal_from(previous_score, Decimal("0")):
                reasons.append("rule2_latest_score_not_below_previous_score")
            elif self._decimal_from(previous_score, Decimal("0")) >= self._decimal_from(previous_previous_score, Decimal("0")):
                reasons.append("rule2_previous_score_not_below_previous_previous_score")
            elif rule2_score_drawdown < Decimal("17"):
                reasons.append("rule2_recent_score_drawdown_lt_17")
            else:
                triggered = True
                tags.append(self.REDUCTION_TAG_MEDIUM_DRAWDOWN_WEAKENING)
                matched_rules.append("规则二")
                reasons.append("rule2_medium_drawdown_and_continuous_weakening")

        if highest <= 0:
            reasons.append("rule3_missing_recent_three_15m_highs")
        elif current_price <= 0:
            reasons.append("rule3_missing_current_price")
        elif drawdown < Decimal("0.035"):
            reasons.append("rule3_price_drawdown_lt_3_5_percent")
        elif unrealized_pnl >= two_r:
            reasons.append("rule3_unrealized_pnl_ge_2r")
        elif latest_score == "":
            reasons.append("rule3_missing_latest_total_score")
        elif previous_score == "" or previous_previous_score == "":
            reasons.append("rule3_missing_recent_three_total_scores")
        elif self._decimal_from(latest_score, Decimal("0")) >= self._decimal_from(previous_score, Decimal("0")):
            reasons.append("rule3_latest_score_not_below_previous_score")
        elif self._decimal_from(previous_score, Decimal("0")) >= self._decimal_from(previous_previous_score, Decimal("0")):
            reasons.append("rule3_previous_score_not_below_previous_previous_score")
        else:
            rule3_score_drawdown = recent_score_drawdown
            if rule3_score_drawdown >= Decimal("18"):
                triggered = True
                tags.append(self.REDUCTION_TAG_ABSOLUTE_SCORE_DRAWDOWN)
                matched_rules.append("规则三")
                reasons.append("rule3_absolute_score_large_drawdown")
            else:
                reasons.append("rule3_score_drawdown_lt_18")

        open_entry_price_decimal = self._decimal_from(open_entry_price, Decimal("0")) if open_entry_price != "" else Decimal("0")
        latest_ema21 = self._latest_15m_ema21(symbol, round_ts)
        rule5_bearish_count = sum(1 for open_, close in recent_15m_open_closes[:3] if close < open_)
        rule5_deep_weakness = (
            latest_ema21 > 0
            and current_price > 0
            and current_price < latest_ema21
            and open_entry_price_decimal > 0
            and current_price <= open_entry_price_decimal
            and len(recent_15m_open_closes) >= 3
            and rule5_bearish_count >= 2
        )
        if open_entry_price == "":
            reasons.append("rule5_missing_open_entry_price")
        elif open_trade_created_at == "":
            reasons.append("rule5_missing_open_trade_lifecycle")
        elif self._has_rule5_reduction_record_since(symbol, int(open_trade_created_at)):
            reasons.append("rule5_already_triggered_in_current_open_lifecycle")
        elif current_price <= 0:
            reasons.append("rule5_missing_current_price")
        elif latest_ema21 <= 0:
            reasons.append("rule5_missing_15m_ema21")
        elif current_price >= latest_ema21:
            reasons.append("rule5_current_price_not_below_15m_ema21")
        elif current_price > open_entry_price_decimal:
            reasons.append("rule5_current_price_gt_open_entry_price")
        elif len(recent_15m_open_closes) < 3:
            reasons.append("rule5_missing_recent_three_15m_klines")
        elif rule5_bearish_count < 2:
            reasons.append("rule5_recent_three_15m_bearish_count_lt_2")
        elif rule5_deep_weakness:
            triggered = True
            tags.append(self.REDUCTION_TAG_DEEP_WEAKNESS)
            matched_rules.append("规则五")
            reasons.append("rule5_deep_weakness")

        reason = "; ".join(reasons)
        matched_reason_map = {
            "规则二": "medium_drawdown_and_continuous_weakening",
            "规则三": "absolute_score_large_drawdown",
            "规则五": "deep_weakness",
        }
        if matched_rules:
            reason = "; ".join(matched_reason_map[rule] for rule in matched_rules)
        return PositionReductionCheck(symbol, round_ts, self._fmt_decimal(highest), self._fmt_decimal(current_price), self._fmt_decimal(drawdown), self._fmt_decimal(equity), self._fmt_decimal(two_r), self._fmt_decimal(one_r), self._fmt_decimal(unrealized_pnl), open_score, latest_score, self._fmt_decimal(score_drawdown), self._fmt_decimal(latest_open), self._fmt_decimal(latest_close), previous_score, self._fmt_decimal(recent_score_drawdown), self._fmt_decimal(self._decimal_from(open_entry_price, Decimal("0"))) if open_entry_price else "", "+".join(matched_rules), triggered, "、".join(dict.fromkeys(tags)), reason, now_ms)

    def _execute_reduction_actions(self, checks: list[PositionReductionCheck], positions: list[dict[str, Any]], now_ms: int) -> int:
        records = 0
        for check in checks:
            if not check.triggered or self._has_reduction_record(check.symbol, check.decision_round_ts):
                continue
            position = next((p for p in positions if self._base_symbol(str(p.get("symbol", ""))) == check.symbol), None)
            if not position:
                continue
            record = self._execute_reduction_action(position, check, now_ms)
            self._save_reduction_record(record)
            records += 1
        return records

    def _execute_reduction_action(self, position: dict[str, Any], check: PositionReductionCheck, now_ms: int) -> PositionReductionRecord:
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        exchange_symbol = self._exchange_symbol(position, check.symbol)
        side = "SELL" if amount > 0 else "BUY"
        original_quantity = abs(amount)
        matched_rule, percent = self._reduction_action_for_rules(check.rule_name)
        reduced_quantity = Decimal("0")
        remaining_quantity = Decimal("0")
        old_limit_order_id = self._latest_open_trade_stop_loss_order_id(check.symbol)
        new_limit_order_id = ""
        market_order_id = ""
        limit_price = self._latest_open_trade_stop_loss_price(check.symbol)
        status = "submitted"
        reason_parts = [check.reason, f"matched_rule={matched_rule}", f"reduction_percent={self._fmt_decimal(percent * Decimal('100'))}%"]
        raw_parts: list[str] = []
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(
            self.db_path, check.symbol, "holding_position_scoring", "reduction", now_ms
        )
        if lock_handle is None:
            return PositionReductionRecord(0, check.symbol, check.decision_round_ts, side, matched_rule, self._fmt_decimal(percent), self._fmt_decimal(original_quantity), self._fmt_decimal(reduced_quantity), self._fmt_decimal(remaining_quantity), old_limit_order_id, new_limit_order_id, market_order_id, limit_price, "failed", f"{'; '.join(reason_parts)}; {lock_reason}", "", now_ms)
        try:
            cancel_reason = self._cancel_existing_exit_orders(exchange_symbol)
            if cancel_reason:
                reason_parts.append(cancel_reason)
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            if percent >= Decimal("1"):
                reduced_quantity = self._floor_to_step(original_quantity, exchange_info["step_size"])
                remaining_quantity = Decimal("0")
            else:
                reduced_quantity = self._floor_to_step(original_quantity * percent, exchange_info["step_size"])
                remaining_quantity = self._floor_to_step(original_quantity - reduced_quantity, exchange_info["step_size"])
                if remaining_quantity <= 0:
                    raise RuntimeError("reduction_remaining_quantity_rounded_to_zero")
                if reduced_quantity <= 0:
                    raise RuntimeError("reduction_market_quantity_rounded_to_zero")
                if not limit_price or self._decimal_from(limit_price, Decimal("0")) <= 0:
                    raise RuntimeError("reduction_limit_price_missing")
                stop_trigger = self._decimal_from(limit_price, Decimal("0"))
                current_mark_price = self._decimal_from(check.current_price, Decimal("0"))
                if current_mark_price <= 0:
                    current_mark_price = self._current_symbol_price(exchange_symbol, position)
                skip_reason = self._replacement_stop_immediate_trigger_reason(side, stop_trigger, current_mark_price)
                if skip_reason:
                    reason_parts.append(skip_reason)
                else:
                    endpoint, params = TradingExperiment._exit_order_request({
                        "symbol": exchange_symbol,
                        "side": side,
                        "type": "STOP",
                        "quantity": self._fmt_decimal(remaining_quantity),
                        "price": limit_price,
                        "stopPrice": limit_price,
                        "timeInForce": "GTC",
                        "reduceOnly": "true",
                        "workingType": "MARK_PRICE",
                    })
                    try:
                        new_limit_response = self.account_manager._signed_post(endpoint, params)
                    except Exception as exc:
                        if not self._is_order_would_immediately_trigger(exc):
                            raise
                        reason_parts.append(f"reduction_replacement_stop_skipped_immediate_trigger: side={side}; stop_price={self._fmt_decimal(stop_trigger)}; current_mark_price={self._fmt_decimal(current_mark_price)}")
                    else:
                        raw_parts.append(str({"new_limit_order": new_limit_response}))
                        new_limit_order_id = TradingExperiment._exit_order_id(new_limit_response if isinstance(new_limit_response, dict) else None)
                        if not new_limit_order_id:
                            raise RuntimeError(f"reduction_new_limit_order_id_missing: response={new_limit_response}")
                        self._update_latest_open_trade_stop_loss(check.symbol, new_limit_order_id, stop_trigger)
            market_response = self.account_manager._signed_post("/fapi/v1/order", self._market_close_order_params(exchange_symbol, side, reduced_quantity))
            raw_parts.append(str({"market_order": market_response}))
            market_order_id = str(market_response.get("orderId", "")) if isinstance(market_response, dict) else ""
            no_fill_reason = self._no_fill_order_response_reason(market_response)
            if no_fill_reason:
                status = "failed"
                reason_parts.append(no_fill_reason.replace("stop_loss", "reduction"))
                market_order_id = ""
            elif remaining_quantity <= 0:
                post_close_cancel_reason = self._cancel_existing_exit_orders(exchange_symbol, context="post_close")
                if post_close_cancel_reason:
                    reason_parts.append(post_close_cancel_reason)
            elif remaining_quantity > 0:
                actual_quantity = remaining_quantity
                actual_entry_price = self._decimal_from(position.get("entryPrice"), Decimal("0"))
                try:
                    actual_quantity, actual_entry_price = self._current_position_quantity_and_entry(exchange_symbol, remaining_quantity, position)
                except Exception as exc:
                    reason_parts.append(f"position_refresh_failed_using_fallback: {type(exc).__name__}: {exc}")

                try:
                    take_profit_order_id, take_profit_price, take_profit_reason = self._replace_hard_take_profit_for_position(
                        helper,
                        exchange_symbol,
                        check.symbol,
                        side,
                        actual_entry_price,
                        actual_quantity,
                        exchange_info["tick_size"],
                    )
                    if take_profit_order_id:
                        self._update_latest_open_trade_take_profit(check.symbol, take_profit_order_id, take_profit_price)
                    reason_parts.append(take_profit_reason)
                except Exception as exc:
                    reason_parts.append(f"hard_take_profit_recreate_failed: {type(exc).__name__}: {exc}")
                    self._record_reduction_error(check, "hard_take_profit_recreate", exc)

                try:
                    stop_loss_order_id, stop_loss_price, stop_loss_reason = self._replace_stop_loss_for_position(
                        exchange_symbol,
                        check.symbol,
                        side,
                        actual_quantity,
                        exchange_info["tick_size"],
                        self._decimal_from(check.current_price, Decimal("0")),
                    )
                    if stop_loss_order_id:
                        self._update_latest_open_trade_stop_loss(check.symbol, stop_loss_order_id, stop_loss_price)
                        new_limit_order_id = stop_loss_order_id
                    reason_parts.append(stop_loss_reason)
                except Exception as exc:
                    status = "failed"
                    reason_parts.append(f"stop_loss_recreate_failed: {type(exc).__name__}: {exc}")
                    self._record_reduction_error(check, "stop_loss_recreate", exc)
        except Exception as exc:
            status = "failed"
            reason_parts.append(f"reduction_action_failed: {type(exc).__name__}: {exc}")
            if self._is_reduce_only_rejected(exc):
                reason_parts.append(f"reduce_only_diagnostics: {self._reduce_only_diagnostics(exchange_symbol)}")
        finally:
            lock_manager.release(lock_handle)
        return PositionReductionRecord(0, check.symbol, check.decision_round_ts, side, matched_rule, self._fmt_decimal(percent), self._fmt_decimal(original_quantity), self._fmt_decimal(reduced_quantity), self._fmt_decimal(remaining_quantity), old_limit_order_id, new_limit_order_id, market_order_id, limit_price, status, "; ".join(reason_parts), " | ".join(raw_parts), now_ms)


    def _record_reduction_error(self, check: PositionReductionCheck, operation: str, exc: Exception) -> None:
        helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
        helper.init_tables()
        candidate = OpenableSymbol(
            symbol=check.symbol,
            decision_round_ts=check.decision_round_ts,
            total_score=int(self._decimal_from(check.latest_total_score, Decimal("0"))) if check.latest_total_score else 0,
            score_band="",
            stop_loss_distance_ratio=None,
            distance_threshold=None,
            stop_loss_distance_tier="",
            opening_leverage="0",
            distance_qualified=False,
            qualified=False,
            reason="holding_position_reduction",
            evaluated_at=int(time.time() * 1000),
        )
        helper._record_error(candidate, f"holding_reduction:{operation}", exc)


    def _cancel_latest_hard_take_profit_order(self, exchange_symbol: str, symbol: str) -> str:
        order_id = self._latest_open_trade_take_profit_order_id(symbol)
        if not order_id:
            return "hard_take_profit_cancel_skipped_missing_order_id"
        try:
            response = self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": order_id})
        except AttributeError as exc:
            return f"hard_take_profit_cancel_unsupported: {type(exc).__name__}: {exc}"
        except Exception as exc:
            return f"hard_take_profit_cancel_failed: {type(exc).__name__}: {exc}"
        return f"hard_take_profit_cancelled; order_id={order_id}; response={response}"

    def _replace_hard_take_profit_for_position(
        self,
        helper: TradingExperiment,
        exchange_symbol: str,
        symbol: str,
        side: str,
        entry_price: Decimal,
        quantity: Decimal,
        tick_size: Decimal,
    ) -> tuple[str, Decimal, str]:
        take_profit_price = self._hard_take_profit_price_for_side(
            side=side,
            entry_price=entry_price,
            quantity=quantity,
            profit_usdt=helper.config.hard_take_profit_usdt,
            tick_size=tick_size,
        )
        if take_profit_price <= 0 or quantity <= 0:
            raise RuntimeError("hard_take_profit_recreate_invalid_price_or_quantity")
        endpoint, params = TradingExperiment._exit_order_request({
            "symbol": exchange_symbol,
            "side": side,
            "type": "TAKE_PROFIT",
            "quantity": self._fmt_decimal(quantity),
            "price": self._fmt_decimal(take_profit_price),
            "stopPrice": self._fmt_decimal(take_profit_price),
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "workingType": "MARK_PRICE",
        })
        response = helper._signed_post_order_with_ioc_retry(endpoint, params, trading_symbol=exchange_symbol)
        order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
        return order_id, take_profit_price, (
            f"hard_take_profit_recreated; target_profit_usdt={self._fmt_decimal(helper.config.hard_take_profit_usdt)}; "
            f"quantity={self._fmt_decimal(quantity)}; price={self._fmt_decimal(take_profit_price)}; order_id={order_id}"
        )

    def _replace_stop_loss_for_position(
        self,
        exchange_symbol: str,
        symbol: str,
        side: str,
        quantity: Decimal,
        tick_size: Decimal,
        current_mark_price: Decimal = Decimal("0"),
    ) -> tuple[str, Decimal, str]:
        old_order_id = self._latest_open_trade_stop_loss_order_id(symbol)
        stop_loss_price = self._decimal_from(self._latest_open_trade_stop_loss_price(symbol), Decimal("0"))
        stop_loss_price = TradingExperiment._floor_to_tick(stop_loss_price, tick_size) if stop_loss_price > 0 else Decimal("0")
        if quantity <= 0 or stop_loss_price <= 0:
            raise RuntimeError("stop_loss_recreate_invalid_price_or_quantity")
        skip_reason = self._replacement_stop_immediate_trigger_reason(side, stop_loss_price, current_mark_price)
        if skip_reason:
            raise RuntimeError(skip_reason)
        cancel_reason = self._cancel_latest_stop_loss_order(exchange_symbol, old_order_id)
        response = self.account_manager._signed_post(
            "/fapi/v1/order",
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
            },
        )
        order_id = TradingExperiment._exit_order_id(response if isinstance(response, dict) else None)
        if not order_id:
            raise RuntimeError(f"stop_loss_order_id_missing: response={response}")
        return order_id, stop_loss_price, (
            f"{cancel_reason}; stop_loss_recreated; quantity={self._fmt_decimal(quantity)}; "
            f"price={self._fmt_decimal(stop_loss_price)}; order_id={order_id}"
        )

    def _cancel_latest_stop_loss_order(self, exchange_symbol: str, order_id: str) -> str:
        if not order_id:
            return "stop_loss_cancel_skipped_missing_order_id"
        try:
            response = self.account_manager._signed_delete("/fapi/v1/algoOrder", {"symbol": exchange_symbol, "algoId": order_id})
        except AttributeError as exc:
            return f"stop_loss_cancel_unsupported: {type(exc).__name__}: {exc}"
        except Exception as exc:
            return f"stop_loss_cancel_failed: {type(exc).__name__}: {exc}"
        return f"stop_loss_cancelled; order_id={order_id}; response={response}"

    def _current_position_quantity_and_entry(
        self,
        exchange_symbol: str,
        fallback_quantity: Decimal,
        fallback_position: dict[str, Any],
    ) -> tuple[Decimal, Decimal]:
        try:
            rows = self.account_manager._signed_get("/fapi/v3/positionRisk", {"symbol": exchange_symbol})
        except Exception:
            rows = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict) or str(row.get("symbol", "")).upper() != exchange_symbol.upper():
                    continue
                amount = abs(self._decimal_from(row.get("positionAmt"), Decimal("0")))
                entry = self._decimal_from(row.get("entryPrice"), Decimal("0"))
                if amount > 0 and entry > 0:
                    return amount, entry
        fallback_entry = self._decimal_from(fallback_position.get("entryPrice"), Decimal("0"))
        return fallback_quantity, fallback_entry

    @staticmethod
    def _hard_take_profit_price_for_side(side: str, entry_price: Decimal, quantity: Decimal, profit_usdt: Decimal, tick_size: Decimal) -> Decimal:
        if entry_price <= 0 or quantity <= 0 or profit_usdt <= 0:
            return Decimal("0")
        distance = profit_usdt / quantity
        if side.upper() == "BUY":
            return TradingExperiment._floor_to_tick(entry_price - distance, tick_size)
        return TradingExperiment._ceil_to_tick(entry_price + distance, tick_size)

    def _latest_open_trade_take_profit_order_id(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT take_profit_order_id FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["take_profit_order_id"]) if row and row["take_profit_order_id"] is not None else ""

    def _update_latest_open_trade_take_profit(self, symbol: str, order_id: str, price: Decimal) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE trading_experiment_trades
                    SET take_profit_order_id = ?, take_profit_price = ?, updated_at = ?
                    WHERE id = (
                        SELECT id FROM trading_experiment_trades
                        WHERE symbol = ? AND status = 'opened'
                        ORDER BY created_at DESC, id DESC LIMIT 1
                    )
                    """,
                    (order_id, self._fmt_decimal(price), int(time.time() * 1000), symbol),
                )
        except sqlite3.Error:
            return

    def _update_latest_open_trade_stop_loss(self, symbol: str, order_id: str, price: Decimal) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE trading_experiment_trades
                    SET stop_loss_order_id = ?, stop_loss_price = ?, updated_at = ?
                    WHERE id = (
                        SELECT id FROM trading_experiment_trades
                        WHERE symbol = ? AND status = 'opened'
                        ORDER BY created_at DESC, id DESC LIMIT 1
                    )
                    """,
                    (order_id, self._fmt_decimal(price), int(time.time() * 1000), symbol),
                )
        except sqlite3.Error:
            return

    @staticmethod
    def _replacement_stop_immediate_trigger_reason(side: str, stop_price: Decimal, current_mark_price: Decimal) -> str:
        if stop_price <= 0 or current_mark_price <= 0:
            return ""
        normalized_side = side.upper()
        would_trigger = (normalized_side == "SELL" and stop_price >= current_mark_price) or (normalized_side == "BUY" and stop_price <= current_mark_price)
        if not would_trigger:
            return ""
        return (
            "reduction_replacement_stop_skipped_immediate_trigger: "
            f"side={normalized_side}; stop_price={HoldingPositionScoringSystem._fmt_decimal(stop_price)}; "
            f"current_mark_price={HoldingPositionScoringSystem._fmt_decimal(current_mark_price)}"
        )

    @staticmethod
    def _is_order_would_immediately_trigger(exc: Exception) -> bool:
        message = str(exc)
        return "-2021" in message or "Order would immediately trigger" in message

    @staticmethod
    def _reduction_action_for_rules(rule_name: str) -> tuple[str, Decimal]:
        mapping = {"规则五": Decimal("0.5"), "规则三": Decimal("0.3"), "规则二": Decimal("0.25")}
        for rule in ("规则五", "规则三", "规则二"):
            if rule in rule_name:
                return rule, mapping[rule]
        return "", Decimal("0")

    def _latest_open_trade_stop_loss_order_id(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT stop_loss_order_id FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["stop_loss_order_id"]) if row and row["stop_loss_order_id"] is not None else ""

    def _latest_open_trade_stop_loss_price(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT stop_loss_price FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1", (symbol,)).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["stop_loss_price"]) if row and row["stop_loss_price"] is not None else ""

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _highest_recent_15m_high(self, symbol: str, limit: int = 3) -> Decimal:
        try:
            with self._connect() as conn:
                rows = conn.execute("SELECT high FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT ?", (symbol, limit)).fetchall()
        except sqlite3.Error:
            return Decimal("0")
        if len(rows) < limit:
            return Decimal("0")
        return max((self._decimal_from(row["high"], Decimal("0")) for row in rows), default=Decimal("0"))

    def _current_symbol_price(self, exchange_symbol: str, position: dict[str, Any]) -> Decimal:
        for key in ("markPrice", "mark_price"):
            price = self._decimal_from(position.get(key), Decimal("0"))
            if price > 0:
                return price
        try:
            response = self.account_manager._public_get("/fapi/v1/ticker/price", {"symbol": exchange_symbol})
            if isinstance(response, dict):
                return self._decimal_from(response.get("price"), Decimal("0"))
        except Exception:
            return Decimal("0")
        return Decimal("0")

    def _latest_open_trade_total_score(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT total_score FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["total_score"]) if row and row["total_score"] is not None else ""


    def _latest_open_trade_entry_price(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(trading_experiment_trades)").fetchall()}
                if "entry_price" not in columns:
                    return ""
                row = conn.execute(
                    "SELECT entry_price FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["entry_price"]) if row and row["entry_price"] is not None else ""

    def _latest_open_trade_created_at(self, symbol: str) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT created_at FROM trading_experiment_trades WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["created_at"]) if row and row["created_at"] is not None else ""

    def _has_rule5_reduction_record_since(self, symbol: str, lifecycle_started_at: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.REDUCTION_RECORDS_TABLE} WHERE symbol = ? AND matched_rule LIKE ? AND created_at >= ? LIMIT 1",
                (symbol, "%规则五%", lifecycle_started_at),
            ).fetchone()
        return row is not None

    def evaluate_increase_conditions(
        self,
        positions: list[dict[str, Any]] | None = None,
        decision_round_ts: int | None = None,
        checked_at: int | None = None,
    ) -> list[PositionIncreaseCheck]:
        """Evaluate the first-add-position module after reduction checks finish."""
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        if not self._has_total_scores_for_round(round_ts):
            return []
        now_ms = checked_at if checked_at is not None else int(time.time() * 1000)
        active_positions = positions if positions is not None else self._active_positions()
        equity = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_experiment_usdt_equity()
        one_r = equity * Decimal("0.01")
        checks: list[PositionIncreaseCheck] = []
        for position in active_positions:
            symbol = self._base_symbol(str(position.get("symbol", "")))
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            if not symbol or amount == 0:
                continue
            check = self._evaluate_increase_rules(position, symbol, round_ts, equity, one_r, now_ms)
            self._save_increase_check(check)
            checks.append(check)
        return checks

    def _evaluate_increase_rules(self, position: dict[str, Any], symbol: str, round_ts: int, equity: Decimal, one_r: Decimal, now_ms: int) -> PositionIncreaseCheck:
        exchange_symbol = self._exchange_symbol(position, symbol)
        current_price = self._current_symbol_price(exchange_symbol, position)
        unrealized_pnl = self._position_unrealized_pnl(position)
        recent_scores = self._recent_total_scores(symbol, limit=2)
        latest_score = recent_scores[0] if recent_scores else ""
        previous_score = recent_scores[1] if len(recent_scores) > 1 else ""
        open_trade_created_at = self._latest_open_trade_created_at(symbol)
        lifecycle_started_at = int(open_trade_created_at) if open_trade_created_at else 0
        latest_reduction_price = self._latest_reduction_price_since(symbol, lifecycle_started_at) if lifecycle_started_at else ""
        reasons: list[str] = []
        triggered = False
        tag = ""
        if open_trade_created_at == "":
            reasons.append("missing_open_trade_lifecycle")
        elif self._has_first_increase_record_since(symbol, lifecycle_started_at):
            reasons.append("first_increase_already_triggered_in_current_lifecycle")
            tag = self.INCREASE_TAG_FIRST_COMPLETED
        elif one_r <= 0:
            reasons.append("non_positive_one_r")
        else:
            condition1_met = unrealized_pnl >= one_r * Decimal("1.3")
            condition2_met = latest_score != "" and previous_score != "" and self._decimal_from(latest_score, Decimal("0")) >= self._decimal_from(previous_score, Decimal("0")) - Decimal("5")
            has_reduction_record = latest_reduction_price != ""
            condition3_met = not has_reduction_record or current_price >= self._decimal_from(latest_reduction_price, Decimal("0"))

            if not condition1_met:
                reasons.append("condition1_unrealized_pnl_lt_1_3r")
            if latest_score == "" or previous_score == "":
                reasons.append("condition2_missing_latest_or_previous_total_score")
            elif not condition2_met:
                reasons.append("condition2_latest_score_lt_previous_minus_5")
            if not condition3_met:
                reasons.append("condition3_current_price_lt_latest_reduction_price")

            if condition1_met and condition2_met and condition3_met:
                triggered = True
                tag = self.INCREASE_TAG_FIRST
                reasons = ["first_increase_conditions_met"]
            elif condition2_met and not condition1_met and (not has_reduction_record or condition3_met):
                tag = self.INCREASE_TAG_PRE_TRIGGER
        return PositionIncreaseCheck(symbol, round_ts, self._fmt_decimal(current_price), self._fmt_decimal(equity), self._fmt_decimal(one_r), self._fmt_decimal(unrealized_pnl), latest_score, previous_score, latest_reduction_price, open_trade_created_at, triggered, tag, "; ".join(reasons), now_ms)

    def refresh_pretrigger_increase_checks(self, now_ms: int | None = None) -> dict[str, Any]:
        """Refresh latest mark prices for pre-triggered first-add checks.

        A pre-triggered symbol has already passed condition 2 while condition
        1 is not yet satisfied; when a reduction record exists, condition 3
        is already satisfied.  This method re-reads active
        positions, forces a fresh
        mark-price lookup for those symbols, re-evaluates the same decision
        round, and executes the first-add action when conditions 1 and 3 both become true.
        """
        self.init_tables()
        checked_at = now_ms if now_ms is not None else int(time.time() * 1000)
        round_ts, latest_checks = self.get_latest_increase_checks()
        pretrigger_symbols = {str(row["symbol"]) for row in latest_checks if str(row["tag"] or "") == self.INCREASE_TAG_PRE_TRIGGER}
        if round_ts is None or not pretrigger_symbols:
            return {"round_ts": round_ts, "refreshed": 0, "triggered": 0, "records": 0}

        positions = [
            position
            for position in self._active_positions()
            if self._base_symbol(str(position.get("symbol", ""))) in pretrigger_symbols
        ]
        equity = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_experiment_usdt_equity()
        one_r = equity * Decimal("0.01")
        refreshed_checks: list[PositionIncreaseCheck] = []
        for position in positions:
            symbol = self._base_symbol(str(position.get("symbol", "")))
            exchange_symbol = self._exchange_symbol(position, symbol)
            latest_mark_price = self._latest_mark_price(exchange_symbol)
            refreshed_position = dict(position)
            if latest_mark_price > 0:
                refreshed_position["markPrice"] = self._fmt_decimal(latest_mark_price)
            check = self._evaluate_increase_rules(refreshed_position, symbol, int(round_ts), equity, one_r, checked_at)
            if check.tag in {self.INCREASE_TAG_PRE_TRIGGER, self.INCREASE_TAG_FIRST} or check.symbol in pretrigger_symbols:
                self._save_increase_check(check)
                refreshed_checks.append(check)
        records = self._record_increase_actions(refreshed_checks, positions, checked_at)
        return {
            "round_ts": round_ts,
            "refreshed": len(refreshed_checks),
            "triggered": sum(1 for row in refreshed_checks if row.triggered),
            "records": records,
        }

    def _latest_mark_price(self, exchange_symbol: str) -> Decimal:
        try:
            response = self.account_manager._public_get("/fapi/v1/premiumIndex", {"symbol": exchange_symbol})
            if isinstance(response, dict):
                return self._decimal_from(response.get("markPrice"), Decimal("0"))
        except Exception:
            return Decimal("0")
        return Decimal("0")

    def _latest_reduction_price_since(self, symbol: str, lifecycle_started_at: int) -> str:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    f"""
                    SELECT COALESCE(NULLIF(c.current_price, ''), NULLIF(r.limit_price, '')) AS price
                    FROM {self.REDUCTION_RECORDS_TABLE} AS r
                    LEFT JOIN {self.REDUCTION_CHECKS_TABLE} AS c
                      ON c.symbol = r.symbol AND c.decision_round_ts = r.decision_round_ts
                    WHERE r.symbol = ? AND r.created_at >= ?
                    ORDER BY r.created_at DESC, r.id DESC
                    LIMIT 1
                    """,
                    (symbol, lifecycle_started_at),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["price"]) if row and row["price"] is not None else ""

    def _has_first_increase_record_since(self, symbol: str, lifecycle_started_at: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.INCREASE_RECORDS_TABLE} WHERE symbol = ? AND action_name = ? AND created_at >= ? LIMIT 1",
                (symbol, self.INCREASE_TAG_FIRST, lifecycle_started_at),
            ).fetchone()
        return row is not None

    def _record_increase_actions(self, checks: list[PositionIncreaseCheck], positions: list[dict[str, Any]] | None = None, now_ms: int | None = None) -> int:
        active_positions = positions if positions is not None else self._active_positions()
        created_at = now_ms if now_ms is not None else int(time.time() * 1000)
        records = 0
        for check in checks:
            if not check.triggered or self._has_increase_record(check.symbol, check.decision_round_ts):
                continue
            position = next((p for p in active_positions if self._base_symbol(str(p.get("symbol", ""))) == check.symbol), None)
            record = self._execute_increase_action(position, check, created_at) if position else PositionIncreaseRecord(0, check.symbol, check.decision_round_ts, self.INCREASE_TAG_FIRST, check.current_price, check.unrealized_pnl, check.one_r_usdt, check.latest_total_score, check.previous_total_score, check.latest_reduction_price, "failed", f"{check.reason}; increase_position_missing", created_at)
            self._save_increase_record(record)
            records += 1
        return records

    def _execute_increase_action(self, position: dict[str, Any], check: PositionIncreaseCheck, now_ms: int) -> PositionIncreaseRecord:
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        exchange_symbol = self._exchange_symbol(position, check.symbol)
        side = "BUY" if amount > 0 else "SELL"
        original_quantity = abs(amount)
        increased_quantity = Decimal("0")
        required_margin = Decimal("0")
        available_experiment_usdt = Decimal("0")
        market_order_id = ""
        status = "submitted"
        reason_parts = [check.reason]
        raw_response = ""
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(
            self.db_path, check.symbol, "holding_position_scoring", "increase", now_ms
        )
        if lock_handle is None:
            return PositionIncreaseRecord(0, check.symbol, check.decision_round_ts, self.INCREASE_TAG_FIRST, check.current_price, check.unrealized_pnl, check.one_r_usdt, check.latest_total_score, check.previous_total_score, check.latest_reduction_price, "failed", f"{'; '.join(reason_parts)}; {lock_reason}", now_ms, self._fmt_decimal(original_quantity), self._fmt_decimal(increased_quantity), self._fmt_decimal(required_margin), self._fmt_decimal(available_experiment_usdt), market_order_id, raw_response)
        try:
            helper = TradingExperiment(self.db_path, account_manager=self.account_manager)
            exchange_info = helper._exchange_symbol_info(exchange_symbol)
            increased_quantity = self._floor_to_step(original_quantity * Decimal("0.5"), exchange_info["step_size"])
            if increased_quantity <= 0:
                raise RuntimeError("increase_market_quantity_rounded_to_zero")
            current_price = self._decimal_from(check.current_price, Decimal("0"))
            if current_price <= 0:
                current_price = self._current_symbol_price(exchange_symbol, position)
            leverage = self._position_leverage(position)
            if current_price <= 0:
                raise RuntimeError("increase_current_price_missing")
            if leverage <= 0:
                raise RuntimeError("increase_position_leverage_missing")
            required_margin = increased_quantity * current_price / leverage
            account_equity = helper._fetch_experiment_usdt_equity()
            reserved_margin_budget = helper._reserved_margin_from_positions(helper._fetch_and_store_positions())
            account = self.account_manager._signed_get("/fapi/v3/account")
            if not isinstance(account, dict):
                raise RuntimeError("Unexpected Binance account response format")
            available_balance = self._decimal_from(account.get("availableBalance"), Decimal("0"))
            available_experiment_usdt = available_balance - helper.config.experiment_uninvested_usdt
            if available_experiment_usdt < 0:
                available_experiment_usdt = Decimal("0")
            if reserved_margin_budget + required_margin > account_equity:
                status = "skipped"
                reason_parts.append("实验组净值预算不足")
            elif required_margin > available_experiment_usdt:
                status = "skipped"
                reason_parts.append("可用金额不足")
            else:
                cancel_take_profit_reason = self._cancel_latest_hard_take_profit_order(exchange_symbol, check.symbol)
                if cancel_take_profit_reason:
                    reason_parts.append(cancel_take_profit_reason)
                response = self.account_manager._signed_post(
                    "/fapi/v1/order",
                    {
                        "symbol": exchange_symbol,
                        "side": side,
                        "type": "MARKET",
                        "quantity": self._fmt_decimal(increased_quantity),
                        "newOrderRespType": "RESULT",
                    },
                )
                raw_response = str(response)
                market_order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
                no_fill_reason = self._no_fill_order_response_reason(response)
                if no_fill_reason:
                    status = "failed"
                    reason_parts.append(no_fill_reason.replace("stop_loss", "increase"))
                    market_order_id = ""
                else:
                    try:
                        actual_quantity, actual_entry_price = self._current_position_quantity_and_entry(
                            exchange_symbol,
                            self._floor_to_step(original_quantity + increased_quantity, exchange_info["step_size"]),
                            position,
                        )
                        close_side = "SELL" if amount > 0 else "BUY"
                        take_profit_order_id, take_profit_price, take_profit_reason = self._replace_hard_take_profit_for_position(
                            helper,
                            exchange_symbol,
                            check.symbol,
                            close_side,
                            actual_entry_price,
                            actual_quantity,
                            exchange_info["tick_size"],
                        )
                        if take_profit_order_id:
                            self._update_latest_open_trade_take_profit(check.symbol, take_profit_order_id, take_profit_price)
                        reason_parts.append(take_profit_reason)
                        try:
                            stop_loss_order_id, stop_loss_price, stop_loss_reason = self._replace_stop_loss_for_position(
                                exchange_symbol,
                                check.symbol,
                                close_side,
                                actual_quantity,
                                exchange_info["tick_size"],
                            )
                            if stop_loss_order_id:
                                self._update_latest_open_trade_stop_loss(check.symbol, stop_loss_order_id, stop_loss_price)
                            reason_parts.append(stop_loss_reason)
                        except Exception as exc:
                            reason_parts.append(f"stop_loss_recreate_failed: {type(exc).__name__}: {exc}")
                    except Exception as exc:
                        reason_parts.append(f"hard_take_profit_recreate_failed: {type(exc).__name__}: {exc}")
        except Exception as exc:
            status = "failed"
            reason_parts.append(f"increase_action_failed: {type(exc).__name__}: {exc}")
        finally:
            lock_manager.release(lock_handle)
        return PositionIncreaseRecord(0, check.symbol, check.decision_round_ts, self.INCREASE_TAG_FIRST, check.current_price, check.unrealized_pnl, check.one_r_usdt, check.latest_total_score, check.previous_total_score, check.latest_reduction_price, status, "; ".join(reason_parts), now_ms, self._fmt_decimal(original_quantity), self._fmt_decimal(increased_quantity), self._fmt_decimal(required_margin), self._fmt_decimal(available_experiment_usdt), market_order_id, raw_response)

    def _save_increase_check(self, check: PositionIncreaseCheck) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.INCREASE_CHECKS_TABLE}
                (symbol, decision_round_ts, current_price, account_equity_usdt, one_r_usdt, unrealized_pnl, latest_total_score, previous_total_score, latest_reduction_price, open_trade_created_at, triggered, tag, reason, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    current_price=excluded.current_price, account_equity_usdt=excluded.account_equity_usdt,
                    one_r_usdt=excluded.one_r_usdt, unrealized_pnl=excluded.unrealized_pnl,
                    latest_total_score=excluded.latest_total_score, previous_total_score=excluded.previous_total_score,
                    latest_reduction_price=excluded.latest_reduction_price, open_trade_created_at=excluded.open_trade_created_at,
                    triggered=excluded.triggered, tag=excluded.tag, reason=excluded.reason, checked_at=excluded.checked_at
                """,
                (check.symbol, check.decision_round_ts, check.current_price, check.account_equity_usdt, check.one_r_usdt, check.unrealized_pnl, check.latest_total_score, check.previous_total_score, check.latest_reduction_price, check.open_trade_created_at, int(check.triggered), check.tag, check.reason, check.checked_at),
            )

    def _save_increase_record(self, record: PositionIncreaseRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.INCREASE_RECORDS_TABLE}
                (symbol, decision_round_ts, action_name, current_price, unrealized_pnl, one_r_usdt, latest_total_score, previous_total_score, latest_reduction_price, status, reason, created_at, original_quantity, increased_quantity, required_margin_usdt, available_experiment_usdt, market_order_id, raw_response)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.symbol, record.decision_round_ts, record.action_name, record.current_price, record.unrealized_pnl, record.one_r_usdt, record.latest_total_score, record.previous_total_score, record.latest_reduction_price, record.status, record.reason, record.created_at, record.original_quantity, record.increased_quantity, record.required_margin_usdt, record.available_experiment_usdt, record.market_order_id, record.raw_response),
            )

    def _has_increase_record(self, symbol: str, decision_round_ts: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(f"SELECT 1 FROM {self.INCREASE_RECORDS_TABLE} WHERE symbol = ? AND decision_round_ts = ? LIMIT 1", (symbol, decision_round_ts)).fetchone()
        return row is not None

    def get_latest_increase_checks(self) -> tuple[int | None, list[sqlite3.Row]]:
        self.init_tables()
        with self._connect() as conn:
            row = conn.execute(f"SELECT MAX(decision_round_ts) AS ts FROM {self.INCREASE_CHECKS_TABLE}").fetchone()
            round_ts = int(row["ts"]) if row and row["ts"] is not None else None
            if round_ts is None:
                return None, []
            rows = conn.execute(f"SELECT * FROM {self.INCREASE_CHECKS_TABLE} WHERE decision_round_ts = ? ORDER BY triggered DESC, symbol ASC", (round_ts,)).fetchall()
        return round_ts, rows


    def latest_pretrigger_increase_rounds(self) -> dict[str, int]:
        """Return each symbol's most recent decision round with a pre-trigger tag."""
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, MAX(decision_round_ts) AS decision_round_ts
                FROM {self.INCREASE_CHECKS_TABLE}
                WHERE tag = ?
                GROUP BY symbol
                """,
                (self.INCREASE_TAG_PRE_TRIGGER,),
            ).fetchall()
        return {str(row["symbol"]): int(row["decision_round_ts"]) for row in rows if row["decision_round_ts"] is not None}

    def recent_increase_records(self, limit: int = 100, since_ms: int | None = None) -> list[sqlite3.Row]:
        self.init_tables()
        where = "WHERE created_at >= ?" if since_ms is not None else ""
        params: tuple[Any, ...] = (int(since_ms), limit) if since_ms is not None else (limit,)
        with self._connect() as conn:
            return conn.execute(f"SELECT * FROM {self.INCREASE_RECORDS_TABLE} {where} ORDER BY created_at DESC, id DESC LIMIT ?", params).fetchall()

    def calculate_portfolio_risk(
        self,
        positions: list[dict[str, Any]] | None = None,
        decision_round_ts: int | None = None,
        calculated_at: int | None = None,
    ) -> PortfolioRiskSummary:
        """Calculate and persist total portfolio risk for active holdings."""
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        now_ms = calculated_at if calculated_at is not None else int(time.time() * 1000)
        active_positions = positions if positions is not None else self._active_positions()
        equity = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_experiment_usdt_equity()
        total_risk = Decimal("0")
        rows: list[PortfolioRiskPosition] = []
        for position in active_positions:
            symbol = self._base_symbol(str(position.get("symbol", "")))
            amount = abs(self._decimal_from(position.get("positionAmt"), Decimal("0")))
            latest_close = self._latest_15m_close(symbol) if symbol else Decimal("0")
            leverage = self._position_leverage(position)
            risk = Decimal("0")
            reason = "ok"
            if not symbol or amount == 0:
                reason = "empty_symbol_or_zero_position"
            elif equity <= 0:
                reason = "non_positive_experiment_usdt_equity"
            elif latest_close <= 0:
                reason = "missing_or_invalid_latest_15m_close"
            elif leverage <= 0:
                reason = "missing_or_invalid_leverage"
            else:
                risk = (amount * latest_close / equity) * leverage
                total_risk += risk
            total_score = self._latest_total_score(symbol) if symbol else ""
            rows.append(
                PortfolioRiskPosition(
                    symbol=symbol,
                    decision_round_ts=round_ts,
                    position_amt=self._fmt_decimal(amount),
                    latest_15m_close=self._fmt_decimal(latest_close),
                    account_equity_usdt=self._fmt_decimal(equity),
                    leverage=self._fmt_decimal(leverage),
                    risk=self._fmt_decimal(risk),
                    total_score=total_score,
                    reason=reason,
                    calculated_at=now_ms,
                )
            )
        summary = PortfolioRiskSummary(round_ts, self._fmt_decimal(total_risk), len(rows), self._fmt_decimal(equity), now_ms, rows)
        self._save_portfolio_risk_summary(summary)
        self._save_portfolio_risk_rows(round_ts, rows)
        return summary


    def _latest_total_score(self, symbol: str) -> str:
        scores = self._recent_total_scores(symbol, limit=1)
        return scores[0] if scores else ""

    def _recent_total_scores(self, symbol: str, limit: int = 2) -> list[str]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT total_score FROM symbol_total_scores WHERE symbol = ? ORDER BY decision_round_ts DESC LIMIT ?",
                    (symbol, limit),
                ).fetchall()
        except sqlite3.Error:
            return []
        return [str(row["total_score"]) for row in rows if row["total_score"] is not None]

    def _position_unrealized_pnl(self, position: dict[str, Any]) -> Decimal:
        return self._decimal_from(position.get("unRealizedProfit", position.get("unrealizedProfit")), Decimal("0"))

    @classmethod
    def _market_close_order_params(cls, exchange_symbol: str, side: str, quantity: Decimal) -> dict[str, str]:
        return {
            "symbol": exchange_symbol,
            "side": side,
            "type": "MARKET",
            "quantity": cls._fmt_decimal(quantity),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        }

    def _latest_15m_close(self, symbol: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        return self._decimal_from(row["close"], Decimal("0")) if row else Decimal("0")

    def _latest_15m_open_close(self, symbol: str) -> tuple[Decimal, Decimal] | None:
        try:
            with self._connect() as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(klines_15m)").fetchall()}
                if "open" not in columns or "close" not in columns:
                    return None
                row = conn.execute(
                    "SELECT open, close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        return self._decimal_from(row["open"], Decimal("0")), self._decimal_from(row["close"], Decimal("0"))

    def _recent_15m_open_closes(self, symbol: str, limit: int = 2) -> list[tuple[Decimal, Decimal]]:
        try:
            with self._connect() as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(klines_15m)").fetchall()}
                if "open" not in columns or "close" not in columns:
                    return []
                rows = conn.execute(
                    "SELECT open, close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT ?",
                    (symbol, limit),
                ).fetchall()
        except sqlite3.Error:
            return []
        return [(self._decimal_from(row["open"], Decimal("0")), self._decimal_from(row["close"], Decimal("0"))) for row in rows]

    def _position_leverage(self, position: dict[str, Any]) -> Decimal:
        raw = str(position.get("leverage", "")).strip().lower().replace("x", "")
        if raw:
            return self._decimal_from(raw, Decimal("0"))
        fallback = TradingExperiment(self.db_path, account_manager=self.account_manager)._latest_opened_trade_leverages()
        return self._decimal_from(fallback.get(self._base_symbol(position.get("symbol", "")), "0"), Decimal("0"))

    def _save_portfolio_risk_summary(self, summary: PortfolioRiskSummary) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.PORTFOLIO_RISK_SUMMARY_TABLE}
                (decision_round_ts, total_risk, position_count, account_equity_usdt, calculated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(decision_round_ts) DO UPDATE SET
                    total_risk=excluded.total_risk,
                    position_count=excluded.position_count,
                    account_equity_usdt=excluded.account_equity_usdt,
                    calculated_at=excluded.calculated_at
                """,
                (summary.decision_round_ts, summary.total_risk, summary.position_count, summary.account_equity_usdt, summary.calculated_at),
            )

    def _save_portfolio_risk_rows(self, decision_round_ts: int, rows: list[PortfolioRiskPosition]) -> None:
        with self._connect() as conn:
            conn.execute(f"DELETE FROM {self.PORTFOLIO_RISK_TABLE} WHERE decision_round_ts = ?", (decision_round_ts,))
            conn.executemany(
                f"""
                INSERT INTO {self.PORTFOLIO_RISK_TABLE}
                (symbol, decision_round_ts, position_amt, latest_15m_close, account_equity_usdt, leverage, risk, total_score, reason, calculated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(row.symbol, row.decision_round_ts, row.position_amt, row.latest_15m_close, row.account_equity_usdt, row.leverage, row.risk, row.total_score, row.reason, row.calculated_at) for row in rows],
            )

    def get_latest_portfolio_risk(self) -> PortfolioRiskSummary | None:
        self.init_tables()
        with self._connect() as conn:
            summary_row = conn.execute(
                f"SELECT * FROM {self.PORTFOLIO_RISK_SUMMARY_TABLE} ORDER BY decision_round_ts DESC LIMIT 1"
            ).fetchone()
            if summary_row is None:
                return None
            round_ts = int(summary_row["decision_round_ts"])
            db_rows = conn.execute(
                f"SELECT * FROM {self.PORTFOLIO_RISK_TABLE} WHERE decision_round_ts = ? ORDER BY CAST(risk AS REAL) DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        positions = [PortfolioRiskPosition(**dict(row)) for row in db_rows]
        return PortfolioRiskSummary(
            round_ts,
            str(summary_row["total_risk"]),
            int(summary_row["position_count"]),
            str(summary_row["account_equity_usdt"]),
            int(summary_row["calculated_at"]),
            positions,
        )

    def evaluate_symbol(self, symbol: str, decision_round_ts: int, checked_at: int | None = None) -> HoldingStopLossCheck:
        checked_at = checked_at if checked_at is not None else int(time.time() * 1000)
        rows = self._latest_two_15m_klines(symbol)
        stops = self._latest_two_structural_stop_losses(symbol)
        if len(rows) < 2:
            return HoldingStopLossCheck(symbol, decision_round_ts, False, None, None, None, None, None, None, "missing_two_15m_klines", checked_at)
        if len(stops) < 2:
            return HoldingStopLossCheck(symbol, decision_round_ts, False, int(rows[0]["open_time"]), float(rows[0]["close"]), None, int(rows[1]["open_time"]), float(rows[1]["close"]), None, "missing_two_structural_stop_losses", checked_at)
        latest_close = float(rows[0]["close"])
        prev_close = float(rows[1]["close"])
        latest_stop = float(stops[0]["structural_stop_loss"])
        prev_stop = float(stops[1]["structural_stop_loss"])
        triggered = latest_stop > 0 and prev_stop > 0 and latest_close < latest_stop and prev_close < prev_stop
        reason = "two_15m_closes_below_structural_stop_loss" if triggered else "stop_loss_rule_not_met"
        return HoldingStopLossCheck(symbol, decision_round_ts, triggered, int(rows[0]["open_time"]), latest_close, latest_stop, int(rows[1]["open_time"]), prev_close, prev_stop, reason, checked_at)

    def _latest_two_15m_klines(self, symbol: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT open_time, close FROM klines_15m WHERE symbol = ? ORDER BY open_time DESC LIMIT 2",
                (symbol,),
            ).fetchall()

    def _latest_two_structural_stop_losses(self, symbol: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT decision_round_ts, structural_stop_loss
                FROM symbol_structural_stop_losses
                WHERE symbol = ?
                ORDER BY decision_round_ts DESC
                LIMIT 2
                """,
                (symbol,),
            ).fetchall()

    def _active_positions(self) -> list[dict[str, Any]]:
        rows = self.account_manager._signed_get("/fapi/v3/positionRisk")
        positions = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        return [row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0]

    def _execute_stop_loss(self, position: dict[str, Any], check: HoldingStopLossCheck, now_ms: int) -> HoldingStopLossRecord:
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        exchange_symbol = self._exchange_symbol(position, check.symbol)
        side = "SELL" if amount > 0 else "BUY"
        quantity = abs(amount)
        status = "submitted"
        order_id = ""
        reason = check.reason
        raw_response = ""
        cancel_reason = ""
        lock_manager, lock_handle, lock_reason = acquire_trade_action_lock(
            self.db_path, check.symbol, "holding_position_scoring", "stop_loss", now_ms
        )
        if lock_handle is None:
            return HoldingStopLossRecord(0, check.symbol, check.decision_round_ts, side, self._fmt_decimal(quantity), check.latest_15m_close or 0.0, check.latest_structural_stop_loss or 0.0, check.prev_15m_close or 0.0, check.prev_structural_stop_loss or 0.0, "failed", "", "", f"{reason}; {lock_reason}", "", now_ms)
        try:
            cancel_reason = self._cancel_existing_exit_orders(exchange_symbol)
            if cancel_reason:
                reason = f"{reason}; {cancel_reason}"
            response = self.account_manager._signed_post("/fapi/v1/order", self._market_close_order_params(exchange_symbol, side, quantity))
            order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            raw_response = str(response)
            no_fill_reason = self._no_fill_order_response_reason(response)
            if no_fill_reason:
                status = "failed"
                reason = f"{reason}; {no_fill_reason}"
                order_id = ""
            else:
                post_close_cancel_reason = self._cancel_existing_exit_orders(exchange_symbol, context="post_close")
                if post_close_cancel_reason:
                    reason = f"{reason}; {post_close_cancel_reason}"
        except Exception as exc:
            status = "failed"
            reason = f"{check.reason}; stop_loss_order_failed: {type(exc).__name__}: {exc}"
            if cancel_reason:
                reason = f"{reason}; {cancel_reason}"
            if self._is_reduce_only_rejected(exc):
                reason = f"{reason}; reduce_only_diagnostics: {self._reduce_only_diagnostics(exchange_symbol)}"
        realized_pnl = ""
        pnl_failure_reason = ""
        if order_id:
            realized_pnl, pnl_failure_reason = self._fetch_order_realized_pnl(
                exchange_symbol,
                order_id,
            )
        if pnl_failure_reason:
            reason = f"{reason}; {pnl_failure_reason}"
        lock_manager.release(lock_handle)
        return HoldingStopLossRecord(0, check.symbol, check.decision_round_ts, side, self._fmt_decimal(quantity), float(check.latest_15m_close or 0), float(check.latest_structural_stop_loss or 0), float(check.prev_15m_close or 0), float(check.prev_structural_stop_loss or 0), status, order_id, realized_pnl, reason, raw_response, now_ms)

    def _cancel_existing_exit_orders(self, exchange_symbol: str, context: str = "pre_close") -> str:
        """Cancel normal and conditional open orders around a reduce-only MARKET close."""
        failures: list[str] = []
        for endpoint, label in (
            ("/fapi/v1/allOpenOrders", "open_orders_cancel"),
            ("/fapi/v1/algoOpenOrders", "algo_orders_cancel"),
        ):
            try:
                self.account_manager._signed_delete(endpoint, {"symbol": exchange_symbol})
            except AttributeError as exc:
                failures.append(f"{label}_unsupported: {type(exc).__name__}: {exc}")
            except Exception as exc:
                failures.append(f"{label}_failed: {type(exc).__name__}: {exc}")
        if failures:
            return f"{context}_cancel_warnings: " + " | ".join(failures)
        return f"{context}_cancelled_existing_open_orders"

    def _reduce_only_diagnostics(self, exchange_symbol: str) -> str:
        diagnostics: list[str] = []
        for endpoint, label, params in (
            ("/fapi/v3/positionRisk", "positions", None),
            ("/fapi/v1/openOrders", "open_orders", {"symbol": exchange_symbol}),
            ("/fapi/v1/openAlgoOrders", "open_algo_orders", {"symbol": exchange_symbol}),
        ):
            try:
                response = self.account_manager._signed_get(endpoint, params)
                diagnostics.append(f"{label}={self._summarize_response(response)}")
            except Exception as exc:
                diagnostics.append(f"{label}_query_failed={type(exc).__name__}: {exc}")
        return "; ".join(diagnostics)

    @staticmethod
    def _is_reduce_only_rejected(exc: Exception) -> bool:
        message = str(exc)
        return "-2022" in message or "ReduceOnly Order is rejected" in message

    @classmethod
    def _no_fill_order_response_reason(cls, response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        order_status = str(response.get("status", "")).upper()
        if order_status not in {"EXPIRED", "CANCELED", "REJECTED"}:
            return ""
        executed_qty = cls._decimal_from(response.get("executedQty") or response.get("cumQty"), Decimal("0"))
        if executed_qty != 0:
            return ""
        return f"stop_loss_order_not_filled: status={order_status}; executedQty={cls._fmt_decimal(executed_qty)}"

    @classmethod
    def _summarize_response(cls, response: Any) -> str:
        if isinstance(response, list):
            return "[" + ", ".join(cls._summarize_order_like_row(row) for row in response[:10]) + f"] total={len(response)}"
        if isinstance(response, dict):
            return cls._summarize_order_like_row(response)
        return str(response)

    @staticmethod
    def _summarize_order_like_row(row: Any) -> str:
        if not isinstance(row, dict):
            return str(row)
        keys = (
            "symbol",
            "positionAmt",
            "positionSide",
            "side",
            "type",
            "origQty",
            "executedQty",
            "reduceOnly",
            "closePosition",
            "status",
            "orderId",
            "algoId",
        )
        parts = [f"{key}={row[key]}" for key in keys if key in row]
        return "{" + ", ".join(parts) + "}"

    def _fetch_order_realized_pnl(self, exchange_symbol: str, order_id: str) -> tuple[str, str]:
        """Return summed realized PnL and a failure reason for a futures order.

        Binance USDⓈ-M exposes realized PnL per fill via the account trades API;
        summing fills by orderId gives the final realized profit/loss for this
        reduce-only stop-loss order. The user-trades endpoint can lag right after
        a close order, so retry with configured delays before returning an empty
        PnL and writing the failure reason into the stop-loss audit record.
        """
        attempts = len(self.realized_pnl_retry_delays) + 1
        last_error = ""
        for attempt in range(1, attempts + 1):
            try:
                trades = self.account_manager._signed_get(
                    "/fapi/v1/userTrades",
                    {"symbol": exchange_symbol, "orderId": order_id},
                )
                if not isinstance(trades, list):
                    last_error = f"unexpected_user_trades_response_type={type(trades).__name__}"
                elif not trades:
                    last_error = "user_trades_empty"
                else:
                    realized = sum(
                        (
                            self._decimal_from(row.get("realizedPnl"), Decimal("0"))
                            for row in trades
                            if isinstance(row, dict)
                        ),
                        Decimal("0"),
                    )
                    return self._fmt_decimal(realized), ""
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

            print(
                f"⚠️ realized PnL query failed symbol={exchange_symbol} order_id={order_id} "
                f"attempt={attempt}/{attempts}: {last_error}"
            )
            if attempt <= len(self.realized_pnl_retry_delays):
                time.sleep(float(self.realized_pnl_retry_delays[attempt - 1]))

        return "", f"realized_pnl_query_failed_after_{attempts}_attempts: {last_error}"


    def _save_reduction_check(self, check: PositionReductionCheck) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.REDUCTION_CHECKS_TABLE}
                (symbol, decision_round_ts, highest_15m_high, current_price, price_drawdown_ratio, account_equity_usdt, two_r_usdt, one_r_usdt, unrealized_pnl, open_total_score, latest_total_score, score_drawdown, latest_15m_open, latest_15m_close, previous_total_score, recent_score_drawdown, open_entry_price, rule_name, triggered, tag, reason, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    highest_15m_high=excluded.highest_15m_high,
                    current_price=excluded.current_price,
                    price_drawdown_ratio=excluded.price_drawdown_ratio,
                    account_equity_usdt=excluded.account_equity_usdt,
                    two_r_usdt=excluded.two_r_usdt,
                    one_r_usdt=excluded.one_r_usdt,
                    unrealized_pnl=excluded.unrealized_pnl,
                    open_total_score=excluded.open_total_score,
                    latest_total_score=excluded.latest_total_score,
                    score_drawdown=excluded.score_drawdown,
                    latest_15m_open=excluded.latest_15m_open,
                    latest_15m_close=excluded.latest_15m_close,
                    previous_total_score=excluded.previous_total_score,
                    recent_score_drawdown=excluded.recent_score_drawdown,
                    open_entry_price=excluded.open_entry_price,
                    rule_name=excluded.rule_name,
                    triggered=excluded.triggered,
                    tag=excluded.tag,
                    reason=excluded.reason,
                    checked_at=excluded.checked_at
                """,
                (check.symbol, check.decision_round_ts, check.highest_15m_high, check.current_price, check.price_drawdown_ratio, check.account_equity_usdt, check.two_r_usdt, check.one_r_usdt, check.unrealized_pnl, check.open_total_score, check.latest_total_score, check.score_drawdown, check.latest_15m_open, check.latest_15m_close, check.previous_total_score, check.recent_score_drawdown, check.open_entry_price, check.rule_name, int(check.triggered), check.tag, check.reason, check.checked_at),
            )

    def _save_reduction_record(self, record: PositionReductionRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.REDUCTION_RECORDS_TABLE}
                (symbol, decision_round_ts, side, matched_rule, reduction_percent, original_quantity, reduced_quantity, remaining_quantity, old_limit_order_id, new_limit_order_id, market_order_id, limit_price, status, reason, raw_response, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.symbol, record.decision_round_ts, record.side, record.matched_rule, record.reduction_percent, record.original_quantity, record.reduced_quantity, record.remaining_quantity, record.old_limit_order_id, record.new_limit_order_id, record.market_order_id, record.limit_price, record.status, record.reason, record.raw_response, record.created_at),
            )

    def _has_reduction_record(self, symbol: str, decision_round_ts: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.REDUCTION_RECORDS_TABLE} WHERE symbol = ? AND decision_round_ts = ? LIMIT 1",
                (symbol, decision_round_ts),
            ).fetchone()
        return row is not None

    def recent_reduction_records(self, limit: int = 100) -> list[sqlite3.Row]:
        self.init_tables()
        with self._connect() as conn:
            return conn.execute(
                f"SELECT * FROM {self.REDUCTION_RECORDS_TABLE} ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def get_latest_reduction_checks(self) -> tuple[int | None, list[sqlite3.Row]]:
        self.init_tables()
        with self._connect() as conn:
            row = conn.execute(f"SELECT MAX(decision_round_ts) AS ts FROM {self.REDUCTION_CHECKS_TABLE}").fetchone()
            round_ts = int(row["ts"]) if row and row["ts"] is not None else None
            if round_ts is None:
                return None, []
            rows = conn.execute(
                f"SELECT * FROM {self.REDUCTION_CHECKS_TABLE} WHERE decision_round_ts = ? ORDER BY triggered DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _save_check(self, check: HoldingStopLossCheck) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.CHECKS_TABLE}
                (symbol, decision_round_ts, triggered, latest_15m_open_time, latest_15m_close, latest_structural_stop_loss, prev_15m_open_time, prev_15m_close, prev_structural_stop_loss, reason, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    triggered=excluded.triggered,
                    latest_15m_open_time=excluded.latest_15m_open_time,
                    latest_15m_close=excluded.latest_15m_close,
                    latest_structural_stop_loss=excluded.latest_structural_stop_loss,
                    prev_15m_open_time=excluded.prev_15m_open_time,
                    prev_15m_close=excluded.prev_15m_close,
                    prev_structural_stop_loss=excluded.prev_structural_stop_loss,
                    reason=excluded.reason,
                    checked_at=excluded.checked_at
                """,
                (check.symbol, check.decision_round_ts, int(check.triggered), check.latest_15m_open_time, check.latest_15m_close, check.latest_structural_stop_loss, check.prev_15m_open_time, check.prev_15m_close, check.prev_structural_stop_loss, check.reason, check.checked_at),
            )

    def _save_record(self, record: HoldingStopLossRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.RECORDS_TABLE}
                (symbol, decision_round_ts, side, quantity, latest_15m_close, latest_structural_stop_loss, prev_15m_close, prev_structural_stop_loss, status, order_id, realized_pnl, reason, raw_response, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.symbol, record.decision_round_ts, record.side, record.quantity, record.latest_15m_close, record.latest_structural_stop_loss, record.prev_15m_close, record.prev_structural_stop_loss, record.status, record.order_id, record.realized_pnl, record.reason, record.raw_response, record.created_at),
            )

    def _has_stop_loss_record(self, symbol: str, decision_round_ts: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self.RECORDS_TABLE} WHERE symbol = ? AND decision_round_ts = ? LIMIT 1",
                (symbol, decision_round_ts),
            ).fetchone()
        return row is not None

    def get_latest_round_checks(self) -> tuple[int | None, list[sqlite3.Row]]:
        self.init_tables()
        with self._connect() as conn:
            row = conn.execute(f"SELECT MAX(decision_round_ts) AS ts FROM {self.CHECKS_TABLE}").fetchone()
            round_ts = int(row["ts"]) if row and row["ts"] is not None else None
            if round_ts is None:
                return None, []
            rows = conn.execute(
                f"SELECT * FROM {self.CHECKS_TABLE} WHERE decision_round_ts = ? ORDER BY triggered DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def recent_stop_loss_records(self, limit: int = 100) -> list[sqlite3.Row]:
        self.init_tables()
        with self._connect() as conn:
            return conn.execute(
                f"""
                SELECT * FROM {self.RECORDS_TABLE}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()


    @staticmethod
    def _base_symbol(symbol: str) -> str:
        normalized = str(symbol).strip().upper()
        if normalized.endswith("USDT"):
            return normalized[:-4]
        return normalized

    @staticmethod
    def _exchange_symbol(position: dict[str, Any], base_symbol: str) -> str:
        exchange_symbol = str(position.get("symbol", "")).strip().upper()
        if exchange_symbol:
            return exchange_symbol
        return f"{base_symbol}USDT" if base_symbol and not base_symbol.endswith("USDT") else base_symbol

    @staticmethod
    def _current_decision_round_ts() -> int:
        round_ms = 15 * 60_000
        return (int(time.time() * 1000) // round_ms) * round_ms

    @staticmethod
    def _decimal_from(value: Any, default: Decimal) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return default

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        normalized = value.normalize()
        return format(normalized, "f")
