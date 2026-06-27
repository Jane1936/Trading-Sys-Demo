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
from decimal import Decimal
from typing import Any, Iterable

from binance_account_manager import BinanceAccountManager
from trading_experiment import TradingExperiment


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
    unrealized_pnl: str
    open_total_score: str
    latest_total_score: str
    score_drawdown: str
    triggered: bool
    tag: str
    reason: str
    checked_at: int


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


class HoldingPositionScoringSystem:
    """Evaluate held symbols and execute structural stop-loss exits."""

    CHECKS_TABLE = "holding_stop_loss_checks"
    RECORDS_TABLE = "holding_stop_loss_records"
    PORTFOLIO_RISK_TABLE = "holding_portfolio_risk_checks"
    PORTFOLIO_RISK_SUMMARY_TABLE = "holding_portfolio_risk_summaries"
    REDUCTION_CHECKS_TABLE = "holding_position_reduction_checks"
    REDUCTION_TAG_ABSOLUTE_SCORE_DRAWDOWN = "绝对分数大幅回撤"

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
                    unrealized_pnl TEXT NOT NULL,
                    open_total_score TEXT NOT NULL,
                    latest_total_score TEXT NOT NULL,
                    score_drawdown TEXT NOT NULL,
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
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.RECORDS_TABLE}_created "
                f"ON {self.RECORDS_TABLE}(created_at DESC, symbol ASC)"
            )

    def run_round(self, decision_round_ts: int | None = None) -> dict[str, Any]:
        """Run one 15m holding-position stop-loss round."""
        self.account_manager.validate_config()
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        now_ms = int(time.time() * 1000)
        positions = self._active_positions()
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
        reduction_checks = self.evaluate_reduction_conditions(positions=positions, decision_round_ts=round_ts, checked_at=now_ms)
        portfolio_risk = self.calculate_portfolio_risk(positions=positions, decision_round_ts=round_ts, calculated_at=now_ms)
        risk_action = self._enforce_portfolio_risk_limit(portfolio_risk, positions, now_ms)
        return {
            "decision_round_ts": round_ts,
            "checked": checked,
            "triggered": triggered,
            "records": records,
            "reduction_checked": len(reduction_checks),
            "reduction_triggered": sum(1 for row in reduction_checks if row.triggered),
            "total_risk": portfolio_risk.total_risk,
            "risk_position_count": portfolio_risk.position_count,
            "portfolio_risk_action": risk_action,
        }


    def evaluate_reduction_conditions(
        self,
        positions: list[dict[str, Any]] | None = None,
        decision_round_ts: int | None = None,
        checked_at: int | None = None,
    ) -> list[PositionReductionCheck]:
        """Evaluate 15m position-reduction condition rule 1 for active holdings."""
        self.init_tables()
        round_ts = decision_round_ts if decision_round_ts is not None else self._current_decision_round_ts()
        now_ms = checked_at if checked_at is not None else int(time.time() * 1000)
        active_positions = positions if positions is not None else self._active_positions()
        equity = TradingExperiment(self.db_path, account_manager=self.account_manager)._fetch_experiment_usdt_equity()
        two_r = equity * Decimal("0.02")
        checks: list[PositionReductionCheck] = []
        for position in active_positions:
            symbol = self._base_symbol(str(position.get("symbol", "")))
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            if not symbol or amount == 0:
                continue
            check = self._evaluate_reduction_rule_one(position, symbol, round_ts, equity, two_r, now_ms)
            self._save_reduction_check(check)
            checks.append(check)
        return checks

    def _evaluate_reduction_rule_one(
        self, position: dict[str, Any], symbol: str, round_ts: int, equity: Decimal, two_r: Decimal, now_ms: int
    ) -> PositionReductionCheck:
        highest = self._highest_recent_15m_high(symbol, limit=3)
        exchange_symbol = self._exchange_symbol(position, symbol)
        current_price = self._current_symbol_price(exchange_symbol, position)
        drawdown = (highest - current_price) / highest if highest > 0 and current_price > 0 else Decimal("0")
        unrealized_pnl = self._position_unrealized_pnl(position)
        latest_score = self._latest_total_score(symbol)
        open_score = self._latest_open_trade_total_score(symbol)
        score_drawdown = Decimal("0")
        triggered = False
        tag = ""
        reason = "price_drawdown_lt_3_percent"
        if highest <= 0:
            reason = "missing_recent_three_15m_highs"
        elif current_price <= 0:
            reason = "missing_current_price"
        elif drawdown < Decimal("0.03"):
            reason = "price_drawdown_lt_3_percent"
        elif unrealized_pnl >= two_r:
            reason = "unrealized_pnl_ge_2r"
        elif open_score == "" or latest_score == "":
            reason = "missing_open_or_latest_total_score"
        else:
            score_drawdown = self._decimal_from(open_score, Decimal("0")) - self._decimal_from(latest_score, Decimal("0"))
            if score_drawdown >= Decimal("25"):
                triggered = True
                tag = self.REDUCTION_TAG_ABSOLUTE_SCORE_DRAWDOWN
                reason = "absolute_score_large_drawdown"
            else:
                reason = "score_drawdown_lt_25"
        return PositionReductionCheck(symbol, round_ts, self._fmt_decimal(highest), self._fmt_decimal(current_price), self._fmt_decimal(drawdown), self._fmt_decimal(equity), self._fmt_decimal(two_r), self._fmt_decimal(unrealized_pnl), open_score, latest_score, self._fmt_decimal(score_drawdown), triggered, tag, reason, now_ms)

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
        for position in active_positions[:10]:
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
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT total_score FROM symbol_total_scores WHERE symbol = ? ORDER BY decision_round_ts DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row["total_score"]) if row and row["total_score"] is not None else ""

    def _enforce_portfolio_risk_limit(self, summary: PortfolioRiskSummary, positions: list[dict[str, Any]], now_ms: int) -> str:
        if self._decimal_from(summary.total_risk, Decimal("0")) <= Decimal("18") or not summary.positions:
            return ""

        lowest_score = min(self._decimal_from(row.total_score, Decimal("999999999")) for row in summary.positions)
        lowest_rows = [
            row
            for row in sorted(summary.positions, key=lambda item: item.symbol)
            if self._decimal_from(row.total_score, Decimal("999999999")) == lowest_score
        ]

        if len(lowest_rows) == 1:
            return self._execute_portfolio_risk_liquidation(lowest_rows[0], positions, summary, now_ms)

        actions: list[str] = []
        for row in lowest_rows:
            position = next((p for p in positions if self._base_symbol(str(p.get("symbol", ""))) == row.symbol), None)
            if not position:
                actions.append(f"portfolio_risk_gt_18_no_position_found_for_lowest_score_symbol={row.symbol}")
                continue
            unrealized_pnl = self._position_unrealized_pnl(position)
            if unrealized_pnl < 0:
                actions.append(self._execute_portfolio_risk_liquidation(row, positions, summary, now_ms, unrealized_pnl=unrealized_pnl))
            else:
                actions.append(f"skipped_non_negative_unrealized_pnl_symbol={row.symbol}; unrealized_pnl={self._fmt_decimal(unrealized_pnl)}")
        return " | ".join(actions)

    def _execute_portfolio_risk_liquidation(
        self,
        risk_row: PortfolioRiskPosition,
        positions: list[dict[str, Any]],
        summary: PortfolioRiskSummary,
        now_ms: int,
        unrealized_pnl: Decimal | None = None,
    ) -> str:
        position = next((p for p in positions if self._base_symbol(str(p.get("symbol", ""))) == risk_row.symbol), None)
        if not position:
            return f"portfolio_risk_gt_18_no_position_found_for_lowest_score_symbol={risk_row.symbol}"
        amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
        if amount == 0:
            return f"portfolio_risk_gt_18_lowest_score_symbol_zero_position={risk_row.symbol}"
        exchange_symbol = self._exchange_symbol(position, risk_row.symbol)
        side = "SELL" if amount > 0 else "BUY"
        quantity = abs(amount)
        cancel_reason = self._cancel_existing_exit_orders(exchange_symbol)
        try:
            response = self.account_manager._signed_post("/fapi/v1/order", self._market_close_order_params(exchange_symbol, side, quantity))
            order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            pnl_reason = f"; unrealized_pnl={self._fmt_decimal(unrealized_pnl)}" if unrealized_pnl is not None else ""
            record = HoldingStopLossRecord(0, risk_row.symbol, summary.decision_round_ts, side, self._fmt_decimal(quantity), 0, 0, 0, 0, "submitted", order_id, "", f"portfolio_total_risk_gt_18; total_risk={summary.total_risk}; total_score={risk_row.total_score}{pnl_reason}; {cancel_reason}", str(response), now_ms)
            self._save_record(record)
            return f"submitted_market_close_symbol={risk_row.symbol}; order_id={order_id}"
        except Exception as exc:
            return f"portfolio_risk_market_close_failed_symbol={risk_row.symbol}: {type(exc).__name__}: {exc}; {cancel_reason}"

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
        cancel_reason = self._cancel_existing_exit_orders(exchange_symbol)
        if cancel_reason:
            reason = f"{reason}; {cancel_reason}"
        try:
            response = self.account_manager._signed_post("/fapi/v1/order", self._market_close_order_params(exchange_symbol, side, quantity))
            order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            raw_response = str(response)
            no_fill_reason = self._no_fill_order_response_reason(response)
            if no_fill_reason:
                status = "failed"
                reason = f"{reason}; {no_fill_reason}"
                order_id = ""
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
        return HoldingStopLossRecord(0, check.symbol, check.decision_round_ts, side, self._fmt_decimal(quantity), float(check.latest_15m_close or 0), float(check.latest_structural_stop_loss or 0), float(check.prev_15m_close or 0), float(check.prev_structural_stop_loss or 0), status, order_id, realized_pnl, reason, raw_response, now_ms)

    def _cancel_existing_exit_orders(self, exchange_symbol: str) -> str:
        """Cancel normal and conditional open orders before a reduce-only MARKET close."""
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
            return "pre_close_cancel_warnings: " + " | ".join(failures)
        return "pre_close_cancelled_existing_open_orders"

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
                (symbol, decision_round_ts, highest_15m_high, current_price, price_drawdown_ratio, account_equity_usdt, two_r_usdt, unrealized_pnl, open_total_score, latest_total_score, score_drawdown, triggered, tag, reason, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    highest_15m_high=excluded.highest_15m_high,
                    current_price=excluded.current_price,
                    price_drawdown_ratio=excluded.price_drawdown_ratio,
                    account_equity_usdt=excluded.account_equity_usdt,
                    two_r_usdt=excluded.two_r_usdt,
                    unrealized_pnl=excluded.unrealized_pnl,
                    open_total_score=excluded.open_total_score,
                    latest_total_score=excluded.latest_total_score,
                    score_drawdown=excluded.score_drawdown,
                    triggered=excluded.triggered,
                    tag=excluded.tag,
                    reason=excluded.reason,
                    checked_at=excluded.checked_at
                """,
                (check.symbol, check.decision_round_ts, check.highest_15m_high, check.current_price, check.price_drawdown_ratio, check.account_equity_usdt, check.two_r_usdt, check.unrealized_pnl, check.open_total_score, check.latest_total_score, check.score_drawdown, int(check.triggered), check.tag, check.reason, check.checked_at),
            )

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
                WHERE reason NOT LIKE 'portfolio_total_risk_gt_18%'
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def recent_portfolio_liquidation_records(self, limit: int = 100) -> list[sqlite3.Row]:
        self.init_tables()
        with self._connect() as conn:
            return conn.execute(
                f"""
                SELECT * FROM {self.RECORDS_TABLE}
                WHERE reason LIKE 'portfolio_total_risk_gt_18%'
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
