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
                f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_round "
                f"ON {self.CHECKS_TABLE}(decision_round_ts DESC, triggered DESC, symbol ASC)"
            )
            record_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.RECORDS_TABLE})").fetchall()}
            if "realized_pnl" not in record_columns:
                conn.execute(f"ALTER TABLE {self.RECORDS_TABLE} ADD COLUMN realized_pnl TEXT NOT NULL DEFAULT ''")
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
        return {"decision_round_ts": round_ts, "checked": checked, "triggered": triggered, "records": records}

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
            order_id = str(response.get("orderId", "")) if isinstance(response, dict) else ""
            raw_response = str(response)
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
        """Cancel normal and conditional open orders before a MARKET close."""
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
        a MARKET close, so retry with configured delays before returning an empty
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
                f"SELECT * FROM {self.RECORDS_TABLE} ORDER BY created_at DESC, id DESC LIMIT ?",
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
