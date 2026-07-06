"""First long-only trading experiment for the Binance Futures demo account.

The experiment intentionally keeps trading rules, order placement, and audit
persistence in one isolated module so it can be reviewed independently before
being scheduled or called from the web UI.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any, Iterable

from binance_account_manager import BinanceAccountManager
from openable_symbol_module import OpenableSymbol, OpenableSymbolModule


@dataclass(frozen=True)
class ExperimentConfig:
    """Configurable risk controls for the first experiment group."""

    initial_equity_usdt: Decimal = Decimal("1000")
    experiment_uninvested_usdt: Decimal = Decimal("4000")
    total_margin_budget_usdt: Decimal = Decimal("1000")
    risk_fraction: Decimal = Decimal("0.01")
    max_stop_loss_pct: Decimal = Decimal("0.10")
    exit_order_missing_position_retries: int = 3
    exit_order_missing_position_retry_delay_seconds: Decimal = Decimal("0.5")
    percent_price_ioc_slippage: Decimal = Decimal("0.01")
    hard_take_profit_usdt: Decimal = Decimal("55")


@dataclass(frozen=True)
class TradePlan:
    leverage: int
    stop_loss_distance_ratio: Decimal
    required_margin_usdt: Decimal
    planned_notional_usdt: Decimal


@dataclass(frozen=True)
class ExperimentTradeRecord:
    id: int
    symbol: str
    decision_round_ts: int | None
    side: str
    status: str
    total_score: int | None
    leverage: int | None
    allocated_notional_usdt: str
    required_margin_usdt: str
    account_equity_usdt: str
    max_loss_usdt: str
    entry_price: str
    quantity: str
    notional_usdt: str
    take_profit_price: str
    stop_loss_price: str
    stop_loss_calculation: str
    take_profit_order_id: str
    stop_loss_order_id: str
    reason: str
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class ExperimentPositionSnapshot:
    id: int
    symbol: str
    position_amt: str
    entry_price: str
    mark_price: str
    unrealized_pnl: str
    leverage: str
    notional: str
    liquidation_price: str
    opened_at: int | None
    holding_hours: str
    updated_at: int


@dataclass(frozen=True)
class ExperimentErrorRecord:
    id: int
    symbol: str
    decision_round_ts: int | None
    total_score: int | None
    leverage: int | None
    operation: str
    error_type: str
    error_message: str
    created_at: int


class TradingExperiment:
    """Run and persist the first long-only trading experiment.

    Rules implemented:
    * experiment equity matches the web page "experiment USDT equity" metric;
    * open-position count is not capped; new entries are allowed as long as
      available balance and the experiment margin budget can cover the order;
    * the experiment's total margin budget is capped at 1,000 USDT;
    * before each new entry, query the latest experiment USDT equity from Binance;
    * each candidate's base margin is sized from 1% equity risk, stop-loss distance,
      and leverage: base margin = (equity * 1%) / (distance_ratio * leverage);
    * required margin uses the full formula result, and planned notional is margin * leverage;
    * stop-loss price distance remains capped by min(entry * 10%, equity * 1% / quantity);
    * new entries place a hard take-profit order that closes when position PnL reaches 55 USDT;
    * stop-loss is also placed for the full visible position;
    * candidates come from the latest qualified openable-symbol round and are
      processed by total_score descending, then symbol ascending.
    """

    TRADES_TABLE = "trading_experiment_trades"
    POSITIONS_TABLE = "trading_experiment_position_snapshots"
    ERRORS_TABLE = "trading_experiment_error_records"

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
                CREATE TABLE IF NOT EXISTS {self.TRADES_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    leverage INTEGER,
                    allocated_usdt TEXT NOT NULL,
                    required_margin_usdt TEXT NOT NULL DEFAULT '0',
                    account_equity_usdt TEXT NOT NULL,
                    max_loss_usdt TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    notional_usdt TEXT NOT NULL,
                    take_profit_price TEXT NOT NULL,
                    stop_loss_price TEXT NOT NULL,
                    stop_loss_calculation TEXT NOT NULL DEFAULT '',
                    take_profit_order_id TEXT NOT NULL DEFAULT '',
                    stop_loss_order_id TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL,
                    raw_response TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.POSITIONS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    position_amt TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    mark_price TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL,
                    leverage TEXT NOT NULL,
                    notional TEXT NOT NULL,
                    liquidation_price TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.ERRORS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER,
                    total_score INTEGER,
                    leverage INTEGER,
                    operation TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
                """
            )
            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.TRADES_TABLE})").fetchall()}
            if "required_margin_usdt" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.TRADES_TABLE} "
                    "ADD COLUMN required_margin_usdt TEXT NOT NULL DEFAULT '0'"
                )
            if "stop_loss_calculation" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.TRADES_TABLE} "
                    "ADD COLUMN stop_loss_calculation TEXT NOT NULL DEFAULT ''"
                )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.TRADES_TABLE}_created "
                f"ON {self.TRADES_TABLE}(created_at DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.POSITIONS_TABLE}_updated "
                f"ON {self.POSITIONS_TABLE}(updated_at DESC, symbol ASC)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.ERRORS_TABLE}_created "
                f"ON {self.ERRORS_TABLE}(created_at DESC, symbol ASC)"
            )

    @staticmethod
    def current_decision_round_ts(now_ms: int | None = None) -> int:
        """Return the current 15-minute decision-round timestamp in milliseconds."""
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        round_ms = 15 * 60_000
        return (now_ms // round_ms) * round_ms

    def run_latest_round(self, decision_round_ts: int | None = None) -> dict[str, Any]:
        """Run one experiment pass against openable symbols already evaluated for a round.

        When ``decision_round_ts`` is provided (for scheduled 15-minute scans),
        only that exact round is eligible. This prevents a timer from opening
        positions from a stale previous round before the current round's
        ``本轮可开仓 symbol 情况`` has completed.
        """
        self.init_tables()
        candidates = self._latest_openable_candidates(decision_round_ts=decision_round_ts)
        if not candidates:
            reason = "current_round_openable_not_ready" if decision_round_ts is not None else "no_qualified_openable_symbols"
            return {"opened": 0, "skipped": 0, "reason": reason}
        return self.run_round(candidates)

    def run_round(self, candidates: Iterable[OpenableSymbol]) -> dict[str, Any]:
        """Attempt long entries for qualified candidates in score order."""
        self.account_manager.validate_config()
        self.init_tables()

        account = self._fetch_account()
        available_balance = self._decimal_from(account.get("availableBalance"), Decimal("0"))
        account_equity = self._fetch_experiment_usdt_equity()
        max_loss = account_equity * self.config.risk_fraction
        positions = self._fetch_and_store_positions()
        reserved_margin_budget = self._reserved_margin_from_positions(positions)

        opened = 0
        skipped = 0
        eligible_candidates = [
            candidate
            for candidate in candidates
            if self._candidate_allows_open(candidate)
        ]
        for candidate in sorted(eligible_candidates, key=lambda row: (-row.total_score, row.symbol)):
            trading_symbol = self._binance_symbol(candidate.symbol)
            if self._has_open_position(trading_symbol, positions):
                self._record_skip(candidate, account_equity, max_loss, "symbol_position_already_open")
                skipped += 1
                continue
            account = self._fetch_account()
            available_balance = self._decimal_from(account.get("availableBalance"), Decimal("0"))
            account_equity = self._fetch_experiment_usdt_equity()
            max_loss = account_equity * self.config.risk_fraction
            trade_plan = self._trade_plan(candidate, account_equity)
            required_margin = trade_plan.required_margin_usdt
            if reserved_margin_budget + required_margin > self.config.total_margin_budget_usdt:
                self._record_skip(candidate, account_equity, max_loss, "total_margin_budget_exhausted", required_margin)
                skipped += 1
                break
            if available_balance < required_margin:
                self._record_skip(candidate, account_equity, max_loss, "available_balance_lt_required_margin", required_margin)
                skipped += 1
                break

            try:
                result = self._open_long(candidate, account_equity, max_loss, trade_plan)
            except RuntimeError as exc:
                self._record_error(candidate, "open_long", exc)
                error_message = str(exc)
                if "not found in exchangeInfo" in error_message:
                    self._record_skip(candidate, account_equity, max_loss, "symbol_not_found_in_exchange_info")
                    skipped += 1
                    continue
                if "Invalid latest price" in error_message:
                    self._record_skip(candidate, account_equity, max_loss, "invalid_latest_price")
                    skipped += 1
                    continue
                if self._is_invalid_symbol_error(exc):
                    self._record_skip(candidate, account_equity, max_loss, "invalid_binance_symbol")
                    skipped += 1
                    continue
                if self._is_invalid_leverage_error(exc):
                    self._record_skip(candidate, account_equity, max_loss, "invalid_binance_leverage")
                    skipped += 1
                    continue
                raise
            except Exception as exc:
                self._record_error(candidate, "open_long", exc)
                if self._is_invalid_symbol_error(exc):
                    self._record_skip(candidate, account_equity, max_loss, "invalid_binance_symbol")
                    skipped += 1
                    continue
                if self._is_invalid_leverage_error(exc):
                    self._record_skip(candidate, account_equity, max_loss, "invalid_binance_leverage")
                    skipped += 1
                    continue
                raise

            if result["status"] == "opened":
                opened += 1
                available_balance -= required_margin
                reserved_margin_budget += required_margin
                positions = self._fetch_and_store_positions()
            else:
                skipped += 1

        self._fetch_and_store_positions()
        return {"opened": opened, "skipped": skipped, "reason": "completed"}

    def recent_trade_records(self, limit: int = 100, since_ms: int | None = None) -> list[ExperimentTradeRecord]:
        self.init_tables()
        since_clause = "AND created_at >= ?" if since_ms is not None else ""
        params: tuple[int, ...]
        if since_ms is not None:
            params = (int(since_ms), int(limit))
        else:
            params = (int(limit),)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self.TRADES_TABLE}
                WHERE status = 'opened' {since_clause}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._trade_from_row(row) for row in rows]

    def latest_position_snapshots(self, limit: int = 100) -> list[ExperimentPositionSnapshot]:
        self.init_tables()
        with self._connect() as conn:
            latest_ts = conn.execute(f"SELECT MAX(updated_at) AS updated_at FROM {self.POSITIONS_TABLE}").fetchone()["updated_at"]
            if latest_ts is None:
                return []
            rows = conn.execute(
                f"""
                SELECT
                    p.*,
                    (
                        SELECT MAX(t.created_at)
                        FROM {self.TRADES_TABLE} AS t
                        WHERE t.status = 'opened'
                          AND UPPER(t.symbol) = UPPER(p.symbol)
                          AND t.created_at <= p.updated_at
                    ) AS opened_at
                FROM {self.POSITIONS_TABLE} AS p
                WHERE p.updated_at = ?
                ORDER BY ABS(CAST(p.position_amt AS REAL)) DESC, p.symbol ASC
                LIMIT ?
                """,
                (latest_ts, int(limit)),
            ).fetchall()
        return [self._position_from_row(row) for row in rows]

    def recent_error_records(self, limit: int = 100, since_ms: int | None = None) -> list[ExperimentErrorRecord]:
        self.init_tables()
        if since_ms is not None:
            query = f"""
                SELECT * FROM {self.ERRORS_TABLE}
                WHERE created_at >= ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
            """
            params = (int(since_ms), int(limit))
        else:
            query = f"SELECT * FROM {self.ERRORS_TABLE} ORDER BY created_at DESC, id DESC LIMIT ?"
            params = (int(limit),)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._error_from_row(row) for row in rows]

    def _latest_openable_candidates(self, decision_round_ts: int | None = None) -> list[OpenableSymbol]:
        module = OpenableSymbolModule(db_path=self.db_path)
        module.init_table()
        with module._connect() as conn:
            if decision_round_ts is None:
                round_row = conn.execute(
                    f"SELECT MAX(decision_round_ts) AS decision_round_ts FROM {module.TABLE_NAME} WHERE qualified = 1"
                ).fetchone()
                decision_round_ts = round_row["decision_round_ts"] if round_row else None
            if decision_round_ts is None:
                return []
            rows = conn.execute(
                f"""
                SELECT * FROM {module.TABLE_NAME}
                WHERE decision_round_ts = ? AND qualified = 1
                  AND CAST(REPLACE(LOWER(opening_leverage), 'x', '') AS INTEGER) > 0
                ORDER BY total_score DESC, symbol ASC
                """,
                (decision_round_ts,),
            ).fetchall()
        return [
            OpenableSymbol(
                symbol=str(row["symbol"]),
                decision_round_ts=int(row["decision_round_ts"]),
                total_score=int(row["total_score"]),
                score_band=str(row["score_band"]),
                stop_loss_distance_ratio=float(row["stop_loss_distance_ratio"]) if row["stop_loss_distance_ratio"] is not None else None,
                distance_threshold=float(row["distance_threshold"]) if row["distance_threshold"] is not None else None,
                stop_loss_distance_tier=str(row["stop_loss_distance_tier"]),
                opening_leverage=str(row["opening_leverage"]),
                distance_qualified=bool(row["distance_qualified"]),
                qualified=bool(row["qualified"]),
                reason=str(row["reason"]),
                evaluated_at=int(row["evaluated_at"]),
            )
            for row in rows
        ]

    @classmethod
    def _candidate_allows_open(cls, candidate: OpenableSymbol) -> bool:
        """Only first-experiment rows with final-openable=yes and usable leverage may open."""
        if not candidate.qualified:
            return False
        return cls._parse_leverage(candidate.opening_leverage) > 0

    def _open_long(
        self,
        candidate: OpenableSymbol,
        account_equity: Decimal,
        max_loss: Decimal,
        trade_plan: TradePlan | None = None,
    ) -> dict[str, Any]:
        now = int(time.time() * 1000)
        trade_plan = trade_plan or self._trade_plan(candidate, account_equity)
        leverage = trade_plan.leverage
        if leverage <= 0:
            self._record_skip(candidate, account_equity, max_loss, "invalid_opening_leverage")
            return {"status": "skipped"}

        trading_symbol = self._binance_symbol(candidate.symbol)
        exchange_info = self._exchange_symbol_info(trading_symbol)
        pre_order_price = self._latest_price(trading_symbol)
        planned_notional = trade_plan.planned_notional_usdt
        quantity = self._floor_to_step(planned_notional / pre_order_price, exchange_info["step_size"])
        if quantity <= 0:
            self._record_skip(candidate, account_equity, max_loss, "quantity_rounded_to_zero")
            return {"status": "skipped"}

        self.account_manager._signed_post("/fapi/v1/leverage", {"symbol": trading_symbol, "leverage": leverage})
        market_order = self._signed_post_order_with_ioc_retry(
            "/fapi/v1/order",
            {
                "symbol": trading_symbol,
                "side": "BUY",
                "type": "MARKET",
                "quantity": self._fmt_decimal(quantity),
                "newOrderRespType": "RESULT",
            },
            tick_size=exchange_info["tick_size"],
        )

        order_entry_price = self._filled_entry_price(market_order, pre_order_price)
        position_amt, position_entry_price = self._wait_for_open_position(candidate, trading_symbol)
        position_visible = position_amt > 0
        entry_price = position_entry_price if position_entry_price > 0 else order_entry_price
        risk_quantity = abs(position_amt) if position_visible and position_entry_price > 0 else quantity
        trigger_reference_price = self._latest_mark_price(trading_symbol, entry_price)
        notional = risk_quantity * entry_price
        required_margin = notional / Decimal(leverage)
        take_profit_price = self._hard_take_profit_price(
            entry_price=entry_price,
            quantity=risk_quantity,
            profit_usdt=self.config.hard_take_profit_usdt,
            tick_size=exchange_info["tick_size"],
        )
        stop_loss_price, stop_loss_calculation = self._risk_capped_stop_loss_price_with_detail(
            entry_price=entry_price,
            quantity=risk_quantity,
            max_loss=max_loss,
            trigger_reference_price=trigger_reference_price,
            tick_size=exchange_info["tick_size"],
        )

        tp_order = None
        sl_order = None
        if position_visible:
            stop_loss_quantity = self._floor_to_step(abs(position_amt), exchange_info["step_size"])
            if stop_loss_quantity <= 0:
                stop_loss_quantity = quantity
            sl_order = self._place_exit_order(
                candidate,
                trading_symbol,
                {
                    "symbol": trading_symbol,
                    "side": "SELL",
                    "type": "STOP",
                    "quantity": self._fmt_decimal(stop_loss_quantity),
                    "price": self._fmt_decimal(stop_loss_price),
                    "stopPrice": self._fmt_decimal(stop_loss_price),
                    "timeInForce": "GTC",
                    "reduceOnly": "true",
                    "workingType": "MARK_PRICE",
                },
                "place_stop_loss",
            )
            take_profit_quantity = self._floor_to_step(abs(position_amt), exchange_info["step_size"])
            if take_profit_quantity <= 0:
                take_profit_quantity = quantity
            tp_order = self._place_exit_order(
                candidate,
                trading_symbol,
                {
                    "symbol": trading_symbol,
                    "side": "SELL",
                    "type": "TAKE_PROFIT",
                    "quantity": self._fmt_decimal(take_profit_quantity),
                    "price": self._fmt_decimal(take_profit_price),
                    "stopPrice": self._fmt_decimal(take_profit_price),
                    "timeInForce": "GTC",
                    "reduceOnly": "true",
                    "workingType": "MARK_PRICE",
                },
                "place_hard_take_profit",
            )
        tp_order_id = self._exit_order_id(tp_order)
        sl_order_id = self._exit_order_id(sl_order)
        exit_order_status = []
        exit_order_status.append("tp_order_id=" + (tp_order_id or "failed"))
        exit_order_status.append("sl_order_id=" + (sl_order_id or "failed"))

        self._insert_trade(
            candidate=candidate,
            side="LONG",
            status="opened",
            leverage=leverage,
            account_equity=account_equity,
            max_loss=max_loss,
            entry_price=entry_price,
            quantity=quantity,
            notional=notional,
            required_margin=required_margin,
            allocated_notional=planned_notional,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            stop_loss_calculation=stop_loss_calculation,
            take_profit_order_id=tp_order_id,
            stop_loss_order_id=sl_order_id,
            reason=f"market_order_id={market_order.get('orderId', '')}; " + "; ".join(exit_order_status),
            raw_response=str({"market": market_order, "tp": tp_order, "sl": sl_order}),
            now=now,
        )
        return {"status": "opened", "required_margin": self._fmt_decimal(required_margin)}

    def _signed_post_order_with_ioc_retry(
        self,
        endpoint: str,
        params: dict[str, Any],
        trading_symbol: str | None = None,
        tick_size: Decimal | None = None,
    ) -> dict[str, Any]:
        """Submit an order and retry Binance -4131 MARKET rejections as LIMIT IOC.

        Binance can reject MARKET orders with -4131 when the counterparty best
        price violates the PERCENT_PRICE filter. The retry keeps the original
        side/quantity/reduceOnly flags, changes the order to LIMIT IOC, and caps
        the limit price at 1% slippage from the latest observed price.
        """
        try:
            response = self.account_manager._signed_post(endpoint, params)
        except Exception as exc:
            if not self._should_retry_percent_price_as_ioc(endpoint, params, exc):
                raise
            retry_params = self._limit_ioc_retry_params(
                params,
                trading_symbol=trading_symbol,
                tick_size=tick_size,
            )
            try:
                return self.account_manager._signed_post("/fapi/v1/order", retry_params)
            except Exception as retry_exc:
                raise RuntimeError(
                    f"percent_price_ioc_retry_failed: original={exc}; "
                    f"retry={retry_exc}; retry_params={retry_params}"
                ) from retry_exc
        return response if isinstance(response, dict) else {"raw_response": response}

    def _limit_ioc_retry_params(
        self,
        params: dict[str, Any],
        trading_symbol: str | None = None,
        tick_size: Decimal | None = None,
    ) -> dict[str, Any]:
        return self._limit_ioc_order_params(params, trading_symbol=trading_symbol, tick_size=tick_size)

    def _limit_ioc_order_params(
        self,
        params: dict[str, Any],
        trading_symbol: str | None = None,
        tick_size: Decimal | None = None,
    ) -> dict[str, Any]:
        symbol = str(trading_symbol or params.get("symbol") or "").upper()
        side = str(params.get("side") or "").upper()
        price = self._latest_price(symbol)
        if tick_size is None:
            tick_size = self._exchange_symbol_info(symbol)["tick_size"]
        if side == "BUY":
            limit_price = self._ceil_to_tick(
                price * (Decimal("1") + self.config.percent_price_ioc_slippage),
                tick_size,
            )
        else:
            limit_price = self._floor_to_tick(
                price * (Decimal("1") - self.config.percent_price_ioc_slippage),
                tick_size,
            )
            if limit_price <= 0:
                limit_price = tick_size
        retry_params = {
            key: value
            for key, value in params.items()
            if key not in {"type", "stopPrice", "triggerPrice", "closePosition", "workingType", "priceProtect"}
        }
        retry_params.update(
            {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": "IOC",
                "price": self._fmt_decimal(limit_price),
                "newOrderRespType": params.get("newOrderRespType", "RESULT"),
            }
        )
        return retry_params

    @staticmethod
    def _should_retry_percent_price_as_ioc(endpoint: str, params: dict[str, Any], exc: Exception) -> bool:
        message = str(exc)
        return (
            endpoint == "/fapi/v1/order"
            and str(params.get("type", "")).upper() == "MARKET"
            and bool(params.get("quantity"))
            and ("-4131" in message or "PERCENT_PRICE" in message)
        )

    def _wait_for_open_position(self, candidate: OpenableSymbol, trading_symbol: str) -> tuple[Decimal, Decimal]:
        max_attempts = max(1, int(self.config.exit_order_missing_position_retries) + 1)
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                position_row = self._position_risk_row(trading_symbol)
                position_amt = (
                    self._decimal_from(position_row.get("positionAmt"), Decimal("0"))
                    if position_row
                    else Decimal("0")
                )
                if position_amt > 0:
                    entry_price = (
                        self._decimal_from(position_row.get("entryPrice"), Decimal("0"))
                        if position_row
                        else Decimal("0")
                    )
                    return position_amt, entry_price
            except Exception as exc:
                last_exc = exc

            if attempt < max_attempts:
                delay_seconds = float(self.config.exit_order_missing_position_retry_delay_seconds)
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

        if last_exc is not None:
            self._record_error(candidate, f"wait_open_position:{trading_symbol}", last_exc)
        else:
            self._record_error(
                candidate,
                f"wait_open_position:{trading_symbol}",
                RuntimeError(f"positionAmt for {trading_symbol} was not positive after market order"),
            )
        return Decimal("0"), Decimal("0")

    def _place_exit_order(
        self,
        candidate: OpenableSymbol,
        trading_symbol: str,
        params: dict[str, Any],
        operation: str,
    ) -> dict[str, Any] | None:
        endpoint, request_params = self._exit_order_request(params)
        max_attempts = max(1, int(self.config.exit_order_missing_position_retries) + 1)
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return self._signed_post_order_with_ioc_retry(endpoint, request_params, trading_symbol=trading_symbol)
            except Exception as exc:
                last_exc = exc
                if not self._is_missing_position_for_close_position_error(exc) or attempt >= max_attempts:
                    break
                self._wait_for_position_visibility(trading_symbol)

        if last_exc is not None:
            self._record_error(candidate, f"{operation}:{trading_symbol}", last_exc)
        return None

    def reconcile_missing_exit_orders(self, checked_at: int | None = None) -> dict[str, Any]:
        """Create missing hard take-profit/stop-loss protection for live positions.

        Binance exit orders can fail independently after the entry MARKET order is
        already filled.  A later reconciliation pass compares live positions with
        currently open conditional orders and recreates any missing reduce-only
        protection from the latest opened trade row.
        """
        now = checked_at or int(time.time() * 1000)
        self.init_tables()
        positions = [
            row for row in self._fetch_and_store_positions()
            if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0
        ]
        checked = 0
        created = 0
        errors = 0
        records: list[dict[str, str]] = []
        for position in positions:
            checked += 1
            exchange_symbol = str(position.get("symbol", "")).upper()
            symbol = exchange_symbol[:-4] if exchange_symbol.endswith("USDT") else exchange_symbol
            amount = self._decimal_from(position.get("positionAmt"), Decimal("0"))
            side = "SELL" if amount > 0 else "BUY"
            quantity_abs = abs(amount)
            trade = self._latest_open_trade_for_symbol(symbol)
            if not trade:
                records.append({"symbol": symbol, "status": "skipped", "reason": "missing_open_trade_row"})
                continue
            try:
                exchange_info = self._exchange_symbol_info(exchange_symbol)
                quantity = self._floor_to_step(quantity_abs, exchange_info["step_size"])
                if quantity <= 0:
                    records.append({"symbol": symbol, "status": "skipped", "reason": "position_quantity_rounded_to_zero"})
                    continue
                open_algo_orders = self._open_algo_orders(exchange_symbol)
                created_ids: dict[str, str] = {}
                if not self._has_open_exit_order(open_algo_orders, side, {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}):
                    entry_price = self._decimal_from(position.get("entryPrice"), self._decimal_from(trade["entry_price"], Decimal("0")))
                    take_profit_price = self._hard_take_profit_price(
                        entry_price=entry_price,
                        quantity=quantity,
                        profit_usdt=self.config.hard_take_profit_usdt,
                        tick_size=exchange_info["tick_size"],
                    )
                    if take_profit_price > 0:
                        endpoint, params = self._exit_order_request({
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
                        response = self._signed_post_order_with_ioc_retry(endpoint, params, trading_symbol=exchange_symbol)
                        created_ids["take_profit_order_id"] = self._exit_order_id(response if isinstance(response, dict) else None)
                        self._update_latest_open_trade_exit_order(symbol, "take_profit_order_id", created_ids["take_profit_order_id"], now)
                        created += 1
                stop_loss_price = self._decimal_from(trade["stop_loss_price"], Decimal("0"))
                if stop_loss_price > 0 and not self._has_open_exit_order(open_algo_orders, side, {"STOP", "STOP_MARKET"}):
                    endpoint, params = self._exit_order_request({
                        "symbol": exchange_symbol,
                        "side": side,
                        "type": "STOP",
                        "quantity": self._fmt_decimal(quantity),
                        "price": self._fmt_decimal(stop_loss_price),
                        "stopPrice": self._fmt_decimal(stop_loss_price),
                        "timeInForce": "GTC",
                        "reduceOnly": "true",
                        "workingType": "MARK_PRICE",
                    })
                    response = self._signed_post_order_with_ioc_retry(endpoint, params, trading_symbol=exchange_symbol)
                    created_ids["stop_loss_order_id"] = self._exit_order_id(response if isinstance(response, dict) else None)
                    self._update_latest_open_trade_exit_order(symbol, "stop_loss_order_id", created_ids["stop_loss_order_id"], now)
                    created += 1
                records.append({"symbol": symbol, "status": "ok", "reason": str(created_ids or "already_protected")})
            except Exception as exc:
                errors += 1
                candidate = OpenableSymbol(symbol=symbol, decision_round_ts=None, total_score=None, qualified=True, reason="exit_order_reconcile")
                self._record_error(candidate, f"reconcile_exit_orders:{exchange_symbol}", exc)
                records.append({"symbol": symbol, "status": "failed", "reason": f"{type(exc).__name__}: {exc}"})
        return {"checked": checked, "created": created, "errors": errors, "records": records}

    def _latest_open_trade_for_symbol(self, symbol: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                f"SELECT * FROM {self.TRADES_TABLE} WHERE symbol = ? AND status = 'opened' ORDER BY created_at DESC, id DESC LIMIT 1",
                (symbol,),
            ).fetchone()

    def _open_algo_orders(self, exchange_symbol: str) -> list[dict[str, Any]]:
        rows = self.account_manager._signed_get("/fapi/v1/openAlgoOrders", {"symbol": exchange_symbol})
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    @staticmethod
    def _has_open_exit_order(open_orders: list[dict[str, Any]], side: str, order_types: set[str]) -> bool:
        for order in open_orders:
            order_side = str(order.get("side", "")).upper()
            order_type = str(order.get("type") or order.get("orderType") or "").upper()
            status = str(order.get("status", "NEW")).upper()
            if order_side == side and order_type in order_types and status in {"NEW", "PENDING", "PARTIALLY_FILLED"}:
                return True
        return False

    def _update_latest_open_trade_exit_order(self, symbol: str, column: str, order_id: str, updated_at: int) -> None:
        if column not in {"take_profit_order_id", "stop_loss_order_id"} or not order_id:
            return
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE {self.TRADES_TABLE}
                SET {column} = ?, updated_at = ?
                WHERE id = (
                    SELECT id FROM {self.TRADES_TABLE}
                    WHERE symbol = ? AND status = 'opened'
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                )
                """,
                (order_id, updated_at, symbol),
            )

    def _wait_for_position_visibility(self, trading_symbol: str) -> None:
        delay_seconds = float(self.config.exit_order_missing_position_retry_delay_seconds)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        try:
            self._position_amount(trading_symbol)
        except Exception:
            return

    def _position_amount(self, trading_symbol: str) -> Decimal:
        row = self._position_risk_row(trading_symbol)
        return self._decimal_from(row.get("positionAmt"), Decimal("0")) if row else Decimal("0")

    def _position_risk_row(self, trading_symbol: str) -> dict[str, Any] | None:
        rows = self.account_manager._signed_get("/fapi/v3/positionRisk", {"symbol": trading_symbol})
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol", "")).upper() == trading_symbol.upper():
                return row
        return None

    @staticmethod
    def _is_missing_position_for_close_position_error(exc: Exception) -> bool:
        message = str(exc)
        return "-4509" in message or "positions are available" in message

    @staticmethod
    def _is_invalid_symbol_error(exc: Exception) -> bool:
        message = str(exc)
        return "-1121" in message or "Invalid symbol" in message

    @staticmethod
    def _is_invalid_leverage_error(exc: Exception) -> bool:
        message = str(exc)
        return "-4028" in message or ("not valid" in message and "Leverage" in message)

    @staticmethod
    def _exit_order_request(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        order_type = str(params.get("type", "")).upper()
        conditional_order_types = {
            "STOP",
            "STOP_MARKET",
            "TAKE_PROFIT",
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
        }
        if order_type not in conditional_order_types:
            return "/fapi/v1/order", dict(params)

        algo_params = dict(params)
        if "stopPrice" in algo_params:
            algo_params["triggerPrice"] = algo_params.pop("stopPrice")
        algo_params["algoType"] = "CONDITIONAL"
        return "/fapi/v1/algoOrder", algo_params

    @staticmethod
    def _exit_order_id(order: dict[str, Any] | None) -> str:
        if not order:
            return ""
        return str(order.get("algoId") or order.get("orderId") or "")

    def _fetch_and_store_positions(self) -> list[dict[str, Any]]:
        rows = self.account_manager._signed_get("/fapi/v3/positionRisk")
        positions = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        active_positions = [
            row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0
        ]
        fallback_leverages = self._latest_opened_trade_leverages()
        now = int(time.time() * 1000)
        with self._connect() as conn:
            conn.execute(f"DELETE FROM {self.POSITIONS_TABLE}")
            conn.executemany(
                f"""
                INSERT INTO {self.POSITIONS_TABLE}
                (symbol, position_amt, entry_price, mark_price, unrealized_pnl, leverage, notional, liquidation_price, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        self._base_symbol(row.get("symbol", "")),
                        str(row.get("positionAmt", "0")),
                        str(row.get("entryPrice", "0")),
                        str(row.get("markPrice", "0")),
                        str(row.get("unRealizedProfit", row.get("unrealizedProfit", "0"))),
                        self._position_leverage(row, fallback_leverages),
                        str(row.get("notional", "0")),
                        str(row.get("liquidationPrice", "0")),
                        now,
                    )
                    for row in active_positions
                ],
            )
        return positions

    def _latest_opened_trade_leverages(self) -> dict[str, str]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, leverage
                FROM {self.TRADES_TABLE}
                WHERE status = 'opened' AND leverage IS NOT NULL
                ORDER BY created_at DESC, id DESC
                """
            ).fetchall()
        leverages: dict[str, str] = {}
        for row in rows:
            symbol = self._base_symbol(row["symbol"])
            if symbol not in leverages and row["leverage"] is not None:
                leverages[symbol] = str(row["leverage"])
        return leverages

    @staticmethod
    def _position_leverage(row: dict[str, Any], fallback_leverages: dict[str, str]) -> str:
        raw_leverage = str(row.get("leverage", "")).strip()
        if raw_leverage:
            return raw_leverage
        return fallback_leverages.get(TradingExperiment._base_symbol(row.get("symbol", "")), "-")

    def _has_open_position(self, trading_symbol: str, cached_positions: Iterable[dict[str, Any]]) -> bool:
        """Return true when the candidate symbol already has an active position."""
        normalized_symbol = trading_symbol.upper()
        for row in cached_positions:
            if str(row.get("symbol", "")).upper() != normalized_symbol:
                continue
            if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0:
                return True
        return False

    def _record_error(self, candidate: OpenableSymbol, operation: str, exc: Exception) -> None:
        now = int(time.time() * 1000)
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.ERRORS_TABLE}
                (symbol, decision_round_ts, total_score, leverage, operation, error_type, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.symbol,
                    candidate.decision_round_ts,
                    candidate.total_score,
                    self._parse_leverage(candidate.opening_leverage),
                    operation,
                    type(exc).__name__,
                    str(exc),
                    now,
                ),
            )

    def _record_skip(
        self,
        candidate: OpenableSymbol,
        account_equity: Decimal,
        max_loss: Decimal,
        reason: str,
        required_margin: Decimal | None = None,
    ) -> None:
        self._insert_trade(
            candidate=candidate,
            side="LONG",
            status="skipped",
            leverage=self._parse_leverage(candidate.opening_leverage),
            account_equity=account_equity,
            max_loss=max_loss,
            entry_price=Decimal("0"),
            quantity=Decimal("0"),
            notional=Decimal("0"),
            required_margin=required_margin if required_margin is not None else self._trade_plan(candidate, account_equity).required_margin_usdt,
            allocated_notional=self._trade_plan(candidate, account_equity).planned_notional_usdt,
            take_profit_price=Decimal("0"),
            stop_loss_price=Decimal("0"),
            stop_loss_calculation="",
            take_profit_order_id="",
            stop_loss_order_id="",
            reason=reason,
            raw_response="",
            now=int(time.time() * 1000),
        )

    def _insert_trade(
        self,
        candidate: OpenableSymbol,
        side: str,
        status: str,
        leverage: int,
        account_equity: Decimal,
        max_loss: Decimal,
        entry_price: Decimal,
        quantity: Decimal,
        notional: Decimal,
        required_margin: Decimal,
        allocated_notional: Decimal,
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
        stop_loss_calculation: str,
        take_profit_order_id: str,
        stop_loss_order_id: str,
        reason: str,
        raw_response: str,
        now: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.TRADES_TABLE}
                (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                 required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                 notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
                 take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.symbol,
                    candidate.decision_round_ts,
                    side,
                    status,
                    candidate.total_score,
                    leverage,
                    self._fmt_decimal(allocated_notional),
                    self._fmt_decimal(required_margin),
                    self._fmt_decimal(account_equity),
                    self._fmt_decimal(max_loss),
                    self._fmt_decimal(entry_price),
                    self._fmt_decimal(quantity),
                    self._fmt_decimal(notional),
                    self._fmt_decimal(take_profit_price),
                    self._fmt_decimal(stop_loss_price),
                    stop_loss_calculation,
                    take_profit_order_id,
                    stop_loss_order_id,
                    reason,
                    raw_response,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _trade_from_row(row: sqlite3.Row) -> ExperimentTradeRecord:
        return ExperimentTradeRecord(
            id=int(row["id"]),
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]) if row["decision_round_ts"] is not None else None,
            side=str(row["side"]),
            status=str(row["status"]),
            total_score=int(row["total_score"]) if row["total_score"] is not None else None,
            leverage=int(row["leverage"]) if row["leverage"] is not None else None,
            allocated_notional_usdt=str(row["allocated_usdt"]),
            required_margin_usdt=str(row["required_margin_usdt"]),
            account_equity_usdt=str(row["account_equity_usdt"]),
            max_loss_usdt=str(row["max_loss_usdt"]),
            entry_price=str(row["entry_price"]),
            quantity=str(row["quantity"]),
            notional_usdt=str(row["notional_usdt"]),
            take_profit_price=str(row["take_profit_price"]),
            stop_loss_price=str(row["stop_loss_price"]),
            stop_loss_calculation=str(row["stop_loss_calculation"] if "stop_loss_calculation" in row.keys() else ""),
            take_profit_order_id=str(row["take_profit_order_id"]),
            stop_loss_order_id=str(row["stop_loss_order_id"]),
            reason=str(row["reason"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    @staticmethod
    def _position_from_row(row: sqlite3.Row) -> ExperimentPositionSnapshot:
        opened_at = int(row["opened_at"]) if "opened_at" in row.keys() and row["opened_at"] is not None else None
        updated_at = int(row["updated_at"])
        holding_hours = "-"
        if opened_at is not None:
            elapsed_ms = max(0, int(time.time() * 1000) - opened_at)
            holding_hours = f"{elapsed_ms / 3_600_000:.2f}"
        return ExperimentPositionSnapshot(
            id=int(row["id"]),
            symbol=str(row["symbol"]),
            position_amt=str(row["position_amt"]),
            entry_price=str(row["entry_price"]),
            mark_price=str(row["mark_price"]),
            unrealized_pnl=str(row["unrealized_pnl"]),
            leverage=str(row["leverage"]),
            notional=str(row["notional"]),
            liquidation_price=str(row["liquidation_price"]),
            opened_at=opened_at,
            holding_hours=holding_hours,
            updated_at=updated_at,
        )

    @staticmethod
    def _error_from_row(row: sqlite3.Row) -> ExperimentErrorRecord:
        return ExperimentErrorRecord(
            id=int(row["id"]),
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]) if row["decision_round_ts"] is not None else None,
            total_score=int(row["total_score"]) if row["total_score"] is not None else None,
            leverage=int(row["leverage"]) if row["leverage"] is not None else None,
            operation=str(row["operation"]),
            error_type=str(row["error_type"]),
            error_message=str(row["error_message"]),
            created_at=int(row["created_at"]),
        )

    def _fetch_account(self) -> dict[str, Any]:
        account = self.account_manager._signed_get("/fapi/v3/account")
        if not isinstance(account, dict):
            raise RuntimeError("Unexpected Binance account response format")
        return account

    def _fetch_experiment_usdt_equity(self) -> Decimal:
        rows = self.account_manager._signed_get("/fapi/v3/balance")
        if not isinstance(rows, list):
            raise RuntimeError("Unexpected Binance balance response format")
        for row in rows:
            if not isinstance(row, dict) or str(row.get("asset", "")).upper() != "USDT":
                continue
            balance = self._decimal_from(row.get("balance"), Decimal("0"))
            equity = balance - self.config.experiment_uninvested_usdt
            return equity if equity > 0 else Decimal("0")
        return Decimal("0")

    def _account_equity(self, account: dict[str, Any]) -> Decimal:
        for key in ("totalMarginBalance", "totalWalletBalance"):
            value = self._decimal_from(account.get(key), Decimal("0"))
            if value > 0:
                return value
        return self.config.initial_equity_usdt

    def _trade_plan(self, candidate: OpenableSymbol, account_equity: Decimal) -> TradePlan:
        leverage = self._effective_leverage(candidate)
        distance_ratio = self._effective_stop_loss_distance_ratio(candidate)
        if leverage <= 0 or distance_ratio <= 0 or account_equity <= 0:
            return TradePlan(leverage, distance_ratio, Decimal("0"), Decimal("0"))
        required_margin = (account_equity * self.config.risk_fraction) / (distance_ratio * Decimal(leverage))
        planned_notional = required_margin * Decimal(leverage)
        return TradePlan(leverage, distance_ratio, required_margin, planned_notional)

    def _effective_leverage(self, candidate: OpenableSymbol) -> int:
        return self._parse_leverage(candidate.opening_leverage)

    def _effective_stop_loss_distance_ratio(self, candidate: OpenableSymbol) -> Decimal:
        return self._candidate_distance_ratio(candidate)

    @classmethod
    def _candidate_distance_ratio(cls, candidate: OpenableSymbol) -> Decimal:
        return cls._decimal_from(candidate.stop_loss_distance_ratio, Decimal("0"))

    def _reserved_margin_from_positions(self, positions: Iterable[dict[str, Any]]) -> Decimal:
        reserved = Decimal("0")
        for row in positions:
            amt = self._decimal_from(row.get("positionAmt"), Decimal("0"))
            if amt == 0:
                continue
            leverage = self._decimal_from(row.get("leverage"), Decimal("0"))
            notional = abs(self._decimal_from(row.get("notional"), Decimal("0")))
            if leverage > 0 and notional > 0:
                reserved += notional / leverage
        return reserved

    def _current_total_risk(
        self,
        positions: list[dict[str, Any]],
        decision_round_ts: int | None = None,
    ) -> Decimal:
        from holding_position_scoring import HoldingPositionScoringSystem

        summary = HoldingPositionScoringSystem(
            db_path=self.db_path,
            account_manager=self.account_manager,
        ).calculate_portfolio_risk(
            positions=positions,
            decision_round_ts=decision_round_ts,
        )
        return self._decimal_from(summary.total_risk, Decimal("0"))

    @staticmethod
    def _base_symbol(symbol: Any) -> str:
        normalized = str(symbol).strip().upper()
        if normalized.endswith("USDT"):
            return normalized[:-4]
        return normalized

    @staticmethod
    def _binance_symbol(symbol: str) -> str:
        """Return the Binance USDⓈ-M Futures trading pair symbol for a stored base symbol."""
        normalized = str(symbol).strip().upper()
        if not normalized:
            return normalized
        return normalized if normalized.endswith("USDT") else f"{normalized}USDT"

    def _exchange_symbol_info(self, symbol: str) -> dict[str, Decimal]:
        info = self.account_manager._public_get("/fapi/v1/exchangeInfo")
        for row in info.get("symbols", []):
            if row.get("symbol") != symbol:
                continue
            filters = {item.get("filterType"): item for item in row.get("filters", [])}
            lot = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            return {
                "step_size": Decimal(str(lot.get("stepSize", "1"))),
                "tick_size": Decimal(str(price_filter.get("tickSize", "0.01"))),
            }
        raise RuntimeError(f"Symbol {symbol} not found in exchangeInfo")

    def _latest_price(self, symbol: str) -> Decimal:
        payload = self.account_manager._public_get("/fapi/v1/ticker/price", {"symbol": symbol})
        price = self._decimal_from(payload.get("price") if isinstance(payload, dict) else None, Decimal("0"))
        if price > 0:
            return price

        mark_payload = self.account_manager._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        mark_price = self._decimal_from(
            mark_payload.get("markPrice") if isinstance(mark_payload, dict) else None,
            Decimal("0"),
        )
        if mark_price > 0:
            return mark_price

        raise RuntimeError(f"Invalid latest price for {symbol}: ticker={payload}, premiumIndex={mark_payload}")

    @staticmethod
    def _filled_entry_price(market_order: dict[str, Any], fallback: Decimal) -> Decimal:
        for key in ("avgPrice", "price"):
            value = TradingExperiment._decimal_from(market_order.get(key), Decimal("0"))
            if value > 0:
                return value
        executed_qty = TradingExperiment._decimal_from(market_order.get("executedQty"), Decimal("0"))
        cumulative_quote = TradingExperiment._decimal_from(market_order.get("cumQuote"), Decimal("0"))
        if executed_qty > 0 and cumulative_quote > 0:
            return cumulative_quote / executed_qty
        return fallback

    def _latest_mark_price(self, symbol: str, fallback: Decimal) -> Decimal:
        try:
            payload = self.account_manager._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        except Exception:
            return fallback
        price = self._decimal_from(payload.get("markPrice"), Decimal("0"))
        return price if price > 0 else fallback


    def _hard_take_profit_price(
        self,
        entry_price: Decimal,
        quantity: Decimal,
        profit_usdt: Decimal,
        tick_size: Decimal,
    ) -> Decimal:
        if entry_price <= 0 or quantity <= 0 or profit_usdt <= 0:
            return Decimal("0")
        return self._ceil_to_tick(entry_price + (profit_usdt / quantity), tick_size)

    def _risk_capped_stop_loss_price(
        self,
        entry_price: Decimal,
        quantity: Decimal,
        max_loss: Decimal,
        trigger_reference_price: Decimal,
        tick_size: Decimal,
    ) -> Decimal:
        stop_loss_price, _ = self._risk_capped_stop_loss_price_with_detail(
            entry_price=entry_price,
            quantity=quantity,
            max_loss=max_loss,
            trigger_reference_price=trigger_reference_price,
            tick_size=tick_size,
        )
        return stop_loss_price

    def _risk_capped_stop_loss_price_with_detail(
        self,
        entry_price: Decimal,
        quantity: Decimal,
        max_loss: Decimal,
        trigger_reference_price: Decimal,
        tick_size: Decimal,
    ) -> tuple[Decimal, str]:
        ten_pct_price_distance = entry_price * self.config.max_stop_loss_pct
        risk_price_distance = max_loss / quantity if quantity > 0 else ten_pct_price_distance
        stop_loss_price_distance = min(ten_pct_price_distance, risk_price_distance)
        selected_cap = "risk_loss_cap" if risk_price_distance <= ten_pct_price_distance else "max_stop_loss_pct_cap"
        desired_price = entry_price - stop_loss_price_distance
        maximum_trigger_price = trigger_reference_price - tick_size
        raw_stop_price = min(desired_price, maximum_trigger_price) if maximum_trigger_price > 0 else desired_price
        stop_loss_price = self._valid_stop_loss_price(
            desired_price=desired_price,
            trigger_reference_price=trigger_reference_price,
            tick_size=tick_size,
        )
        detail = (
            f"max_loss={self._fmt_decimal(max_loss)}; "
            f"entry_price={self._fmt_decimal(entry_price)}; "
            f"quantity={self._fmt_decimal(quantity)}; "
            f"max_stop_loss_pct={self._fmt_decimal(self.config.max_stop_loss_pct)}; "
            f"ten_pct_distance=entry_price*max_stop_loss_pct={self._fmt_decimal(ten_pct_price_distance)}; "
            f"risk_distance=max_loss/quantity={self._fmt_decimal(risk_price_distance)}; "
            f"selected_distance=min(ten_pct_distance,risk_distance)={self._fmt_decimal(stop_loss_price_distance)} ({selected_cap}); "
            f"desired_price=entry_price-selected_distance={self._fmt_decimal(desired_price)}; "
            f"trigger_reference_price={self._fmt_decimal(trigger_reference_price)}; "
            f"tick_size={self._fmt_decimal(tick_size)}; "
            f"maximum_trigger_price=trigger_reference_price-tick_size={self._fmt_decimal(maximum_trigger_price)}; "
            f"raw_stop_price=min(desired_price,maximum_trigger_price)={self._fmt_decimal(raw_stop_price)}; "
            f"final_stop_loss_price=floor_to_tick(raw_stop_price)={self._fmt_decimal(stop_loss_price)}"
        )
        return stop_loss_price, detail

    def _valid_stop_loss_price(
        self,
        desired_price: Decimal,
        trigger_reference_price: Decimal,
        tick_size: Decimal,
    ) -> Decimal:
        maximum_trigger_price = trigger_reference_price - tick_size
        raw_stop_price = min(desired_price, maximum_trigger_price) if maximum_trigger_price > 0 else desired_price
        stop_price = self._floor_to_tick(raw_stop_price, tick_size)
        return stop_price if stop_price > 0 else tick_size

    @staticmethod
    def _parse_leverage(leverage: str) -> int:
        text = str(leverage).strip().lower().replace("x", "")
        if not text.isdigit():
            return 0
        return int(text)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    @staticmethod
    def _floor_to_tick(value: Decimal, tick: Decimal) -> Decimal:
        return TradingExperiment._floor_to_step(value, tick)

    @staticmethod
    def _ceil_to_tick(value: Decimal, tick: Decimal) -> Decimal:
        if tick <= 0:
            return value
        return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick

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


if __name__ == "__main__":
    # Standalone 15-minute scans must not use stale openable results from a
    # previous round. They are allowed to trade only after the current round's
    # openable-symbol evaluation has already persisted qualified candidates.
    experiment = TradingExperiment()
    result = experiment.run_latest_round(
        decision_round_ts=experiment.current_decision_round_ts(),
    )
    print(result)
