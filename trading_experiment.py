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
    total_notional_budget_usdt: Decimal = Decimal("500")
    total_margin_budget_usdt: Decimal = Decimal("500")
    max_open_positions: int = 10
    per_trade_notional_usdt: Decimal = Decimal("50")
    per_trade_margin_budget_usdt: Decimal = Decimal("50")
    risk_fraction: Decimal = Decimal("0.01")
    take_profit_pct: Decimal = Decimal("0.20")
    max_stop_loss_pct: Decimal = Decimal("0.10")


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
    * experiment equity is capped at 1,000 USDT for risk calculation;
    * before opening, query current positions and skip if there are already 10;
    * the experiment reserves 500 USDT for total notional and 500 USDT for margin;
    * each candidate receives at most 50 USDT total notional;
    * each candidate reserves 50 USDT from the margin budget;
    * required margin is calculated as actual_notional / leverage;
    * stop-loss price distance is min(10%, experiment equity * 1% / notional);
    * take-profit is 20% above entry;
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

    def run_latest_round(self) -> dict[str, Any]:
        """Run one experiment pass against the latest openable-symbol round."""
        self.init_tables()
        candidates = self._latest_openable_candidates()
        if not candidates:
            return {"opened": 0, "skipped": 0, "reason": "no_qualified_openable_symbols"}
        return self.run_round(candidates)

    def run_round(self, candidates: Iterable[OpenableSymbol]) -> dict[str, Any]:
        """Attempt long entries for qualified candidates in score order."""
        self.account_manager.validate_config()
        self.init_tables()

        account = self.account_manager._signed_get("/fapi/v3/account")
        available_balance = self._decimal_from(account.get("availableBalance"), Decimal("0"))
        account_equity = self._account_equity(account)
        max_loss = account_equity * self.config.risk_fraction
        positions = self._fetch_and_store_positions()
        open_positions = self._open_position_symbols(positions)

        reserved_notional_budget = Decimal(len(open_positions)) * self.config.per_trade_notional_usdt
        reserved_margin_budget = Decimal(len(open_positions)) * self.config.per_trade_margin_budget_usdt

        opened = 0
        skipped = 0
        for candidate in sorted(candidates, key=lambda row: (-row.total_score, row.symbol)):
            if not candidate.qualified:
                continue
            if len(open_positions) >= self.config.max_open_positions:
                self._record_skip(candidate, account_equity, max_loss, "max_open_positions_reached")
                skipped += 1
                continue
            trading_symbol = self._binance_symbol(candidate.symbol)
            if trading_symbol in open_positions:
                self._record_skip(candidate, account_equity, max_loss, "symbol_position_already_open")
                skipped += 1
                continue
            required_margin = self._planned_required_margin(candidate)
            if reserved_notional_budget + self.config.per_trade_notional_usdt > self.config.total_notional_budget_usdt:
                self._record_skip(candidate, account_equity, max_loss, "total_notional_budget_exhausted")
                skipped += 1
                break
            if reserved_margin_budget + self.config.per_trade_margin_budget_usdt > self.config.total_margin_budget_usdt:
                self._record_skip(candidate, account_equity, max_loss, "total_margin_budget_exhausted")
                skipped += 1
                break
            if available_balance < self.config.per_trade_margin_budget_usdt:
                self._record_skip(candidate, account_equity, max_loss, "available_balance_lt_50usdt_margin_budget", required_margin)
                skipped += 1
                break

            try:
                result = self._open_long(candidate, account_equity, max_loss)
            except RuntimeError as exc:
                self._record_error(candidate, "open_long", exc)
                if "not found in exchangeInfo" in str(exc):
                    self._record_skip(candidate, account_equity, max_loss, "symbol_not_found_in_exchange_info")
                    skipped += 1
                    continue
                raise
            except Exception as exc:
                self._record_error(candidate, "open_long", exc)
                raise

            if result["status"] == "opened":
                opened += 1
                available_balance -= self.config.per_trade_margin_budget_usdt
                reserved_notional_budget += self.config.per_trade_notional_usdt
                reserved_margin_budget += self.config.per_trade_margin_budget_usdt
                open_positions.add(trading_symbol)
            else:
                skipped += 1

        self._fetch_and_store_positions()
        return {"opened": opened, "skipped": skipped, "reason": "completed"}

    def recent_trade_records(self, limit: int = 100) -> list[ExperimentTradeRecord]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {self.TRADES_TABLE}
                WHERE status = 'opened'
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (int(limit),),
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
                SELECT * FROM {self.POSITIONS_TABLE}
                WHERE updated_at = ?
                ORDER BY ABS(CAST(position_amt AS REAL)) DESC, symbol ASC
                LIMIT ?
                """,
                (latest_ts, int(limit)),
            ).fetchall()
        return [self._position_from_row(row) for row in rows]

    def recent_error_records(self, limit: int = 100) -> list[ExperimentErrorRecord]:
        self.init_tables()
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self.ERRORS_TABLE} ORDER BY created_at DESC, id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [self._error_from_row(row) for row in rows]

    def _latest_openable_candidates(self) -> list[OpenableSymbol]:
        module = OpenableSymbolModule(db_path=self.db_path)
        module.init_table()
        with module._connect() as conn:
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

    def _open_long(self, candidate: OpenableSymbol, account_equity: Decimal, max_loss: Decimal) -> dict[str, Any]:
        now = int(time.time() * 1000)
        leverage = self._parse_leverage(candidate.opening_leverage)
        if leverage <= 0:
            self._record_skip(candidate, account_equity, max_loss, "invalid_opening_leverage")
            return {"status": "skipped"}

        trading_symbol = self._binance_symbol(candidate.symbol)
        exchange_info = self._exchange_symbol_info(trading_symbol)
        pre_order_price = self._latest_price(trading_symbol)
        notional = self.config.per_trade_notional_usdt
        quantity = self._floor_to_step(notional / pre_order_price, exchange_info["step_size"])
        if quantity <= 0:
            self._record_skip(candidate, account_equity, max_loss, "quantity_rounded_to_zero")
            return {"status": "skipped"}

        self.account_manager._signed_post("/fapi/v1/leverage", {"symbol": trading_symbol, "leverage": leverage})
        market_order = self.account_manager._signed_post(
            "/fapi/v1/order",
            {
                "symbol": trading_symbol,
                "side": "BUY",
                "type": "MARKET",
                "quantity": self._fmt_decimal(quantity),
                "newOrderRespType": "RESULT",
            },
        )

        entry_price = self._filled_entry_price(market_order, pre_order_price)
        trigger_reference_price = self._latest_mark_price(trading_symbol, entry_price)
        notional = quantity * entry_price
        required_margin = notional / Decimal(leverage)
        stop_loss_pct = min(self.config.max_stop_loss_pct, max_loss / notional) if notional > 0 else self.config.max_stop_loss_pct
        take_profit_price = self._valid_take_profit_price(
            entry_price=entry_price,
            trigger_reference_price=trigger_reference_price,
            tick_size=exchange_info["tick_size"],
        )
        stop_loss_price = self._valid_stop_loss_price(
            entry_price=entry_price,
            stop_loss_pct=stop_loss_pct,
            trigger_reference_price=trigger_reference_price,
            tick_size=exchange_info["tick_size"],
        )

        sl_order = self._place_exit_order(
            candidate,
            trading_symbol,
            {
                "symbol": trading_symbol,
                "side": "SELL",
                "type": "STOP_MARKET",
                "stopPrice": self._fmt_decimal(stop_loss_price),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            },
            "place_stop_loss",
        )
        tp_order = self._place_exit_order(
            candidate,
            trading_symbol,
            {
                "symbol": trading_symbol,
                "side": "SELL",
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": self._fmt_decimal(take_profit_price),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            },
            "place_take_profit",
        )
        tp_order_id = str(tp_order.get("orderId", "")) if tp_order else ""
        sl_order_id = str(sl_order.get("orderId", "")) if sl_order else ""
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
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            take_profit_order_id=tp_order_id,
            stop_loss_order_id=sl_order_id,
            reason=f"market_order_id={market_order.get('orderId', '')}; " + "; ".join(exit_order_status),
            raw_response=str({"market": market_order, "tp": tp_order, "sl": sl_order}),
            now=now,
        )
        return {"status": "opened", "required_margin": self._fmt_decimal(required_margin)}

    def _place_exit_order(
        self,
        candidate: OpenableSymbol,
        trading_symbol: str,
        params: dict[str, Any],
        operation: str,
    ) -> dict[str, Any] | None:
        try:
            return self.account_manager._signed_post("/fapi/v1/order", params)
        except Exception as exc:
            self._record_error(candidate, f"{operation}:{trading_symbol}", exc)
            return None

    def _fetch_and_store_positions(self) -> list[dict[str, Any]]:
        rows = self.account_manager._signed_get("/fapi/v3/positionRisk")
        positions = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        active_positions = [
            row for row in positions if self._decimal_from(row.get("positionAmt"), Decimal("0")) != 0
        ]
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
                        str(row.get("symbol", "")),
                        str(row.get("positionAmt", "0")),
                        str(row.get("entryPrice", "0")),
                        str(row.get("markPrice", "0")),
                        str(row.get("unRealizedProfit", row.get("unrealizedProfit", "0"))),
                        str(row.get("leverage", "")),
                        str(row.get("notional", "0")),
                        str(row.get("liquidationPrice", "0")),
                        now,
                    )
                    for row in active_positions
                ],
            )
        return positions

    @staticmethod
    def _open_position_symbols(positions: Iterable[dict[str, Any]]) -> set[str]:
        symbols: set[str] = set()
        for row in positions:
            amt = TradingExperiment._decimal_from(row.get("positionAmt"), Decimal("0"))
            if amt != 0:
                symbols.add(str(row.get("symbol", "")))
        return symbols

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
            required_margin=required_margin if required_margin is not None else self._planned_required_margin(candidate),
            take_profit_price=Decimal("0"),
            stop_loss_price=Decimal("0"),
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
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
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
                 notional_usdt, take_profit_price, stop_loss_price, take_profit_order_id,
                 stop_loss_order_id, reason, raw_response, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.symbol,
                    candidate.decision_round_ts,
                    side,
                    status,
                    candidate.total_score,
                    leverage,
                    self._fmt_decimal(self.config.per_trade_notional_usdt),
                    self._fmt_decimal(required_margin),
                    self._fmt_decimal(account_equity),
                    self._fmt_decimal(max_loss),
                    self._fmt_decimal(entry_price),
                    self._fmt_decimal(quantity),
                    self._fmt_decimal(notional),
                    self._fmt_decimal(take_profit_price),
                    self._fmt_decimal(stop_loss_price),
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
            take_profit_order_id=str(row["take_profit_order_id"]),
            stop_loss_order_id=str(row["stop_loss_order_id"]),
            reason=str(row["reason"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    @staticmethod
    def _position_from_row(row: sqlite3.Row) -> ExperimentPositionSnapshot:
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
            updated_at=int(row["updated_at"]),
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

    def _account_equity(self, account: dict[str, Any]) -> Decimal:
        for key in ("totalMarginBalance", "totalWalletBalance"):
            value = self._decimal_from(account.get(key), Decimal("0"))
            if value > 0:
                return min(value, self.config.initial_equity_usdt)
        return self.config.initial_equity_usdt

    def _planned_required_margin(self, candidate: OpenableSymbol) -> Decimal:
        leverage = self._parse_leverage(candidate.opening_leverage)
        if leverage <= 0:
            return Decimal("0")
        return self.config.per_trade_notional_usdt / Decimal(leverage)


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
        price = self._decimal_from(payload.get("price"), Decimal("0"))
        if price <= 0:
            raise RuntimeError(f"Invalid latest price for {symbol}: {payload}")
        return price

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

    def _valid_take_profit_price(
        self,
        entry_price: Decimal,
        trigger_reference_price: Decimal,
        tick_size: Decimal,
    ) -> Decimal:
        desired_price = entry_price * (Decimal("1") + self.config.take_profit_pct)
        minimum_trigger_price = trigger_reference_price + tick_size
        return self._ceil_to_tick(max(desired_price, minimum_trigger_price), tick_size)

    def _valid_stop_loss_price(
        self,
        entry_price: Decimal,
        stop_loss_pct: Decimal,
        trigger_reference_price: Decimal,
        tick_size: Decimal,
    ) -> Decimal:
        desired_price = entry_price * (Decimal("1") - stop_loss_pct)
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
    result = TradingExperiment().run_latest_round()
    print(result)
