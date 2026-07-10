"""15-minute trailing reduction pre-trigger tracker.

After the normal position-reduction module runs for a closed 15m candle, this
module scans active positions. If a position's unrealized profit is greater than
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
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.CHECKS_TABLE}_checked ON {self.CHECKS_TABLE}(checked_at DESC, symbol ASC)")

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
        eligible = amount != 0 and entry_price > 0 and trigger_r > 0 and unrealized_pnl > trigger_r
        pretriggered = False
        tag = ""
        reason = "unrealized_pnl_lte_2r"
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

    @staticmethod
    def _check_from_row(row: sqlite3.Row) -> TrailingReductionCheck:
        return TrailingReductionCheck(**{**dict(row), "eligible": bool(row["eligible"]), "pretriggered": bool(row["pretriggered"])})

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
