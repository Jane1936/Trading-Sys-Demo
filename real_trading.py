"""Production-account entry and zombie-liquidation wiring.

Only scoring decisions are shared with the simulator.  Every order, snapshot,
audit row, lock and error produced here is stored in real_trading_core.db.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Iterable

import db_config
from binance_account_manager import BinanceAccountManager
from openable_symbol_module import OpenableSymbol
from trading_experiment import ExperimentConfig, TradingExperiment
from zombie_force_liquidation import ZombieForceLiquidationModule


def config() -> ExperimentConfig:
    initial = Decimal(os.getenv("REAL_TRADING_INITIAL_EQUITY_USDT", "100"))
    return ExperimentConfig(
        initial_equity_usdt=initial,
        experiment_uninvested_usdt=Decimal("0"),
        total_margin_budget_usdt=initial,
    )


def experiment() -> TradingExperiment:
    return TradingExperiment(
        db_path=db_config.REAL_TRADING_CORE_DB_PATH,
        openable_db_path=db_config.SCORING_DB_PATH,
        account_manager=BinanceAccountManager.live(),
        config=config(),
    )


def zombie_module() -> ZombieForceLiquidationModule:
    return ZombieForceLiquidationModule(
        db_path=db_config.REAL_TRADING_CORE_DB_PATH,
        account_manager=BinanceAccountManager.live(),
        config=config(),
    )


def initialize() -> None:
    experiment().init_tables()
    zombie_module().init_tables()


def run_round(candidates: Iterable[OpenableSymbol], round_ts: int) -> dict[str, Any]:
    """Run mandatory stale-position cleanup before considering new entries."""
    zombie_result = zombie_module().run_round(checked_at=round_ts)
    rows = list(candidates)
    if not any(row.qualified for row in rows):
        return {"opened": 0, "skipped": 0, "reason": "no_qualified_symbols", "zombie_force_liquidation": zombie_result}
    result = experiment().run_round(rows)
    result["zombie_force_liquidation"] = zombie_result
    return result
