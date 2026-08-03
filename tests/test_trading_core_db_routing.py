import sqlite3

import db_config
from break_even_take_profit import BreakEvenTakeProfitStrategy
from holding_position_scoring import HoldingPositionScoringSystem
from trading_experiment import TradingExperiment
from zombie_force_liquidation import ZombieForceLiquidationModule


def _tables(path):
    with sqlite3.connect(path) as conn:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }


def test_migrated_tables_are_initialized_in_trading_core_db(monkeypatch, tmp_path):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)

    TradingExperiment(db_path=trading_db).init_tables()
    ZombieForceLiquidationModule(db_path=trading_db).init_tables()

    assert {
        TradingExperiment.TRADES_TABLE,
        TradingExperiment.POSITIONS_TABLE,
        ZombieForceLiquidationModule.CHECKS_TABLE,
        ZombieForceLiquidationModule.RECORDS_TABLE,
    } <= _tables(core_db)
    assert TradingExperiment.ERRORS_TABLE in _tables(trading_db)
    assert not {
        TradingExperiment.TRADES_TABLE,
        TradingExperiment.POSITIONS_TABLE,
        ZombieForceLiquidationModule.CHECKS_TABLE,
        ZombieForceLiquidationModule.RECORDS_TABLE,
    } & _tables(trading_db)


def test_custom_database_path_keeps_all_tables_together(tmp_path):
    custom_db = str(tmp_path / "test.db")

    TradingExperiment(db_path=custom_db).init_tables()
    ZombieForceLiquidationModule(db_path=custom_db).init_tables()

    assert {
        TradingExperiment.TRADES_TABLE,
        TradingExperiment.POSITIONS_TABLE,
        TradingExperiment.ERRORS_TABLE,
        ZombieForceLiquidationModule.CHECKS_TABLE,
        ZombieForceLiquidationModule.RECORDS_TABLE,
    } <= _tables(custom_db)


def test_break_even_reads_latest_stop_loss_from_trading_core_db(monkeypatch, tmp_path):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)

    experiment = TradingExperiment(db_path=trading_db)
    experiment.init_tables()
    with experiment._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, side, status, allocated_usdt, account_equity_usdt,
             max_loss_usdt, entry_price, quantity, notional_usdt,
             take_profit_price, stop_loss_price, stop_loss_order_id, reason,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BANK", "LONG", "opened", "20", "1100", "11", "10", "2",
                "20", "12", "9", "core-stop-123", "test", 1, 1,
            ),
        )

    strategy = BreakEvenTakeProfitStrategy(db_path=trading_db)

    assert strategy._latest_stop_loss_order_id("BANK") == "core-stop-123"


def test_holding_risk_tables_are_initialized_in_trading_core_db(monkeypatch, tmp_path):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)

    scoring = HoldingPositionScoringSystem(db_path=trading_db)
    scoring.init_tables()

    migrated_tables = {
        scoring.CHECKS_TABLE,
        scoring.RECORDS_TABLE,
        scoring.PORTFOLIO_RISK_TABLE,
        scoring.PORTFOLIO_RISK_SUMMARY_TABLE,
        scoring.REDUCTION_CHECKS_TABLE,
        scoring.REDUCTION_RECORDS_TABLE,
    }
    assert migrated_tables <= _tables(core_db)
    assert not migrated_tables & _tables(trading_db)
    assert {
        scoring.INCREASE_CHECKS_TABLE,
        scoring.INCREASE_RECORDS_TABLE,
    } <= _tables(trading_db)


def test_custom_holding_database_path_keeps_all_tables_together(tmp_path):
    custom_db = str(tmp_path / "test.db")
    scoring = HoldingPositionScoringSystem(db_path=custom_db)

    scoring.init_tables()

    assert {
        scoring.CHECKS_TABLE,
        scoring.RECORDS_TABLE,
        scoring.PORTFOLIO_RISK_TABLE,
        scoring.PORTFOLIO_RISK_SUMMARY_TABLE,
        scoring.REDUCTION_CHECKS_TABLE,
        scoring.REDUCTION_RECORDS_TABLE,
        scoring.INCREASE_CHECKS_TABLE,
        scoring.INCREASE_RECORDS_TABLE,
    } <= _tables(custom_db)
