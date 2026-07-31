import sqlite3

import db_config
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
