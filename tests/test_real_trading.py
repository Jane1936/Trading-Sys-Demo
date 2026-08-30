from decimal import Decimal

import db_config
import real_trading
from binance_account_manager import BinanceAccountManager


def test_real_experiment_isolated_from_simulation_database(tmp_path, monkeypatch):
    real_db = tmp_path / "real_trading_core.db"
    simulation_db = tmp_path / "trading.db"
    scoring_db = tmp_path / "scoring.db"
    monkeypatch.setattr(db_config, "REAL_TRADING_CORE_DB_PATH", str(real_db))
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", str(simulation_db))
    monkeypatch.setattr(db_config, "SCORING_DB_PATH", str(scoring_db))
    monkeypatch.setenv("BINANCE_REAL_API_KEY", "live-key")
    monkeypatch.setenv("BINANCE_REAL_API_SECRET", "live-secret")

    live = real_trading.experiment()
    live.init_tables()

    assert live.db_path == str(real_db)
    assert live.core_db_path == str(real_db)
    assert live.openable_db_path == str(scoring_db)
    assert live.account_manager.testnet is False
    assert real_db.exists()
    assert not simulation_db.exists()


def test_real_config_uses_100u_without_simulation_reserve_and_10u_take_profit(monkeypatch):
    monkeypatch.delenv("REAL_TRADING_INITIAL_EQUITY_USDT", raising=False)

    value = real_trading.config()

    assert value.initial_equity_usdt == Decimal("100")
    assert value.total_margin_budget_usdt == Decimal("100")
    assert value.experiment_uninvested_usdt == Decimal("0")
    assert value.hard_take_profit_usdt == Decimal("10")


def test_real_holding_scoring_uses_live_api_and_real_database(tmp_path, monkeypatch):
    real_db = tmp_path / "real_trading_core.db"
    simulation_db = tmp_path / "trading.db"
    monkeypatch.setattr(db_config, "REAL_TRADING_CORE_DB_PATH", str(real_db))
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", str(simulation_db))
    monkeypatch.setenv("BINANCE_REAL_API_KEY", "live-key")
    monkeypatch.setenv("BINANCE_REAL_API_SECRET", "live-secret")

    scoring = real_trading.holding_scoring()
    scoring.init_tables()

    assert scoring.db_path == str(real_db)
    assert scoring.info_db_path == str(real_db)
    assert scoring.account_manager.testnet is False
    assert real_db.exists()
    assert not simulation_db.exists()


def test_real_high_frequency_modules_use_live_api_and_split_databases(tmp_path, monkeypatch):
    live_db = tmp_path / "real_trading.db"
    live_core = tmp_path / "real_trading_core.db"
    live_info = tmp_path / "real_trading_info.db"
    monkeypatch.setattr(db_config, "REAL_TRADING_DB_PATH", str(live_db))
    monkeypatch.setattr(db_config, "REAL_TRADING_CORE_DB_PATH", str(live_core))
    monkeypatch.setattr(db_config, "REAL_TRADING_INFO_DB_PATH", str(live_info))
    monkeypatch.setenv("BINANCE_REAL_API_KEY", "live-key")
    monkeypatch.setenv("BINANCE_REAL_API_SECRET", "live-secret")

    modules = real_trading.high_frequency_modules()

    assert len(modules) == 5
    assert all(module.db_path == str(live_db) for module in modules)
    assert all(module.info_db_path == str(live_info) for module in modules)
    assert all(module.account_manager.testnet is False for module in modules)
