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


def test_real_config_uses_100u_without_simulation_reserve(monkeypatch):
    monkeypatch.delenv("REAL_TRADING_INITIAL_EQUITY_USDT", raising=False)

    value = real_trading.config()

    assert value.initial_equity_usdt == Decimal("100")
    assert value.total_margin_budget_usdt == Decimal("100")
    assert value.experiment_uninvested_usdt == Decimal("0")


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
