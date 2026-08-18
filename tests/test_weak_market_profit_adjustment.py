import sqlite3

import allusdt_15m_ma20
import db_config
from weak_market_profit_adjustment import (
    WeakMarketProfitAdjustmentModule, get_settings, set_settings,
)


def _sources(path, *, close=90, ma20=100, close_time=899_999):
    with sqlite3.connect(path) as conn:
        allusdt_15m_ma20.init_db(conn)
        conn.execute(f"INSERT INTO {allusdt_15m_ma20.KLINE_TABLE} (open_time, open, high, low, close, volume, close_time) VALUES (0, ?, ?, ?, ?, 1, ?)", (close, close, close, close, close_time))
        conn.execute(f"INSERT INTO {allusdt_15m_ma20.H1_MA20_TABLE} (open_time, close_time, close, ma20, updated_at) VALUES (0, ?, ?, ?, 1)", (close_time, close, ma20))


def test_weak_market_uses_1_4r_and_fifty_percent(tmp_path, monkeypatch):
    path = str(tmp_path / "market.db")
    _sources(path)
    monkeypatch.setattr(db_config, "BASE_DB_PATH", path)
    module = WeakMarketProfitAdjustmentModule(path)
    result = module.run_round(decision_round_ts=900_000, evaluated_at=900_001)
    assert result.weak_market is True
    assert result.trigger_r_multiple == 1.4
    assert result.take_profit_fraction == 0.5
    assert module.recent_results(days=7, now_ms=900_001) == [result]


def test_normal_market_keeps_2r_and_thirty_percent(tmp_path, monkeypatch):
    path = str(tmp_path / "market.db")
    _sources(path, close=101, ma20=100)
    monkeypatch.setattr(db_config, "BASE_DB_PATH", path)
    result = WeakMarketProfitAdjustmentModule(path).run_round(decision_round_ts=900_000)
    assert result.weak_market is False
    assert result.trigger_r_multiple == 2.0
    assert result.take_profit_fraction == 0.3


def test_weak_market_uses_persisted_settings(tmp_path, monkeypatch):
    path = str(tmp_path / "market.db")
    _sources(path)
    monkeypatch.setattr(db_config, "BASE_DB_PATH", path)
    saved = set_settings({"trigger_r_multiple": 1.8, "take_profit_fraction": 0.65}, path)
    assert saved["trigger_r_multiple"] == 1.8
    assert get_settings(path)["take_profit_fraction"] == 0.65

    result = WeakMarketProfitAdjustmentModule(path).run_round(decision_round_ts=900_000)
    assert result.trigger_r_multiple == 1.8
    assert result.take_profit_fraction == 0.65


def test_weak_market_settings_reject_invalid_values(tmp_path):
    path = str(tmp_path / "base.db")
    for payload in (
        {"trigger_r_multiple": 0, "take_profit_fraction": 0.5},
        {"trigger_r_multiple": 1.4, "take_profit_fraction": 1.01},
        {"trigger_r_multiple": "nan", "take_profit_fraction": 0.5},
    ):
        try:
            set_settings(payload, path)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid settings should be rejected")


def test_hour_round_waits_for_hourly_ma20_convergence(tmp_path, monkeypatch):
    path = str(tmp_path / "market.db")
    _sources(path, close_time=899_999)
    monkeypatch.setattr(db_config, "BASE_DB_PATH", path)
    module = WeakMarketProfitAdjustmentModule(path)
    assert module.is_data_converged_for_round(3_600_000) == (False, "waiting_allusdt_15m_convergence")
    with sqlite3.connect(path) as conn:
        conn.execute(f"UPDATE {allusdt_15m_ma20.KLINE_TABLE} SET close_time = 3599999")
    assert module.is_data_converged_for_round(3_600_000) == (False, "waiting_allusdt_1h_ma20_convergence")
    with sqlite3.connect(path) as conn:
        conn.execute(f"UPDATE {allusdt_15m_ma20.H1_MA20_TABLE} SET close_time = 3599999")
    assert module.is_data_converged_for_round(3_600_000) == (True, "data_converged")
