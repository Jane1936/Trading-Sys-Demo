import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import feature_flags


def test_feature_flags_seed_enabled_by_default(tmp_path):
    db_path = str(tmp_path / "base_data.db")

    flags = feature_flags.list_feature_flags(db_path)

    assert [flag.key for flag in flags] == [
        feature_flags.BASE_DATA_COLLECTION,
        feature_flags.SCORING_SYSTEM,
        feature_flags.TRADING_SYSTEM,
        feature_flags.REAL_TRADING_SYSTEM,
        feature_flags.MARKET_FILTER,
        feature_flags.STOP_LOSS_RULE,
        feature_flags.REDUCTION_CONDITIONS,
        feature_flags.INCREASE_CONDITIONS,
        feature_flags.PORTFOLIO_RISK,
        feature_flags.REAL_STOP_LOSS_RULE,
        feature_flags.REAL_REDUCTION_CONDITIONS,
        feature_flags.REAL_INCREASE_CONDITIONS,
        feature_flags.REAL_PORTFOLIO_RISK,
        feature_flags.BREAK_EVEN_TAKE_PROFIT,
        feature_flags.PARTIAL_TAKE_PROFIT,
        feature_flags.TRAILING_REDUCTION,
        feature_flags.TRAILING_STOP,
        feature_flags.DYNAMIC_PROFIT_PROTECTION,
        feature_flags.HARD_TAKE_PROFIT,
        feature_flags.REAL_BREAK_EVEN_TAKE_PROFIT,
        feature_flags.REAL_PARTIAL_TAKE_PROFIT,
        feature_flags.REAL_TRAILING_REDUCTION,
        feature_flags.REAL_TRAILING_STOP,
        feature_flags.REAL_DYNAMIC_PROFIT_PROTECTION,
        feature_flags.REAL_HARD_TAKE_PROFIT,
    ]
    assert all(
        flag.enabled == (flag.key not in {
            feature_flags.REAL_TRADING_SYSTEM,
            feature_flags.REAL_STOP_LOSS_RULE,
            feature_flags.REAL_REDUCTION_CONDITIONS,
            feature_flags.REAL_INCREASE_CONDITIONS,
            feature_flags.REAL_PORTFOLIO_RISK,
            feature_flags.REAL_BREAK_EVEN_TAKE_PROFIT,
            feature_flags.REAL_PARTIAL_TAKE_PROFIT,
            feature_flags.REAL_TRAILING_REDUCTION,
            feature_flags.REAL_TRAILING_STOP,
            feature_flags.REAL_DYNAMIC_PROFIT_PROTECTION,
            feature_flags.REAL_HARD_TAKE_PROFIT,
        })
        for flag in flags
    )
    assert all(flag.updated_at > 0 for flag in flags)
    assert flags[2].name == "模拟盘交易系统"
    assert flags[3].name == "实盘交易系统"
    assert all(flag.name.startswith("模拟盘") for flag in flags[5:9])
    assert all(flag.name.startswith("实盘") for flag in flags[9:13])
    assert {
        flag.name for flag in flags if flag.key in {
            feature_flags.REAL_BREAK_EVEN_TAKE_PROFIT,
            feature_flags.REAL_PARTIAL_TAKE_PROFIT,
            feature_flags.REAL_TRAILING_REDUCTION,
            feature_flags.REAL_DYNAMIC_PROFIT_PROTECTION,
            feature_flags.REAL_TRAILING_STOP,
            feature_flags.REAL_HARD_TAKE_PROFIT,
        }
    } == {
        "实盘保本止盈",
        "实盘分批止盈",
        "实盘移动追踪减仓",
        "实盘动态利润保护",
        "实盘移动追踪止盈",
        "实盘硬止盈",
    }


def test_real_holding_flags_default_off_and_can_be_enabled(tmp_path):
    db_path = str(tmp_path / "config.db")

    for key in (
        feature_flags.REAL_STOP_LOSS_RULE,
        feature_flags.REAL_REDUCTION_CONDITIONS,
        feature_flags.REAL_INCREASE_CONDITIONS,
        feature_flags.REAL_PORTFOLIO_RISK,
        feature_flags.REAL_HARD_TAKE_PROFIT,
    ):
        assert feature_flags.get_feature_flag(key, db_path).enabled is False
        assert feature_flags.set_feature_flag(key, True, db_path).enabled is True


def test_real_trading_flag_defaults_off_and_can_be_enabled(tmp_path):
    db_path = str(tmp_path / "config.db")

    original = feature_flags.get_feature_flag(feature_flags.REAL_TRADING_SYSTEM, db_path)
    updated = feature_flags.set_feature_flag(
        feature_flags.REAL_TRADING_SYSTEM, True, db_path
    )

    assert original.enabled is False
    assert updated.enabled is True
    assert feature_flags.is_feature_enabled(
        feature_flags.REAL_TRADING_SYSTEM, db_path
    ) is True


def test_set_feature_flag_persists_status_and_updates_timestamp(tmp_path, monkeypatch):
    db_path = str(tmp_path / "base_data.db")
    monkeypatch.setattr(feature_flags, "_now_ms", lambda: 2000)

    feature_flags.init_feature_flags(db_path)
    updated = feature_flags.set_feature_flag(feature_flags.TRADING_SYSTEM, False, db_path)

    assert updated.enabled is False
    assert updated.updated_at == 2000
    assert feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM, db_path) is False


def test_trailing_stop_flag_can_be_disabled(tmp_path):
    db_path = str(tmp_path / "base_data.db")

    updated = feature_flags.set_feature_flag(feature_flags.TRAILING_STOP, False, db_path)

    assert updated.name == "模拟盘移动追踪止盈规则"
    assert updated.enabled is False
    assert feature_flags.is_feature_enabled(feature_flags.TRAILING_STOP, db_path) is False


def test_hard_take_profit_flag_defaults_on_and_can_be_disabled(tmp_path):
    db_path = str(tmp_path / "base_data.db")

    original = feature_flags.get_feature_flag(feature_flags.HARD_TAKE_PROFIT, db_path)
    updated = feature_flags.set_feature_flag(feature_flags.HARD_TAKE_PROFIT, False, db_path)

    assert original.name == "模拟盘硬止盈模块"
    assert original.enabled is True
    assert updated.enabled is False


def test_unknown_feature_flag_rejected(tmp_path):
    db_path = str(tmp_path / "base_data.db")

    try:
        feature_flags.set_feature_flag("missing", False, db_path)
    except KeyError:
        pass
    else:
        raise AssertionError("unknown feature flag should raise KeyError")


def test_initialized_flags_do_not_wait_for_schema_lock(tmp_path, monkeypatch):
    db_path = str(tmp_path / "base_data.db")
    feature_flags.init_feature_flags(db_path)

    class UnexpectedSchemaLock:
        def __init__(self, _db_path):
            raise AssertionError("initialized feature flags must not acquire the schema lock")

    monkeypatch.setattr(feature_flags.db_config, "sqlite_schema_lock", UnexpectedSchemaLock)

    flags = feature_flags.list_feature_flags(db_path)

    assert len(flags) == len(feature_flags.FEATURE_FLAG_DEFINITIONS)
