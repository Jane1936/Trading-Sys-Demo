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
        feature_flags.MARKET_FILTER,
    ]
    assert all(flag.enabled for flag in flags)
    assert all(flag.updated_at > 0 for flag in flags)


def test_set_feature_flag_persists_status_and_updates_timestamp(tmp_path, monkeypatch):
    db_path = str(tmp_path / "base_data.db")
    monkeypatch.setattr(feature_flags, "_now_ms", lambda: 2000)

    feature_flags.init_feature_flags(db_path)
    updated = feature_flags.set_feature_flag(feature_flags.TRADING_SYSTEM, False, db_path)

    assert updated.enabled is False
    assert updated.updated_at == 2000
    assert feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM, db_path) is False


def test_unknown_feature_flag_rejected(tmp_path):
    db_path = str(tmp_path / "base_data.db")

    try:
        feature_flags.set_feature_flag("missing", False, db_path)
    except KeyError:
        pass
    else:
        raise AssertionError("unknown feature flag should raise KeyError")
