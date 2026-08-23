import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db_config
import feature_flags
import market_filter_settings
import openable_symbol_settings
import scoring_rule_election
import weak_market_profit_adjustment
from config_database import MIGRATED_TABLES, initialize_config_database
from dynamic_open_threshold import get_settings as get_dynamic_settings
from dynamic_open_threshold import set_settings as set_dynamic_settings
from scoring_system import DEFAULT_RULE_SCORE_WEIGHTS, set_rule_score_weight_settings


def _election_rules(required_rule: int) -> list[dict]:
    return [
        {
            "rule_id": rule_id,
            "status": "required" if rule_id == required_rule else "ignored",
        }
        for rule_id in range(1, 19)
    ]


def test_initialize_config_database_migrates_only_requested_runtime_config(tmp_path):
    base_db = str(tmp_path / "base_data.db")
    config_db = str(tmp_path / "config.db")

    feature_flags.set_feature_flag(feature_flags.TRADING_SYSTEM, False, base_db)
    market_filter_settings.set_settings(
        {
            "btc_siphon_threshold": 0.02,
            "market_crash_threshold": 0.04,
            "block_duration_minutes": 90,
        },
        base_db,
    )
    set_dynamic_settings(
        {
            "window_hours": 24,
            "unrestricted_score": 90,
            "restricted_score_floor": 75,
            "min_open_total_score": 83,
        },
        base_db,
    )
    weak_market_profit_adjustment.set_settings(
        {"trigger_r_multiple": 1.8, "take_profit_fraction": 0.6}, base_db
    )
    weights = DEFAULT_RULE_SCORE_WEIGHTS.copy()
    weights[1] = 42
    set_rule_score_weight_settings(weights, base_db)
    scoring_rule_election.set_settings(
        {"rules": _election_rules(3), "optional_min": 0}, base_db
    )
    openable_symbol_settings.set_settings(
        openable_symbol_settings.DEFAULT_SETTINGS, base_db
    )

    initialize_config_database(config_db, base_db)

    assert feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM, config_db) is False
    assert market_filter_settings.get_settings(config_db)["block_duration_minutes"] == 90
    assert get_dynamic_settings(config_db)["window_hours"] == 24
    assert weak_market_profit_adjustment.get_settings(config_db)["trigger_r_multiple"] == 1.8
    with db_config.connect_sqlite(config_db, row_factory=sqlite3.Row) as conn:
        assert conn.execute(
            "SELECT weight FROM scoring_rule_weights WHERE rule_id=1"
        ).fetchone()["weight"] == 42
        assert conn.execute(
            "SELECT status FROM scoring_rule_election WHERE rule_id=3"
        ).fetchone()["status"] == "required"
        assert conn.execute(
            "SELECT optional_min FROM scoring_rule_election_config WHERE id=1"
        ).fetchone()["optional_min"] == 0
        table_names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert set(MIGRATED_TABLES) <= table_names
    assert "runtime_settings" not in table_names


def test_legacy_values_are_imported_once_and_do_not_overwrite_new_config(tmp_path):
    base_db = str(tmp_path / "base_data.db")
    config_db = str(tmp_path / "config.db")
    feature_flags.set_feature_flag(feature_flags.TRADING_SYSTEM, False, base_db)
    initialize_config_database(config_db, base_db)

    feature_flags.set_feature_flag(feature_flags.TRADING_SYSTEM, True, config_db)
    initialize_config_database(config_db, base_db)

    assert feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM, config_db) is True
