"""Initialization and one-time migration for the runtime configuration DB."""
from __future__ import annotations

import os
import sqlite3

import db_config
import feature_flags
import market_filter_settings
import openable_symbol_settings
import scoring_rule_election
import weak_market_profit_adjustment
import position_limit_settings
import dynamic_profit_protection_settings
from dynamic_open_threshold import get_settings as get_dynamic_open_threshold_settings
from scoring_system import init_rule_score_weight_settings


MIGRATION_NAME = "base_data_runtime_config_v1"
RUNTIME_SETTINGS_MIGRATION_NAME = "base_data_runtime_settings_v1"
MIGRATED_TABLES = (
    "feature_flags",
    "market_filter_settings",
    "dynamic_open_threshold_settings",
    "weak_market_profit_adjustment_settings",
    "dynamic_profit_protection_settings",
    "scoring_rule_weights",
    "scoring_rule_election",
    "scoring_rule_election_config",
)


def _seed_config_database(config_db_path: str) -> None:
    feature_flags.init_feature_flags(config_db_path)
    market_filter_settings.get_settings(config_db_path)
    get_dynamic_open_threshold_settings(config_db_path)
    weak_market_profit_adjustment.get_settings(config_db_path)
    init_rule_score_weight_settings(config_db_path)
    scoring_rule_election.init_settings(config_db_path)
    openable_symbol_settings.get_settings(config_db_path)
    position_limit_settings.get_settings(config_db_path)
    dynamic_profit_protection_settings.get_settings(config_db_path)


def _source_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM legacy.sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def initialize_config_database(
    config_db_path: str | None = None,
    legacy_base_db_path: str | None = None,
) -> None:
    """Create config tables and import their legacy base-DB values exactly once.

    Target tables are seeded first so a new installation gets normal defaults.
    On an upgrade, rows from ``base_data.db`` replace those defaults before a
    migration marker is committed. Existing legacy tables are intentionally
    retained to make rollback safe; all production readers use ``config.db``.
    """
    config_db_path = config_db_path or db_config.CONFIG_DB_PATH
    legacy_base_db_path = legacy_base_db_path or db_config.BASE_DB_PATH
    _seed_config_database(config_db_path)

    with db_config.sqlite_schema_lock(config_db_path):
        with db_config.connect_sqlite(config_db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS config_migrations (
                    name TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""
            )
            legacy_config_migrated = conn.execute(
                "SELECT 1 FROM config_migrations WHERE name = ?", (MIGRATION_NAME,)
            ).fetchone()
            runtime_settings_migrated = conn.execute(
                "SELECT 1 FROM config_migrations WHERE name = ?",
                (RUNTIME_SETTINGS_MIGRATION_NAME,),
            ).fetchone()

            same_file = os.path.realpath(config_db_path) == os.path.realpath(
                legacy_base_db_path
            )
            has_legacy_database = not same_file and os.path.exists(legacy_base_db_path)
            if has_legacy_database:
                db_config.attach_databases(conn, [("legacy", legacy_base_db_path)])
                if not legacy_config_migrated:
                    for table_name in MIGRATED_TABLES:
                        if _source_table_exists(conn, table_name):
                            quoted = db_config.quote_identifier(table_name)
                            conn.execute(f"DELETE FROM main.{quoted}")
                            conn.execute(
                                f"INSERT INTO main.{quoted} SELECT * FROM legacy.{quoted}"
                            )
                if (
                    not runtime_settings_migrated
                    and _source_table_exists(conn, "runtime_settings")
                ):
                    conn.execute("DELETE FROM main.runtime_settings")
                    conn.execute(
                        "INSERT INTO main.runtime_settings "
                        "SELECT * FROM legacy.runtime_settings"
                    )
            if not legacy_config_migrated:
                conn.execute(
                    "INSERT INTO config_migrations (name) VALUES (?)", (MIGRATION_NAME,)
                )
            if not runtime_settings_migrated:
                conn.execute(
                    "INSERT INTO config_migrations (name) VALUES (?)",
                    (RUNTIME_SETTINGS_MIGRATION_NAME,),
                )
            conn.commit()
