"""Runtime configuration for scoring-rule opening elections."""

from __future__ import annotations

import sqlite3
import time

import db_config
from scoring_system import DEFAULT_RULE_SCORE_WEIGHTS, RULE_SCORE_NAMES


RULE_STATUSES = ("required", "optional", "ignored")
CONFIG_KEYS = ("A", "B", "C", "D", "E")
COMBINATION_MODES = ("any", "all")
DEFAULT_STATUS = "ignored"
DEFAULT_AUTO_THRESHOLD_PERCENT = -1.0
DEFAULT_AUTO_CONFIG_KEY = "A"


def init_settings(db_path: str | None = None) -> None:
    db_path = db_path or db_config.CONFIG_DB_PATH
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            # Keep the two legacy tables so upgrades and rollback remain safe.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election (
                    rule_id INTEGER PRIMARY KEY, status TEXT NOT NULL, updated_at INTEGER NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1), optional_min INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election_profiles (
                    config_key TEXT NOT NULL, rule_id INTEGER NOT NULL, status TEXT NOT NULL,
                    updated_at INTEGER NOT NULL, PRIMARY KEY (config_key, rule_id)
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election_profile_config (
                    config_key TEXT PRIMARY KEY, enabled INTEGER NOT NULL, optional_min INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election_combination (
                    id INTEGER PRIMARY KEY CHECK (id = 1), mode TEXT NOT NULL, updated_at INTEGER NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_rule_election_automation (
                    id INTEGER PRIMARY KEY CHECK (id = 1), enabled INTEGER NOT NULL,
                    threshold_percent REAL NOT NULL, config_key TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )"""
            )
            now_ms = int(time.time() * 1000)
            conn.executemany(
                "INSERT INTO scoring_rule_election VALUES (?, ?, ?) ON CONFLICT(rule_id) DO NOTHING",
                [(rule_id, DEFAULT_STATUS, now_ms) for rule_id in DEFAULT_RULE_SCORE_WEIGHTS],
            )
            conn.execute(
                "INSERT INTO scoring_rule_election_config VALUES (1, 0, ?) ON CONFLICT(id) DO NOTHING",
                (now_ms,),
            )
            # Configuration A inherits the former single configuration. B-E start disabled.
            legacy = conn.execute(
                "SELECT rule_id, status, updated_at FROM scoring_rule_election ORDER BY rule_id"
            ).fetchall()
            for key in CONFIG_KEYS:
                conn.executemany(
                    "INSERT INTO scoring_rule_election_profiles VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(config_key, rule_id) DO NOTHING",
                    [(key, row[0], row[1] if key == "A" else DEFAULT_STATUS, row[2]) for row in legacy],
                )
                conn.execute(
                    "INSERT INTO scoring_rule_election_profile_config VALUES (?, ?, 0, ?) "
                    "ON CONFLICT(config_key) DO NOTHING",
                    (key, int(key == "A"), now_ms),
                )
            conn.execute(
                "INSERT INTO scoring_rule_election_combination VALUES (1, 'any', ?) ON CONFLICT(id) DO NOTHING",
                (now_ms,),
            )
            conn.execute(
                "INSERT INTO scoring_rule_election_automation VALUES (1, 1, ?, ?, ?) "
                "ON CONFLICT(id) DO NOTHING",
                (DEFAULT_AUTO_THRESHOLD_PERCENT, DEFAULT_AUTO_CONFIG_KEY, now_ms),
            )


def get_settings(db_path: str | None = None) -> dict:
    db_path = db_path or db_config.CONFIG_DB_PATH
    init_settings(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        rows = conn.execute(
            "SELECT config_key, rule_id, status, updated_at FROM scoring_rule_election_profiles "
            "ORDER BY config_key, rule_id"
        ).fetchall()
        profile_configs = {
            row["config_key"]: row for row in conn.execute(
                "SELECT * FROM scoring_rule_election_profile_config"
            ).fetchall()
        }
        combination = conn.execute(
            "SELECT mode, updated_at FROM scoring_rule_election_combination WHERE id = 1"
        ).fetchone()
        automation = conn.execute(
            "SELECT enabled, threshold_percent, config_key, updated_at "
            "FROM scoring_rule_election_automation WHERE id = 1"
        ).fetchone()
    configurations = []
    for key in CONFIG_KEYS:
        config_rows = [row for row in rows if row["config_key"] == key]
        metadata = profile_configs[key]
        configurations.append({
            "key": key,
            "enabled": bool(metadata["enabled"]),
            "optional_min": int(metadata["optional_min"]),
            "updated_at": int(metadata["updated_at"]),
            "rules": [{
                "rule_id": int(row["rule_id"]),
                "name": RULE_SCORE_NAMES[int(row["rule_id"])],
                "status": row["status"],
                "updated_at": int(row["updated_at"]),
            } for row in config_rows],
        })
    return {
        "configurations": configurations,
        "combination_mode": combination["mode"],
        "updated_at": int(combination["updated_at"]),
        "automation": {
            "enabled": bool(automation["enabled"]),
            "threshold_percent": float(automation["threshold_percent"]),
            "config_key": automation["config_key"],
            "updated_at": int(automation["updated_at"]),
        },
    }


def _validate_configuration(raw: dict, expected_key: str) -> tuple[bool, dict[int, str], int]:
    if not isinstance(raw, dict) or raw.get("key") != expected_key:
        raise ValueError(f"configuration {expected_key} is required and must be in order")
    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ValueError(f"configuration {expected_key} enabled must be a boolean")
    raw_rules = raw.get("rules")
    if not isinstance(raw_rules, list) or len(raw_rules) != 18:
        raise ValueError(f"configuration {expected_key} rules must contain all 18 scoring rules")
    try:
        statuses = {int(item["rule_id"]): item["status"] for item in raw_rules}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Each rule requires a valid rule_id and status") from exc
    if len(statuses) != 18 or set(statuses) != set(DEFAULT_RULE_SCORE_WEIGHTS):
        raise ValueError("rules must contain each rule_id from 1 through 18 exactly once")
    if any(status not in RULE_STATUSES for status in statuses.values()):
        raise ValueError("status must be required, optional, or ignored")
    optional_min = raw.get("optional_min")
    if isinstance(optional_min, bool) or not isinstance(optional_min, int):
        raise ValueError("optional_min must be an integer")
    optional_count = sum(status == "optional" for status in statuses.values())
    if not 0 <= optional_min <= optional_count:
        raise ValueError("optional_min must be between 0 and the number of optional rules")
    return enabled, statuses, optional_min


def set_settings(payload: dict, db_path: str | None = None) -> dict:
    db_path = db_path or db_config.CONFIG_DB_PATH
    init_settings(db_path)
    configurations = payload.get("configurations")
    if not isinstance(configurations, list) or len(configurations) != len(CONFIG_KEYS):
        raise ValueError("configurations must contain A, B, C, D and E")
    validated = [
        _validate_configuration(raw, key)
        for key, raw in zip(CONFIG_KEYS, configurations)
    ]
    if not any(enabled for enabled, _, _ in validated):
        raise ValueError("at least one configuration must be enabled")
    mode = payload.get("combination_mode")
    if mode not in COMBINATION_MODES:
        raise ValueError("combination_mode must be any or all")
    automation = payload.get("automation")
    if automation is None:
        with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
            current = conn.execute(
                "SELECT enabled, threshold_percent, config_key "
                "FROM scoring_rule_election_automation WHERE id = 1"
            ).fetchone()
        automation = {
            "enabled": bool(current["enabled"]),
            "threshold_percent": float(current["threshold_percent"]),
            "config_key": current["config_key"],
        }
    if not isinstance(automation, dict) or not isinstance(automation.get("enabled"), bool):
        raise ValueError("automation enabled must be a boolean")
    threshold_percent = automation.get("threshold_percent")
    if isinstance(threshold_percent, bool) or not isinstance(threshold_percent, (int, float)):
        raise ValueError("automation threshold_percent must be a number")
    if not -100 <= float(threshold_percent) <= 100:
        raise ValueError("automation threshold_percent must be between -100 and 100")
    auto_config_key = automation.get("config_key")
    if auto_config_key not in CONFIG_KEYS:
        raise ValueError("automation config_key must be A, B, C, D or E")

    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(db_path) as conn:
        for key, (enabled, statuses, optional_min) in zip(CONFIG_KEYS, validated):
            conn.executemany(
                "UPDATE scoring_rule_election_profiles SET status = ?, updated_at = ? "
                "WHERE config_key = ? AND rule_id = ?",
                [(status, now_ms, key, rule_id) for rule_id, status in statuses.items()],
            )
            conn.execute(
                "UPDATE scoring_rule_election_profile_config SET enabled = ?, optional_min = ?, "
                "updated_at = ? WHERE config_key = ?",
                (int(enabled), optional_min, now_ms, key),
            )
        conn.execute(
            "UPDATE scoring_rule_election_combination SET mode = ?, updated_at = ? WHERE id = 1",
            (mode, now_ms),
        )
        conn.execute(
            "UPDATE scoring_rule_election_automation SET enabled = ?, threshold_percent = ?, "
            "config_key = ?, updated_at = ? WHERE id = 1",
            (int(automation["enabled"]), float(threshold_percent), auto_config_key, now_ms),
        )
        # Mirror A for old binaries during a rolling deployment.
        _, a_statuses, a_optional_min = validated[0]
        conn.executemany(
            "UPDATE scoring_rule_election SET status = ?, updated_at = ? WHERE rule_id = ?",
            [(status, now_ms, rule_id) for rule_id, status in a_statuses.items()],
        )
        conn.execute(
            "UPDATE scoring_rule_election_config SET optional_min = ?, updated_at = ? WHERE id = 1",
            (a_optional_min, now_ms),
        )
    return get_settings(db_path)


def apply_automation(settings: dict, allusdt_24h_change_percent: float | None) -> dict:
    """Return effective election settings for a round without overwriting user choices."""
    automation = settings.get("automation", {})
    triggered = (
        automation.get("enabled") is True
        and allusdt_24h_change_percent is not None
        and float(allusdt_24h_change_percent) < float(automation["threshold_percent"])
    )
    if not triggered:
        return settings
    selected = automation["config_key"]
    return {
        **settings,
        "configurations": [
            {**config, "enabled": config["key"] == selected}
            for config in settings["configurations"]
        ],
        "automation_triggered": True,
    }
