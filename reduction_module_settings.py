"""Persistent per-rule settings for the holding reduction module."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


SETTINGS_TABLE_NAME = "reduction_module_settings"
DEFAULTS = {
    "rule2": {"enabled": True, "reduction_fraction": 0.25},
    "rule5": {"enabled": True, "reduction_fraction": 0.5},
}


def _initialize(db_path: str) -> None:
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                    rule_key TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL,
                    reduction_fraction REAL NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            now_ms = int(time.time() * 1000)
            for rule_key, defaults in DEFAULTS.items():
                conn.execute(
                    f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} VALUES (?, ?, ?, ?)",
                    (rule_key, int(defaults["enabled"]), defaults["reduction_fraction"], now_ms),
                )


def get_settings(db_path: str | None = None) -> dict[str, dict[str, bool | float | int]]:
    settings_path = db_path or db_config.CONFIG_DB_PATH
    _initialize(settings_path)
    with db_config.connect_sqlite(settings_path, row_factory=sqlite3.Row) as conn:
        rows = conn.execute(
            f"SELECT rule_key, enabled, reduction_fraction, updated_at FROM {SETTINGS_TABLE_NAME}"
        ).fetchall()
    return {
        str(row["rule_key"]): {
            "enabled": bool(row["enabled"]),
            "reduction_fraction": float(row["reduction_fraction"]),
            "updated_at": int(row["updated_at"]),
        }
        for row in rows
    }


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, dict[str, bool | float | int]]:
    if not isinstance(payload, dict) or set(payload) != set(DEFAULTS):
        raise ValueError("必须同时提供规则二和规则五的完整配置")
    normalized: dict[str, tuple[bool, float]] = {}
    for rule_key in DEFAULTS:
        item = payload.get(rule_key)
        if not isinstance(item, dict) or not isinstance(item.get("enabled"), bool):
            raise ValueError(f"{rule_key} 的启用状态必须是布尔值")
        try:
            fraction = float(item["reduction_fraction"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{rule_key} 的减仓比例必须是数字") from exc
        if not math.isfinite(fraction) or not 0 < fraction <= 1:
            raise ValueError(f"{rule_key} 的减仓比例必须大于 0% 且不超过 100%")
        normalized[rule_key] = (item["enabled"], fraction)

    settings_path = db_path or db_config.CONFIG_DB_PATH
    _initialize(settings_path)
    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(settings_path) as conn:
        for rule_key, (enabled, fraction) in normalized.items():
            conn.execute(
                f"UPDATE {SETTINGS_TABLE_NAME} SET enabled = ?, reduction_fraction = ?, updated_at = ? WHERE rule_key = ?",
                (int(enabled), fraction, now_ms, rule_key),
            )
    return get_settings(settings_path)
