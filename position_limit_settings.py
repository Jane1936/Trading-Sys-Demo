"""Persistent maximum concurrent-position settings for simulated and live trading."""
from __future__ import annotations

import math
import sqlite3
import time

import db_config
import allusdt_24h_ticker


DEFAULT_SETTINGS = {
    "simulation_max_open_positions": 10,
    "live_max_open_positions": 10,
    "max_new_positions_per_round": 5,
    "allusdt_24h_rise_threshold_percent": 4.0,
    "allusdt_24h_rise_max_open_positions": 15,
}
SETTINGS_TABLE_NAME = "position_limit_settings"


def _validate_settings(payload: dict) -> dict[str, int | float]:
    try:
        values = {key: float(payload.get(key, DEFAULT_SETTINGS[key])) for key in DEFAULT_SETTINGS}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("最大持仓仓位个数和每轮最多新开仓个数均为必填数字") from exc
    count_keys = {
        "simulation_max_open_positions",
        "live_max_open_positions",
        "max_new_positions_per_round",
        "allusdt_24h_rise_max_open_positions",
    }
    if any(
        not math.isfinite(values[key]) or not values[key].is_integer()
        for key in count_keys
    ):
        raise ValueError("仓位个数配置必须是整数")
    settings: dict[str, int | float] = {
        key: (int(value) if key in count_keys else value)
        for key, value in values.items()
    }
    if any(settings[key] < 1 or settings[key] > 1000 for key in count_keys):
        raise ValueError("仓位个数配置必须在 1–1000 之间")
    threshold = values["allusdt_24h_rise_threshold_percent"]
    if not math.isfinite(threshold) or threshold < 0 or threshold > 100:
        raise ValueError("ALLUSDT涨幅阈值必须在 0–100% 之间")
    return settings


def get_settings(db_path: str | None = None) -> dict[str, int | float]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                simulation_max_open_positions INTEGER NOT NULL,
                live_max_open_positions INTEGER NOT NULL,
                max_new_positions_per_round INTEGER NOT NULL DEFAULT 5,
                updated_at INTEGER NOT NULL,
                allusdt_24h_rise_threshold_percent REAL NOT NULL DEFAULT 4.0,
                allusdt_24h_rise_max_open_positions INTEGER NOT NULL DEFAULT 15)"""
        )
        columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({SETTINGS_TABLE_NAME})")
        }
        if "max_new_positions_per_round" not in columns:
            conn.execute(
                f"ALTER TABLE {SETTINGS_TABLE_NAME} "
                "ADD COLUMN max_new_positions_per_round INTEGER NOT NULL DEFAULT 5"
            )
        for name, definition in (("allusdt_24h_rise_threshold_percent", "REAL NOT NULL DEFAULT 4.0"), ("allusdt_24h_rise_max_open_positions", "INTEGER NOT NULL DEFAULT 15")):
            if name not in columns:
                conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN {name} {definition}")
        conn.execute(
            f"""INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME}
                (id, simulation_max_open_positions, live_max_open_positions,
                max_new_positions_per_round, updated_at, allusdt_24h_rise_threshold_percent,
                allusdt_24h_rise_max_open_positions)
                VALUES (1, ?, ?, ?, ?, ?, ?)""",
            (DEFAULT_SETTINGS["simulation_max_open_positions"], DEFAULT_SETTINGS["live_max_open_positions"], DEFAULT_SETTINGS["max_new_positions_per_round"], int(time.time() * 1000), DEFAULT_SETTINGS["allusdt_24h_rise_threshold_percent"], DEFAULT_SETTINGS["allusdt_24h_rise_max_open_positions"]),
        )
        row = conn.execute(
            f"SELECT simulation_max_open_positions, live_max_open_positions, "
            f"max_new_positions_per_round, allusdt_24h_rise_threshold_percent, allusdt_24h_rise_max_open_positions "
            f"FROM {SETTINGS_TABLE_NAME} WHERE id = 1"
        ).fetchone()
        conn.commit()
    return {key: (float(row[key]) if "threshold" in key else int(row[key])) for key in DEFAULT_SETTINGS}


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, int | float]:
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"""UPDATE {SETTINGS_TABLE_NAME}
                SET simulation_max_open_positions = ?, live_max_open_positions = ?,
                    max_new_positions_per_round = ?, allusdt_24h_rise_threshold_percent = ?,
                    allusdt_24h_rise_max_open_positions = ?,
                    updated_at = ? WHERE id = 1""",
            (
                settings["simulation_max_open_positions"],
                settings["live_max_open_positions"],
                settings["max_new_positions_per_round"],
                settings["allusdt_24h_rise_threshold_percent"], settings["allusdt_24h_rise_max_open_positions"],
                int(time.time() * 1000),
            ),
        )
        conn.commit()
    return settings


def effective_max_open_positions(base: int, settings: dict | None = None) -> int:
    """Raise the limit when ALLUSDT's rolling 24h gain exceeds the configured threshold."""
    settings = settings or get_settings()
    try:
        change = allusdt_24h_ticker.fetch_change_percent()
    except Exception:
        return int(base)
    if change > settings["allusdt_24h_rise_threshold_percent"]:
        return int(settings["allusdt_24h_rise_max_open_positions"])
    return int(base)
