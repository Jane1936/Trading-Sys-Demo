"""Persistent maximum concurrent-position settings for simulated and live trading."""
from __future__ import annotations

import math
import sqlite3
import time

import db_config


DEFAULT_SETTINGS = {
    "simulation_max_open_positions": 10,
    "live_max_open_positions": 10,
    "max_new_positions_per_round": 5,
}
SETTINGS_TABLE_NAME = "position_limit_settings"


def _validate_settings(payload: dict) -> dict[str, int]:
    try:
        values = {key: float(payload[key]) for key in DEFAULT_SETTINGS}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("最大持仓仓位个数和每轮最多新开仓个数均为必填数字") from exc
    if any(not math.isfinite(value) or not value.is_integer() for value in values.values()):
        raise ValueError("仓位个数配置必须是整数")
    settings = {key: int(value) for key, value in values.items()}
    if any(value < 1 or value > 1000 for value in settings.values()):
        raise ValueError("仓位个数配置必须在 1–1000 之间")
    return settings


def get_settings(db_path: str | None = None) -> dict[str, int]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                simulation_max_open_positions INTEGER NOT NULL,
                live_max_open_positions INTEGER NOT NULL,
                max_new_positions_per_round INTEGER NOT NULL DEFAULT 5,
                updated_at INTEGER NOT NULL)"""
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
        conn.execute(
            f"""INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME}
                (id, simulation_max_open_positions, live_max_open_positions,
                 max_new_positions_per_round, updated_at)
                VALUES (1, ?, ?, ?, ?)""",
            (*DEFAULT_SETTINGS.values(), int(time.time() * 1000)),
        )
        row = conn.execute(
            f"SELECT simulation_max_open_positions, live_max_open_positions, "
            f"max_new_positions_per_round "
            f"FROM {SETTINGS_TABLE_NAME} WHERE id = 1"
        ).fetchone()
        conn.commit()
    return {key: int(row[key]) for key in DEFAULT_SETTINGS}


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, int]:
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"""UPDATE {SETTINGS_TABLE_NAME}
                SET simulation_max_open_positions = ?, live_max_open_positions = ?,
                    max_new_positions_per_round = ?,
                    updated_at = ? WHERE id = 1""",
            (
                settings["simulation_max_open_positions"],
                settings["live_max_open_positions"],
                settings["max_new_positions_per_round"],
                int(time.time() * 1000),
            ),
        )
        conn.commit()
    return settings
