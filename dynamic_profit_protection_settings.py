"""Shared runtime settings for simulated and live dynamic profit protection."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


SETTINGS_TABLE_NAME = "dynamic_profit_protection_settings"
DEFAULT_SETTINGS = {
    "enabled": True,
    "tier_2_min_r": 2.0,
    "tier_3_min_r": 3.0,
    "tier_4_min_r": 4.0,
    "tier_2_drawdown_ratio": 0.40,
    "tier_3_drawdown_ratio": 0.30,
    "tier_4_drawdown_ratio": 0.20,
}


def _validate_settings(payload: dict) -> dict[str, bool | float]:
    if not isinstance(payload, dict) or set(payload) != set(DEFAULT_SETTINGS):
        raise ValueError("必须提供动态利润保护的启用状态、三个R档位和三个回撤阈值")
    if not isinstance(payload.get("enabled"), bool):
        raise ValueError("周期累积盈亏历史最高到达档位动态保护的启用状态必须是布尔值")
    try:
        values = {
            key: float(payload[key])
            for key in DEFAULT_SETTINGS
            if key != "enabled"
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("R档位和回撤阈值必须是数字") from exc
    if any(not math.isfinite(value) for value in values.values()):
        raise ValueError("R档位和回撤阈值必须是有限数字")
    boundaries = [values["tier_2_min_r"], values["tier_3_min_r"], values["tier_4_min_r"]]
    if boundaries[0] <= 0 or not boundaries[0] < boundaries[1] < boundaries[2]:
        raise ValueError("三个R档位必须大于0并严格递增")
    drawdowns = [values[key] for key in (
        "tier_2_drawdown_ratio", "tier_3_drawdown_ratio", "tier_4_drawdown_ratio"
    )]
    if any(value <= 0 or value > 1 for value in drawdowns):
        raise ValueError("回撤阈值必须大于0%且不超过100%")
    return {"enabled": payload["enabled"], **values}


def get_settings(db_path: str | None = None) -> dict[str, bool | float]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                enabled INTEGER NOT NULL,
                tier_2_min_r REAL NOT NULL,
                tier_3_min_r REAL NOT NULL,
                tier_4_min_r REAL NOT NULL,
                tier_2_drawdown_ratio REAL NOT NULL,
                tier_3_drawdown_ratio REAL NOT NULL,
                tier_4_drawdown_ratio REAL NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        conn.execute(
            f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(DEFAULT_SETTINGS["enabled"]),
                *(DEFAULT_SETTINGS[key] for key in DEFAULT_SETTINGS if key != "enabled"),
                int(time.time() * 1000),
            ),
        )
        row = conn.execute(
            f"SELECT {', '.join(DEFAULT_SETTINGS)} FROM {SETTINGS_TABLE_NAME} WHERE id = 1"
        ).fetchone()
        conn.commit()
    return {
        key: bool(row[key]) if key == "enabled" else float(row[key])
        for key in DEFAULT_SETTINGS
    }


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, bool | float]:
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    keys = list(DEFAULT_SETTINGS)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"UPDATE {SETTINGS_TABLE_NAME} SET "
            + ", ".join(f"{key} = ?" for key in keys)
            + ", updated_at = ? WHERE id = 1",
            (
                *(int(settings[key]) if key == "enabled" else settings[key] for key in keys),
                int(time.time() * 1000),
            ),
        )
    return get_settings(path)
