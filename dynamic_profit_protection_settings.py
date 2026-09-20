"""Shared runtime settings for simulated and live dynamic profit protection."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


SETTINGS_TABLE_NAME = "dynamic_profit_protection_settings"
DEFAULT_SETTINGS = {
    "enabled": True,
    "simulation_enabled": True,
    "live_enabled": True,
    "high_simulation_enabled": True,
    "high_live_enabled": True,
    "allusdt_24h_rise_threshold_percent": 4.0,
    "tier_2_min_r": 2.0,
    "tier_3_min_r": 3.0,
    "tier_4_min_r": 4.0,
    "tier_2_drawdown_ratio": 0.40,
    "tier_3_drawdown_ratio": 0.30,
    "tier_4_drawdown_ratio": 0.20,
    "high_tier_2_min_r": 2.0, "high_tier_3_min_r": 3.0, "high_tier_4_min_r": 4.0,
    "high_tier_2_drawdown_ratio": 0.40, "high_tier_3_drawdown_ratio": 0.30, "high_tier_4_drawdown_ratio": 0.20,
}


def _validate_settings(payload: dict) -> dict[str, bool | float]:
    if not isinstance(payload, dict) or not set(payload).issubset(DEFAULT_SETTINGS):
        raise ValueError("动态利润保护配置包含未知字段")
    payload = {**DEFAULT_SETTINGS, **payload}
    for flag in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled"):
        if not isinstance(payload.get(flag, DEFAULT_SETTINGS[flag]), bool):
            raise ValueError("动态利润保护启用状态必须是布尔值")
    if not isinstance(payload.get("enabled"), bool):
        raise ValueError("周期累积盈亏历史最高到达档位动态保护的启用状态必须是布尔值")
    try:
        values = {
            key: float(payload[key])
            for key in DEFAULT_SETTINGS
            if key not in ("enabled", "simulation_enabled", "live_enabled")
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("R档位和回撤阈值必须是数字") from exc
    if any(not math.isfinite(value) for value in values.values()):
        raise ValueError("R档位和回撤阈值必须是有限数字")
    threshold = values["allusdt_24h_rise_threshold_percent"]
    if not math.isfinite(threshold) or threshold < 0 or threshold > 100:
        raise ValueError("ALLUSDT涨幅阈值必须在 0–100% 之间")
    boundaries = [values["tier_2_min_r"], values["tier_3_min_r"], values["tier_4_min_r"]]
    if boundaries[0] <= 0 or not boundaries[0] < boundaries[1] < boundaries[2]:
        raise ValueError("三个R档位必须大于0并严格递增")
    drawdowns = [values[key] for key in (
        "tier_2_drawdown_ratio", "tier_3_drawdown_ratio", "tier_4_drawdown_ratio"
    )]
    if any(value <= 0 or value > 1 for value in drawdowns):
        raise ValueError("回撤阈值必须大于0%且不超过100%")
    high_boundaries = [values[f"high_tier_{n}_min_r"] for n in (2,3,4)]
    if not high_boundaries[0] < high_boundaries[1] < high_boundaries[2] or high_boundaries[0] <= 0:
        raise ValueError("高涨幅模式三个R档位必须严格递增")
    if any(not 0 < values[f"high_tier_{n}_drawdown_ratio"] <= 1 for n in (2,3,4)):
        raise ValueError("高涨幅模式回撤阈值必须在0–100%之间")
    return {"enabled": payload["enabled"], "simulation_enabled": payload.get("simulation_enabled", True), "live_enabled": payload.get("live_enabled", True), "high_simulation_enabled": payload.get("high_simulation_enabled", True), "high_live_enabled": payload.get("high_live_enabled", True), **values}


def get_settings(db_path: str | None = None) -> dict[str, bool | float]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                enabled INTEGER NOT NULL,
                simulation_enabled INTEGER NOT NULL DEFAULT 1,
                live_enabled INTEGER NOT NULL DEFAULT 1,
                high_simulation_enabled INTEGER NOT NULL DEFAULT 1,
                high_live_enabled INTEGER NOT NULL DEFAULT 1,
                allusdt_24h_rise_threshold_percent REAL NOT NULL DEFAULT 4.0,
                tier_2_min_r REAL NOT NULL,
                tier_3_min_r REAL NOT NULL,
                tier_4_min_r REAL NOT NULL,
                tier_2_drawdown_ratio REAL NOT NULL,
                tier_3_drawdown_ratio REAL NOT NULL,
                tier_4_drawdown_ratio REAL NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({SETTINGS_TABLE_NAME})")}
        for name, definition in (("simulation_enabled", "INTEGER NOT NULL DEFAULT 1"), ("live_enabled", "INTEGER NOT NULL DEFAULT 1"), ("high_simulation_enabled", "INTEGER NOT NULL DEFAULT 1"), ("high_live_enabled", "INTEGER NOT NULL DEFAULT 1"), ("allusdt_24h_rise_threshold_percent", "REAL NOT NULL DEFAULT 4.0")):
            if name not in columns:
                conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN {name} {definition}")
        for name in DEFAULT_SETTINGS:
            if name not in columns and name not in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled", "allusdt_24h_rise_threshold_percent"):
                conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN {name} REAL NOT NULL DEFAULT {DEFAULT_SETTINGS[name]}")
        # Keep the seed columns and values in lockstep as settings evolve.  The
        # high-rise tier fields are added by the migration above and must also
        # be included here; hard-coding the legacy column list causes SQLite to
        # reject the 17-value seed tuple against the old 11 placeholders.
        seed_columns = ["id", *DEFAULT_SETTINGS, "updated_at"]
        seed_values = [
            1,
            *(int(DEFAULT_SETTINGS[key]) if key in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled")
              else DEFAULT_SETTINGS[key] for key in DEFAULT_SETTINGS),
            int(time.time() * 1000),
        ]
        placeholders = ", ".join("?" for _ in seed_columns)
        conn.execute(
            f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} ({', '.join(seed_columns)}) VALUES ({placeholders})",
            seed_values,
        )
        row = conn.execute(
            f"SELECT {', '.join(DEFAULT_SETTINGS)} FROM {SETTINGS_TABLE_NAME} WHERE id = 1"
        ).fetchone()
        conn.commit()
    return {
        key: bool(row[key]) if key in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled") else float(row[key])
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
                *(int(settings[key]) if key in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled") else settings[key] for key in keys),
                int(time.time() * 1000),
            ),
        )
    return get_settings(path)
