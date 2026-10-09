"""Independent runtime settings for simulated and live dynamic profit protection."""

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
    "simulation_allusdt_24h_rise_threshold_percent": 4.0, "live_allusdt_24h_rise_threshold_percent": 4.0,
    "allusdt_24h_disable_threshold_percent": 10.0,
    "simulation_allusdt_24h_disable_threshold_percent": 10.0,
    "live_allusdt_24h_disable_threshold_percent": 10.0,
    "tier_2_min_r": 2.0,
    "tier_3_min_r": 3.0,
    "tier_4_min_r": 4.0,
    "tier_2_drawdown_ratio": 0.40,
    "tier_3_drawdown_ratio": 0.30,
    "tier_4_drawdown_ratio": 0.20,
    "high_tier_2_min_r": 2.0, "high_tier_3_min_r": 3.0, "high_tier_4_min_r": 4.0,
    "high_tier_2_drawdown_ratio": 0.40, "high_tier_3_drawdown_ratio": 0.30, "high_tier_4_drawdown_ratio": 0.20,
    "simulation_tier_2_min_r": 2.0, "simulation_tier_3_min_r": 3.0, "simulation_tier_4_min_r": 4.0,
    "simulation_tier_2_drawdown_ratio": 0.40, "simulation_tier_3_drawdown_ratio": 0.30, "simulation_tier_4_drawdown_ratio": 0.20,
    "live_tier_2_min_r": 2.0, "live_tier_3_min_r": 3.0, "live_tier_4_min_r": 4.0,
    "live_tier_2_drawdown_ratio": 0.40, "live_tier_3_drawdown_ratio": 0.30, "live_tier_4_drawdown_ratio": 0.20,
    "simulation_high_tier_2_min_r": 2.0, "simulation_high_tier_3_min_r": 3.0, "simulation_high_tier_4_min_r": 4.0,
    "simulation_high_tier_2_drawdown_ratio": 0.40, "simulation_high_tier_3_drawdown_ratio": 0.30, "simulation_high_tier_4_drawdown_ratio": 0.20,
    "live_high_tier_2_min_r": 2.0, "live_high_tier_3_min_r": 3.0, "live_high_tier_4_min_r": 4.0,
    "live_high_tier_2_drawdown_ratio": 0.40, "live_high_tier_3_drawdown_ratio": 0.30, "live_high_tier_4_drawdown_ratio": 0.20,
}


def _validate_settings(payload: dict) -> dict[str, bool | float]:
    if not isinstance(payload, dict) or not set(payload).issubset(DEFAULT_SETTINGS):
        raise ValueError("动态利润保护配置包含未知字段")
    payload = {**DEFAULT_SETTINGS, **payload}
    boolean_fields = ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled")
    for flag in boolean_fields:
        if not isinstance(payload.get(flag, DEFAULT_SETTINGS[flag]), bool):
            raise ValueError("动态利润保护启用状态必须是布尔值")
    if not isinstance(payload.get("enabled"), bool):
        raise ValueError("周期累积盈亏历史最高到达档位动态保护的启用状态必须是布尔值")
    try:
        values = {
            key: float(payload[key])
            for key in DEFAULT_SETTINGS
            if key not in boolean_fields
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("R档位和回撤阈值必须是数字") from exc
    if any(not math.isfinite(value) for value in values.values()):
        raise ValueError("R档位和回撤阈值必须是有限数字")
    legacy_disable_threshold = values["allusdt_24h_disable_threshold_percent"]
    if legacy_disable_threshold < 0 or legacy_disable_threshold > 100:
        raise ValueError("关闭动态利润保护阈值必须在 0–100% 之间")
    configurations = (
        ("兼容配置基础模式", "tier"),
        ("兼容配置高涨幅模式", "high_tier"),
    )
    for env_label, env in (("模拟盘", "simulation"), ("实盘", "live")):
        disable_threshold = values[f"{env}_allusdt_24h_disable_threshold_percent"]
        if disable_threshold < 0 or disable_threshold > 100:
            raise ValueError(f"{env_label}关闭动态利润保护阈值必须在 0–100% 之间")
        configurations += (
            (f"{env_label}基础模式", f"{env}_tier"),
            (f"{env_label}高涨幅模式", f"{env}_high_tier"),
        )
    for label, prefix in configurations:
        boundaries = [values[f"{prefix}_{n}_min_r"] for n in (2, 3, 4)]
        if boundaries[0] <= 0 or not boundaries[0] < boundaries[1] < boundaries[2]:
            raise ValueError(f"{label}三个R档位必须大于0并严格递增")
        drawdowns = [values[f"{prefix}_{n}_drawdown_ratio"] for n in (2, 3, 4)]
        if any(value <= 0 or value > 1 for value in drawdowns):
            raise ValueError(f"{label}回撤阈值必须大于0%且不超过100%")
    return {key: payload[key] for key in boolean_fields} | values


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
                allusdt_24h_disable_threshold_percent REAL NOT NULL DEFAULT 10.0,
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
        for name, definition in (("simulation_enabled", "INTEGER NOT NULL DEFAULT 1"), ("live_enabled", "INTEGER NOT NULL DEFAULT 1"), ("high_simulation_enabled", "INTEGER NOT NULL DEFAULT 1"), ("high_live_enabled", "INTEGER NOT NULL DEFAULT 1"), ("allusdt_24h_rise_threshold_percent", "REAL NOT NULL DEFAULT 4.0"), ("allusdt_24h_disable_threshold_percent", "REAL NOT NULL DEFAULT 10.0")):
            if name not in columns:
                conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN {name} {definition}")
                columns.add(name)
        for name in (
            "simulation_allusdt_24h_disable_threshold_percent",
            "live_allusdt_24h_disable_threshold_percent",
        ):
            if name not in columns:
                conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN {name} REAL NOT NULL DEFAULT 10.0")
                conn.execute(
                    f"UPDATE {SETTINGS_TABLE_NAME} SET {name} = allusdt_24h_disable_threshold_percent"
                )
                columns.add(name)
        for name in DEFAULT_SETTINGS:
            if name not in columns and name not in ("enabled", "simulation_enabled", "live_enabled", "high_simulation_enabled", "high_live_enabled", "allusdt_24h_rise_threshold_percent", "allusdt_24h_disable_threshold_percent"):
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
