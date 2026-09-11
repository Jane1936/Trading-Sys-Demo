"""Persistent configuration for the independent market filter."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


DEFAULT_BTC_SIPHON_THRESHOLD = 0.005
DEFAULT_MARKET_CRASH_THRESHOLD = 0.03
DEFAULT_ALLUSDT_24H_DROP_THRESHOLD = -0.05
DEFAULT_BLOCK_DURATION_MINUTES = 30
SETTINGS_TABLE_NAME = "market_filter_settings"


def get_settings(db_path: str | None = None) -> dict[str, float | int]:
    """Return the current thresholds and blocking duration."""
    settings_path = db_path or db_config.CONFIG_DB_PATH
    with db_config.sqlite_schema_lock(settings_path):
        with db_config.connect_sqlite(settings_path, row_factory=sqlite3.Row) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    btc_siphon_threshold REAL NOT NULL,
                    market_crash_threshold REAL NOT NULL,
                    allusdt_24h_drop_threshold REAL NOT NULL DEFAULT -0.05,
                    block_duration_minutes INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            columns = {
                row["name"]
                for row in conn.execute(f"PRAGMA table_info({SETTINGS_TABLE_NAME})")
            }
            if "allusdt_24h_drop_threshold" not in columns:
                conn.execute(
                    f"ALTER TABLE {SETTINGS_TABLE_NAME} "
                    "ADD COLUMN allusdt_24h_drop_threshold REAL NOT NULL DEFAULT -0.05"
                )
            conn.execute(f"""
                INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME}
                (id, btc_siphon_threshold, market_crash_threshold,
                 allusdt_24h_drop_threshold, block_duration_minutes, updated_at)
                VALUES (1, ?, ?, ?, ?, ?)
            """, (
                DEFAULT_BTC_SIPHON_THRESHOLD,
                DEFAULT_MARKET_CRASH_THRESHOLD,
                DEFAULT_ALLUSDT_24H_DROP_THRESHOLD,
                DEFAULT_BLOCK_DURATION_MINUTES,
                int(time.time() * 1000),
            ))
            row = conn.execute(f"SELECT * FROM {SETTINGS_TABLE_NAME} WHERE id = 1").fetchone()
    return {
        "btc_siphon_threshold": float(row["btc_siphon_threshold"]),
        "market_crash_threshold": float(row["market_crash_threshold"]),
        "allusdt_24h_drop_threshold": float(row["allusdt_24h_drop_threshold"]),
        "block_duration_minutes": int(row["block_duration_minutes"]),
        "updated_at": int(row["updated_at"]),
    }


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, float | int]:
    """Validate and persist independent market-filter configuration."""
    settings_path = db_path or db_config.CONFIG_DB_PATH
    current = get_settings(settings_path)
    try:
        btc_threshold = float(payload["btc_siphon_threshold"])
        crash_threshold = float(payload["market_crash_threshold"])
        allusdt_24h_threshold = float(payload.get(
            "allusdt_24h_drop_threshold", current["allusdt_24h_drop_threshold"]
        ))
        duration_raw = payload["block_duration_minutes"]
        duration = int(duration_raw)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("BTC吸血阈值、大盘暴跌阈值和禁止开仓时间必须是数字") from exc
    if isinstance(duration_raw, bool) or float(duration_raw) != duration:
        raise ValueError("禁止开仓时间必须是整数分钟")
    if not math.isfinite(btc_threshold) or not 0 < btc_threshold <= 1:
        raise ValueError("BTC吸血阈值必须大于 0% 且不超过 100%")
    if not math.isfinite(crash_threshold) or not 0 < crash_threshold <= 1:
        raise ValueError("大盘暴跌阈值必须大于 0% 且不超过 100%")
    if not math.isfinite(allusdt_24h_threshold) or not -1 <= allusdt_24h_threshold < 0:
        raise ValueError("ALLUSDT 最近24h涨跌幅阈值必须不低于 -100% 且小于 0%")
    if not 1 <= duration <= 10_080:
        raise ValueError("禁止开仓时间必须为 1–10080 分钟")

    updated_at = int(time.time() * 1000)
    with db_config.connect_sqlite(settings_path) as conn:
        conn.execute(f"""
            UPDATE {SETTINGS_TABLE_NAME}
            SET btc_siphon_threshold = ?, market_crash_threshold = ?,
                allusdt_24h_drop_threshold = ?, block_duration_minutes = ?, updated_at = ?
            WHERE id = 1
        """, (btc_threshold, crash_threshold, allusdt_24h_threshold, duration, updated_at))
    return get_settings(settings_path)
