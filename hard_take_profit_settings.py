"""Persistent runtime settings for the simulated hard take-profit module."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


SETTINGS_TABLE_NAME = "hard_take_profit_settings"
DEFAULT_PROFIT_RATIO = 0.20


def _validate_settings(payload: dict) -> dict[str, float]:
    if not isinstance(payload, dict) or set(payload) != {"profit_ratio"}:
        raise ValueError("必须提供硬止盈幅度")
    try:
        profit_ratio = float(payload["profit_ratio"])
    except (TypeError, ValueError) as exc:
        raise ValueError("硬止盈幅度必须是数字") from exc
    if not math.isfinite(profit_ratio) or profit_ratio <= 0 or profit_ratio > 1:
        raise ValueError("硬止盈幅度必须大于0%且不超过100%")
    return {"profit_ratio": profit_ratio}


def get_settings(db_path: str | None = None) -> dict[str, float]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                profit_ratio REAL NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        conn.execute(
            f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} VALUES (1, ?, ?)",
            (DEFAULT_PROFIT_RATIO, int(time.time() * 1000)),
        )
        row = conn.execute(
            f"SELECT profit_ratio FROM {SETTINGS_TABLE_NAME} WHERE id = 1"
        ).fetchone()
        conn.commit()
    return {"profit_ratio": float(row["profit_ratio"])}


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, float]:
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"UPDATE {SETTINGS_TABLE_NAME} SET profit_ratio = ?, updated_at = ? WHERE id = 1",
            (settings["profit_ratio"], int(time.time() * 1000)),
        )
    return get_settings(path)
