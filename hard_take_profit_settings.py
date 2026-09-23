"""Persistent runtime settings for the simulated hard take-profit module."""

from __future__ import annotations

import math
import sqlite3
import time

import db_config


SETTINGS_TABLE_NAME = "hard_take_profit_settings"
DEFAULT_PROFIT_RATIO = 0.20
DEFAULT_SIMULATION_HARD_TAKE_PROFIT_USDT = 55.0
DEFAULT_LIVE_HARD_TAKE_PROFIT_USDT = 10.0


def _validate_settings(payload: dict) -> dict[str, float]:
    if not isinstance(payload, dict) or set(payload) != {"simulation_hard_take_profit_usdt", "live_hard_take_profit_usdt"}:
        raise ValueError("必须提供模拟盘和实盘硬止盈金额")
    try:
        profit_ratio = float(payload["profit_ratio"])
    except (TypeError, ValueError) as exc:
        raise ValueError("硬止盈幅度必须是数字") from exc
    if not math.isfinite(profit_ratio) or profit_ratio <= 0 or profit_ratio > 1:
        raise ValueError("硬止盈幅度必须大于0%且不超过100%")
    values = {k: payload[k] for k in payload}
    try: values = {k: float(v) for k, v in values.items()}
    except (TypeError, ValueError) as exc: raise ValueError("硬止盈金额必须是数字") from exc
    if any(not math.isfinite(v) or v <= 0 for v in values.values()): raise ValueError("硬止盈金额必须大于0")
    return values


def get_settings(db_path: str | None = None) -> dict[str, float]:
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                profit_ratio REAL NOT NULL DEFAULT 0.2,
                simulation_hard_take_profit_usdt REAL NOT NULL DEFAULT 55,
                live_hard_take_profit_usdt REAL NOT NULL DEFAULT 10,
                updated_at INTEGER NOT NULL
            )
        """)
        conn.execute(
            f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} (id, profit_ratio, updated_at) VALUES (1, ?, ?)",
            (DEFAULT_PROFIT_RATIO, int(time.time() * 1000)),
        )
        conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN simulation_hard_take_profit_usdt REAL NOT NULL DEFAULT 55") if 'simulation_hard_take_profit_usdt' not in [r[1] for r in conn.execute(f'PRAGMA table_info({SETTINGS_TABLE_NAME})')] else None
        conn.execute(f"ALTER TABLE {SETTINGS_TABLE_NAME} ADD COLUMN live_hard_take_profit_usdt REAL NOT NULL DEFAULT 10") if 'live_hard_take_profit_usdt' not in [r[1] for r in conn.execute(f'PRAGMA table_info({SETTINGS_TABLE_NAME})')] else None
        row = conn.execute(
            f"SELECT profit_ratio, simulation_hard_take_profit_usdt, live_hard_take_profit_usdt FROM {SETTINGS_TABLE_NAME}"
        ).fetchone()
        conn.commit()
    return {"profit_ratio": float(row["profit_ratio"]), "simulation_hard_take_profit_usdt": float(row["simulation_hard_take_profit_usdt"]), "live_hard_take_profit_usdt": float(row["live_hard_take_profit_usdt"])}


def set_settings(payload: dict, db_path: str | None = None) -> dict[str, float]:
    settings = _validate_settings(payload)
    path = db_path or db_config.CONFIG_DB_PATH
    get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"UPDATE {SETTINGS_TABLE_NAME} SET simulation_hard_take_profit_usdt = ?, live_hard_take_profit_usdt = ?, updated_at = ? WHERE id = 1",
            (settings["simulation_hard_take_profit_usdt"], settings["live_hard_take_profit_usdt"], int(time.time() * 1000)),
        )
    return get_settings(path)
