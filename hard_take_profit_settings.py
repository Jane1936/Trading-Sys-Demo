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
    if not isinstance(payload, dict):
        raise ValueError("必须提供模拟盘和实盘硬止盈金额")
    allowed = {"profit_ratio", "simulation_hard_take_profit_usdt", "live_hard_take_profit_usdt"}
    if set(payload) - allowed:
        raise ValueError("硬止盈配置包含未知字段")
    # profit_ratio is retained for compatibility with the scanner, but the
    # settings form updates the two monetary thresholds only.
    if "profit_ratio" in payload:
        try:
            profit_ratio = float(payload["profit_ratio"])
        except (TypeError, ValueError) as exc:
            raise ValueError("硬止盈幅度必须是数字") from exc
        if not math.isfinite(profit_ratio) or profit_ratio <= 0 or profit_ratio > 1:
            raise ValueError("硬止盈幅度必须大于0%且不超过100%")
    if not {"simulation_hard_take_profit_usdt", "live_hard_take_profit_usdt"}.issubset(payload):
        if "profit_ratio" in payload:  # legacy callers may update only the ratio
            return {"profit_ratio": profit_ratio}
        raise ValueError("必须提供模拟盘和实盘硬止盈金额")
    values = {k: payload[k] for k in ("simulation_hard_take_profit_usdt", "live_hard_take_profit_usdt")}
    try: values = {k: float(v) for k, v in values.items()}
    except (TypeError, ValueError) as exc: raise ValueError("硬止盈金额必须是数字") from exc
    if any(not math.isfinite(v) or v <= 0 for v in values.values()): raise ValueError("硬止盈金额必须大于0")
    if "profit_ratio" in payload:
        values["profit_ratio"] = profit_ratio
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
    current = get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(
            f"UPDATE {SETTINGS_TABLE_NAME} SET profit_ratio = ?, simulation_hard_take_profit_usdt = ?, live_hard_take_profit_usdt = ?, updated_at = ? WHERE id = 1",
            (settings.get("profit_ratio", current["profit_ratio"]), settings.get("simulation_hard_take_profit_usdt", current["simulation_hard_take_profit_usdt"]), settings.get("live_hard_take_profit_usdt", current["live_hard_take_profit_usdt"]), int(time.time() * 1000)),
        )
    return get_settings(path)
