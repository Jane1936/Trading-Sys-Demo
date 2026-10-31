"""Independent simulation/live percentage thresholds for price-gain take-profit."""
from __future__ import annotations
import math, sqlite3, time
import db_config

SETTINGS_TABLE_NAME = "price_gain_take_profit_settings"
DEFAULT_SIMULATION_PROFIT_RATIO = 0.20
DEFAULT_LIVE_PROFIT_RATIO = 0.20

def _validate(payload):
    if not isinstance(payload, dict): raise ValueError("必须提供模拟盘和实盘涨幅止盈比例")
    allowed = {"simulation_profit_ratio", "live_profit_ratio"}
    if set(payload) - allowed: raise ValueError("涨幅止盈配置包含未知字段")
    if not allowed <= set(payload): raise ValueError("必须提供模拟盘和实盘涨幅止盈比例")
    values = {}
    for key in allowed:
        try: value = float(payload[key])
        except (TypeError, ValueError) as exc: raise ValueError("涨幅止盈比例必须是数字") from exc
        if not math.isfinite(value) or value <= 0 or value > 1: raise ValueError("涨幅止盈比例必须大于0%且不超过100%")
        values[key] = value
    return values

def get_settings(db_path=None):
    path = db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path, row_factory=sqlite3.Row) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE_NAME} (id INTEGER PRIMARY KEY CHECK(id=1), simulation_profit_ratio REAL NOT NULL DEFAULT 0.2, live_profit_ratio REAL NOT NULL DEFAULT 0.2, updated_at INTEGER NOT NULL)")
        conn.execute(f"INSERT OR IGNORE INTO {SETTINGS_TABLE_NAME} (id, updated_at) VALUES (1, ?)", (int(time.time()*1000),))
        row = conn.execute(f"SELECT simulation_profit_ratio, live_profit_ratio FROM {SETTINGS_TABLE_NAME} WHERE id=1").fetchone()
    return {"simulation_profit_ratio": float(row["simulation_profit_ratio"]), "live_profit_ratio": float(row["live_profit_ratio"])}

def set_settings(payload, db_path=None):
    values = _validate(payload); path = db_path or db_config.CONFIG_DB_PATH; get_settings(path)
    with db_config.connect_sqlite(path) as conn:
        conn.execute(f"UPDATE {SETTINGS_TABLE_NAME} SET simulation_profit_ratio=?, live_profit_ratio=?, updated_at=? WHERE id=1", (values["simulation_profit_ratio"], values["live_profit_ratio"], int(time.time()*1000)))
    return get_settings(path)
