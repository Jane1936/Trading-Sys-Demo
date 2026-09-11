"""Persistent simulated/live total position margin budget settings."""
from __future__ import annotations
import sqlite3, time
from decimal import Decimal
import db_config
DEFAULT_SETTINGS = {"simulation_max_margin_cost_usdt": Decimal("1000"), "live_max_margin_cost_usdt": Decimal("100")}
TABLE_NAME = "margin_budget_settings"
def _validate(payload: dict) -> dict[str, Decimal]:
    try: values = {k: Decimal(str(payload[k])) for k in DEFAULT_SETTINGS}
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc: raise ValueError("模拟盘和实盘最大保证金成本均为必填数字") from exc
    if any(not v.is_finite() or v <= 0 for v in values.values()): raise ValueError("最大保证金成本必须是大于 0 的有限数字")
    return values
def get_settings(db_path=None):
    path=db_path or db_config.CONFIG_DB_PATH
    with db_config.connect_sqlite(path,row_factory=sqlite3.Row) as c:
        c.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id INTEGER PRIMARY KEY CHECK(id=1), simulation_max_margin_cost_usdt TEXT NOT NULL, live_max_margin_cost_usdt TEXT NOT NULL, updated_at INTEGER NOT NULL)")
        c.execute(f"INSERT OR IGNORE INTO {TABLE_NAME} VALUES (1,?,?,?)", tuple(str(v) for v in DEFAULT_SETTINGS.values())+(int(time.time()*1000),))
        row=c.execute(f"SELECT simulation_max_margin_cost_usdt,live_max_margin_cost_usdt FROM {TABLE_NAME} WHERE id=1").fetchone(); c.commit()
    return {k: Decimal(str(row[k])) for k in DEFAULT_SETTINGS}
def set_settings(payload, db_path=None):
    values=_validate(payload); path=db_path or db_config.CONFIG_DB_PATH; get_settings(path)
    with db_config.connect_sqlite(path) as c: c.execute(f"UPDATE {TABLE_NAME} SET simulation_max_margin_cost_usdt=?,live_max_margin_cost_usdt=?,updated_at=? WHERE id=1", tuple(str(v) for v in values.values())+(int(time.time()*1000),)); c.commit()
    return values
