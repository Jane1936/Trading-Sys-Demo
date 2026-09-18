"""Persistent zombie force-liquidation configuration."""
from db_config import connect_sqlite as connect_config_database

DEFAULTS = {"holding_hours": 24.0, "allusdt_rise_threshold_percent": 4.0, "high_rise_holding_hours": 12.0}

def _ensure(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS zombie_force_liquidation_settings (id INTEGER PRIMARY KEY CHECK (id=1), holding_hours REAL NOT NULL DEFAULT 24, allusdt_rise_threshold_percent REAL NOT NULL DEFAULT 4, high_rise_holding_hours REAL NOT NULL DEFAULT 12)")
    for name, default in DEFAULTS.items():
        if name not in {r[1] for r in conn.execute("PRAGMA table_info(zombie_force_liquidation_settings)")}: conn.execute(f"ALTER TABLE zombie_force_liquidation_settings ADD COLUMN {name} REAL NOT NULL DEFAULT {default}")

def get_settings(db_path):
    with connect_config_database(db_path) as conn:
        _ensure(conn); row = conn.execute("SELECT holding_hours, allusdt_rise_threshold_percent, high_rise_holding_hours FROM zombie_force_liquidation_settings WHERE id=1").fetchone()
        if row is None: conn.execute("INSERT INTO zombie_force_liquidation_settings (id, holding_hours, allusdt_rise_threshold_percent, high_rise_holding_hours) VALUES (1,?,?,?)", tuple(DEFAULTS.values())); return dict(DEFAULTS)
        return {"holding_hours": float(row[0]), "allusdt_rise_threshold_percent": float(row[1]), "high_rise_holding_hours": float(row[2])}

def set_settings(payload, db_path):
    current = get_settings(db_path)
    values = {k: float(payload.get(k, current[k])) for k in DEFAULTS}
    if values["holding_hours"] <= 0 or values["high_rise_holding_hours"] <= 0 or values["allusdt_rise_threshold_percent"] < 0: raise ValueError("配置值必须为正数，涨幅阈值不可小于0")
    with connect_config_database(db_path) as conn:
        _ensure(conn); conn.execute("INSERT INTO zombie_force_liquidation_settings (id,holding_hours,allusdt_rise_threshold_percent,high_rise_holding_hours) VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET holding_hours=excluded.holding_hours, allusdt_rise_threshold_percent=excluded.allusdt_rise_threshold_percent, high_rise_holding_hours=excluded.high_rise_holding_hours", tuple(values.values()))
    return values
