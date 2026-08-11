"""Persistent configuration for stop-distance tiers and score/tier leverage mapping."""
from __future__ import annotations

import json
import time

import db_config

DEFAULT_SETTINGS = {
    "tier_min_percent": 0.0,
    "tier_max_percent": {"A档": 2.0, "B档": 3.0, "C档": 4.0},
    "bands": [
        {"label": "低档试错单", "lower": 67, "upper": 72, "distance_threshold_percent": 5, "leverages": {"A档": 4, "B档": 3, "C档": 2, "D档": 1}},
        {"label": "标准试错单", "lower": 73, "upper": 80, "distance_threshold_percent": 6, "leverages": {"A档": 8, "B档": 6, "C档": 4, "D档": 2}},
        {"label": "趋势标准单", "lower": 81, "upper": 88, "distance_threshold_percent": 7, "leverages": {"A档": 10, "B档": 7, "C档": 5, "D档": 3}},
        {"label": "确定性强趋势单", "lower": 89, "upper": 100, "distance_threshold_percent": 8, "leverages": {"A档": 12, "B档": 8, "C档": 6, "D档": 4}},
    ],
}


def _validate(settings: dict) -> dict:
    tiers = settings.get("tier_max_percent")
    bands = settings.get("bands")
    if not isinstance(tiers, dict) or not isinstance(bands, list) or len(bands) != 4:
        raise ValueError("止损距离档位和四个总分区间均为必填项")
    tier_min = float(settings.get("tier_min_percent", 0))
    limits = [float(tiers.get(key, 0)) for key in ("A档", "B档", "C档")]
    if not (0 <= tier_min < limits[0] < limits[1] < limits[2] <= 100):
        raise ValueError("A 档下限及 A/B/C 档上限必须依次递增，且在 0–100% 之间")
    normalized = {
        "tier_min_percent": tier_min,
        "tier_max_percent": dict(zip(("A档", "B档", "C档"), limits)),
        "bands": [],
    }
    previous_upper = None
    for raw in bands:
        lower, upper = int(raw["lower"]), int(raw["upper"])
        threshold = float(raw["distance_threshold_percent"])
        if lower < 0 or upper > 100 or lower > upper or (previous_upper is not None and lower != previous_upper + 1):
            raise ValueError("总分区间必须在 0–100 内连续且不重叠")
        leverages = {tier: int(raw["leverages"][tier]) for tier in ("A档", "B档", "C档", "D档")}
        if threshold <= 0 or threshold > 100 or any(value < 1 or value > 125 for value in leverages.values()):
            raise ValueError("筛查距离须在 0–100%，杠杆须在 1–125 倍")
        normalized["bands"].append({"label": str(raw["label"]), "lower": lower, "upper": upper, "distance_threshold_percent": threshold, "leverages": leverages})
        previous_upper = upper
    return normalized


def get_settings(db_path: str | None = None) -> dict:
    db_path = db_path or db_config.BASE_DB_PATH
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS runtime_settings (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at INTEGER NOT NULL)")
        row = conn.execute("SELECT value_json FROM runtime_settings WHERE key='openable_symbol_settings'").fetchone()
    return _validate(json.loads(row[0])) if row else _validate(DEFAULT_SETTINGS)


def set_settings(settings: dict, db_path: str | None = None) -> dict:
    db_path = db_path or db_config.BASE_DB_PATH
    value = _validate(settings)
    with db_config.connect_sqlite(db_path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS runtime_settings (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at INTEGER NOT NULL)")
        conn.execute("INSERT INTO runtime_settings VALUES ('openable_symbol_settings', ?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at", (json.dumps(value, ensure_ascii=False), int(time.time() * 1000)))
    return value
