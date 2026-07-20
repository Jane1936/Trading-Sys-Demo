"""Persistent runtime feature switches shared by web and worker processes."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Iterable

import db_config

BASE_DATA_COLLECTION = "base_data_collection"
SCORING_SYSTEM = "scoring_system"
TRADING_SYSTEM = "trading_system"
MARKET_FILTER = "market_filter"


@dataclass(frozen=True)
class FeatureFlagDefinition:
    key: str
    name: str
    description: str


@dataclass(frozen=True)
class FeatureFlag:
    key: str
    name: str
    description: str
    enabled: bool
    updated_at: int


FEATURE_FLAG_DEFINITIONS: tuple[FeatureFlagDefinition, ...] = (
    FeatureFlagDefinition(
        key=BASE_DATA_COLLECTION,
        name="基础数据收集",
        description="控制 K线/OI/funding/BTC 5m、ATR、MA/EMA/MACD 等基础数据采集与处理。",
    ),
    FeatureFlagDefinition(
        key=SCORING_SYSTEM,
        name="评分系统",
        description="控制异常插针、冷却、评分规则、可开仓列表、动态开仓门槛等评分链路。",
    ),
    FeatureFlagDefinition(
        key=TRADING_SYSTEM,
        name="交易系统",
        description="关闭后不再开新仓；已有仓位止损、止盈、风控保护默认继续运行。",
    ),
    FeatureFlagDefinition(
        key=MARKET_FILTER,
        name="市场行情过滤",
        description="控制独立市场行情过滤模块及新开仓前的市场过滤拦截。",
    ),
)

_DEFINITIONS_BY_KEY = {definition.key: definition for definition in FEATURE_FLAG_DEFINITIONS}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _connect(db_path: str) -> sqlite3.Connection:
    return db_config.connect_sqlite(db_path, row_factory=sqlite3.Row)


def init_feature_flags(db_path: str | None = None) -> None:
    """Create and seed the feature flag table with all switches enabled by default."""
    db_path = db_path or db_config.BASE_DB_PATH
    with db_config.sqlite_schema_lock(db_path):
        with _connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_flags (
                    key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            now_ms = _now_ms()
            for definition in FEATURE_FLAG_DEFINITIONS:
                conn.execute(
                    """
                    INSERT INTO feature_flags (key, name, description, enabled, updated_at)
                    VALUES (?, ?, ?, 1, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        name = excluded.name,
                        description = excluded.description
                    """,
                    (definition.key, definition.name, definition.description, now_ms),
                )
            conn.commit()


def list_feature_flags(db_path: str | None = None) -> list[FeatureFlag]:
    db_path = db_path or db_config.BASE_DB_PATH
    init_feature_flags(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT key, name, description, enabled, updated_at
            FROM feature_flags
            ORDER BY CASE key
                WHEN ? THEN 1
                WHEN ? THEN 2
                WHEN ? THEN 3
                WHEN ? THEN 4
                ELSE 99
            END, key
            """,
            (BASE_DATA_COLLECTION, SCORING_SYSTEM, TRADING_SYSTEM, MARKET_FILTER),
        ).fetchall()
    return [
        FeatureFlag(
            key=str(row["key"]),
            name=str(row["name"]),
            description=str(row["description"]),
            enabled=bool(int(row["enabled"])),
            updated_at=int(row["updated_at"] or 0),
        )
        for row in rows
    ]


def get_feature_flag(key: str, db_path: str | None = None) -> FeatureFlag:
    for flag in list_feature_flags(db_path):
        if flag.key == key:
            return flag
    raise KeyError(f"Unknown feature flag: {key}")


def set_feature_flag(key: str, enabled: bool, db_path: str | None = None) -> FeatureFlag:
    db_path = db_path or db_config.BASE_DB_PATH
    definition = _DEFINITIONS_BY_KEY.get(key)
    if definition is None:
        raise KeyError(f"Unknown feature flag: {key}")
    init_feature_flags(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            """
            UPDATE feature_flags
            SET enabled = ?, name = ?, description = ?, updated_at = ?
            WHERE key = ?
            """,
            (1 if enabled else 0, definition.name, definition.description, _now_ms(), key),
        )
        conn.commit()
    return get_feature_flag(key, db_path)


def is_feature_enabled(key: str, db_path: str | None = None) -> bool:
    try:
        return get_feature_flag(key, db_path).enabled
    except Exception as exc:
        print(f"⚠️ feature flag lookup failed key={key}: {exc}; defaulting to enabled")
        return True


def flags_to_dict(flags: Iterable[FeatureFlag]) -> list[dict]:
    return [
        {
            "key": flag.key,
            "name": flag.name,
            "description": flag.description,
            "enabled": flag.enabled,
            "updated_at": flag.updated_at,
        }
        for flag in flags
    ]
