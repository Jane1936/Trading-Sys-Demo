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
TRAILING_STOP = "trailing_stop"
STOP_LOSS_RULE = "stop_loss_rule"
REDUCTION_CONDITIONS = "reduction_conditions"
INCREASE_CONDITIONS = "increase_conditions"
PORTFOLIO_RISK = "portfolio_risk"
BREAK_EVEN_TAKE_PROFIT = "break_even_take_profit"
PARTIAL_TAKE_PROFIT = "partial_take_profit"
TRAILING_REDUCTION = "trailing_reduction"
DYNAMIC_PROFIT_PROTECTION = "dynamic_profit_protection"

PRIMARY_FEATURE_FLAGS = frozenset({BASE_DATA_COLLECTION, SCORING_SYSTEM, TRADING_SYSTEM, MARKET_FILTER})


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
        name="模拟盘交易系统",
        description="仅控制模拟盘交易；关闭后模拟盘不再开新仓，已有模拟盘仓位的止损、止盈、风控保护默认继续运行。",
    ),
    FeatureFlagDefinition(
        key=MARKET_FILTER,
        name="市场行情过滤",
        description="控制独立市场行情过滤模块及新开仓前的市场过滤拦截。",
    ),
    FeatureFlagDefinition(
        key=TRAILING_STOP,
        name="模拟盘移动追踪止盈规则",
        description="控制移动追踪止盈的每分钟扫描及平仓操作；关闭后不再执行该规则。",
    ),
    FeatureFlagDefinition(STOP_LOSS_RULE, "模拟盘止损规则", "控制持仓结构止损判断及平仓操作。"),
    FeatureFlagDefinition(REDUCTION_CONDITIONS, "模拟盘减仓条件模块", "控制持仓评分减仓条件判断及减仓操作。"),
    FeatureFlagDefinition(INCREASE_CONDITIONS, "模拟盘加仓条件模块", "控制持仓评分加仓条件判断及加仓操作。"),
    FeatureFlagDefinition(PORTFOLIO_RISK, "模拟盘组合风险约束", "控制持仓组合风险计算与约束数据更新。"),
    FeatureFlagDefinition(BREAK_EVEN_TAKE_PROFIT, "模拟盘保本止盈策略", "控制保本止盈的每分钟扫描及平仓操作。"),
    FeatureFlagDefinition(PARTIAL_TAKE_PROFIT, "模拟盘分批止盈规则", "控制分批止盈的每分钟扫描及减仓操作。"),
    FeatureFlagDefinition(TRAILING_REDUCTION, "模拟盘移动追踪减仓", "控制移动追踪减仓判断、刷新及减仓操作。"),
    FeatureFlagDefinition(DYNAMIC_PROFIT_PROTECTION, "模拟盘动态利润保护模块", "控制动态利润保护的每分钟扫描及平仓操作。"),
)

_DEFINITIONS_BY_KEY = {definition.key: definition for definition in FEATURE_FLAG_DEFINITIONS}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _connect(db_path: str) -> sqlite3.Connection:
    return db_config.connect_sqlite(db_path, row_factory=sqlite3.Row)


def _feature_flags_are_current(db_path: str) -> bool:
    """Return whether the feature-flag schema and definitions are already seeded.

    This intentionally uses ordinary SQLite reads rather than the cross-process
    schema lock.  Web requests call ``init_feature_flags`` frequently, and
    waiting on a lock held by an unrelated migration can exceed Gunicorn's
    worker timeout even though this table needs no initialization.
    """
    with _connect(db_path) as conn:
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'feature_flags'"
        ).fetchone()
        if table_exists is None:
            return False
        rows = conn.execute(
            "SELECT key, name, description FROM feature_flags"
        ).fetchall()

    actual = {
        str(row["key"]): (str(row["name"]), str(row["description"])) for row in rows
    }
    expected = {
        definition.key: (definition.name, definition.description)
        for definition in FEATURE_FLAG_DEFINITIONS
    }
    return all(actual.get(key) == value for key, value in expected.items())


def init_feature_flags(db_path: str | None = None) -> None:
    """Create and seed the feature flag table with all switches enabled by default."""
    db_path = db_path or db_config.BASE_DB_PATH
    if _feature_flags_are_current(db_path):
        return
    with db_config.sqlite_schema_lock(db_path):
        # Another process may have completed initialization while this process
        # was waiting.  Recheck to avoid unnecessary DDL and writes.
        if _feature_flags_are_current(db_path):
            return
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
                WHEN 'base_data_collection' THEN 1 WHEN 'scoring_system' THEN 2
                WHEN 'trading_system' THEN 3 WHEN 'market_filter' THEN 4
                WHEN 'stop_loss_rule' THEN 5 WHEN 'reduction_conditions' THEN 6
                WHEN 'increase_conditions' THEN 7 WHEN 'portfolio_risk' THEN 8
                WHEN 'break_even_take_profit' THEN 9 WHEN 'partial_take_profit' THEN 10
                WHEN 'trailing_reduction' THEN 11 WHEN 'trailing_stop' THEN 12
                WHEN 'dynamic_profit_protection' THEN 13 ELSE 99 END, key
            """,
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
            "primary": flag.key in PRIMARY_FEATURE_FLAGS,
        }
        for flag in flags
    ]
