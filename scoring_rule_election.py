"""Runtime configuration for scoring-rule opening elections."""

from __future__ import annotations

import sqlite3
import time

import db_config
from scoring_system import DEFAULT_RULE_SCORE_WEIGHTS, RULE_SCORE_NAMES


RULE_STATUSES = ("required", "optional", "ignored")
DEFAULT_STATUS = "ignored"


def init_settings(db_path: str | None = None) -> None:
    db_path = db_path or db_config.CONFIG_DB_PATH
    with db_config.sqlite_schema_lock(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scoring_rule_election (
                    rule_id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scoring_rule_election_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    optional_min INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            now_ms = int(time.time() * 1000)
            conn.executemany(
                "INSERT INTO scoring_rule_election (rule_id, status, updated_at) VALUES (?, ?, ?) ON CONFLICT(rule_id) DO NOTHING",
                [(rule_id, DEFAULT_STATUS, now_ms) for rule_id in DEFAULT_RULE_SCORE_WEIGHTS],
            )
            conn.execute(
                "INSERT INTO scoring_rule_election_config (id, optional_min, updated_at) VALUES (1, 0, ?) ON CONFLICT(id) DO NOTHING",
                (now_ms,),
            )


def get_settings(db_path: str | None = None) -> dict:
    db_path = db_path or db_config.CONFIG_DB_PATH
    init_settings(db_path)
    with db_config.connect_sqlite(db_path, row_factory=sqlite3.Row) as conn:
        rows = conn.execute(
            "SELECT rule_id, status, updated_at FROM scoring_rule_election ORDER BY rule_id"
        ).fetchall()
        config = conn.execute(
            "SELECT optional_min, updated_at FROM scoring_rule_election_config WHERE id = 1"
        ).fetchone()
    return {
        "rules": [
            {
                "rule_id": int(row["rule_id"]),
                "name": RULE_SCORE_NAMES[int(row["rule_id"])],
                "status": row["status"],
                "updated_at": int(row["updated_at"]),
            }
            for row in rows
        ],
        "optional_min": int(config["optional_min"]),
        "updated_at": int(config["updated_at"]),
    }


def set_settings(payload: dict, db_path: str | None = None) -> dict:
    raw_rules = payload.get("rules")
    if not isinstance(raw_rules, list) or len(raw_rules) != 18:
        raise ValueError("rules must contain all 18 scoring rules")
    try:
        statuses = {int(item["rule_id"]): item["status"] for item in raw_rules}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Each rule requires a valid rule_id and status") from exc
    if set(statuses) != set(DEFAULT_RULE_SCORE_WEIGHTS):
        raise ValueError("rules must contain each rule_id from 1 through 18 exactly once")
    if any(status not in RULE_STATUSES for status in statuses.values()):
        raise ValueError("status must be required, optional, or ignored")
    optional_min = payload.get("optional_min")
    if isinstance(optional_min, bool) or not isinstance(optional_min, int):
        raise ValueError("optional_min must be an integer")
    optional_count = sum(status == "optional" for status in statuses.values())
    if not 0 <= optional_min <= optional_count:
        raise ValueError("optional_min must be between 0 and the number of optional rules")

    db_path = db_path or db_config.CONFIG_DB_PATH
    init_settings(db_path)
    now_ms = int(time.time() * 1000)
    with db_config.connect_sqlite(db_path) as conn:
        conn.executemany(
            "UPDATE scoring_rule_election SET status = ?, updated_at = ? WHERE rule_id = ?",
            [(status, now_ms, rule_id) for rule_id, status in statuses.items()],
        )
        conn.execute(
            "UPDATE scoring_rule_election_config SET optional_min = ?, updated_at = ? WHERE id = 1",
            (optional_min, now_ms),
        )
    return get_settings(db_path)
