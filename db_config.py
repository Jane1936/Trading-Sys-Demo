"""Central SQLite database layout.

The system uses four independent SQLite files so a corrupted/deleted module DB
only disables that module instead of taking down the whole app.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

DATA_DIR = os.getenv("DATA_DIR", "data")
BASE_DB_PATH = os.getenv("BASE_DB_PATH", os.getenv("DB_PATH", f"{DATA_DIR}/base_data.db"))
SCORING_DB_PATH = os.getenv("SCORING_DB_PATH", f"{DATA_DIR}/scoring.db")
TRADING_DB_PATH = os.getenv("TRADING_DB_PATH", f"{DATA_DIR}/trading.db")
MARKET_DB_PATH = os.getenv("MARKET_DB_PATH", f"{DATA_DIR}/market.db")

DB_LABELS = {
    "基础数据库": BASE_DB_PATH,
    "评分系统数据库": SCORING_DB_PATH,
    "交易数据库": TRADING_DB_PATH,
    "市场行情数据库": MARKET_DB_PATH,
}


def ensure_parent_dir(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def attach_databases(conn: sqlite3.Connection, attachments: Iterable[tuple[str, str]]) -> None:
    """Attach readable companion databases if they are distinct from main.

    SQLite resolves unqualified table names through temp, main, then attached
    schemas, which lets module-owned tables live in main while read-only source
    tables can be found in their own database files.
    """
    main_path = Path(conn.execute("PRAGMA database_list").fetchone()[2] or "").resolve()
    seen = {"main"}
    for schema, path in attachments:
        if not path or schema in seen:
            continue
        ensure_parent_dir(path)
        try:
            if Path(path).resolve() == main_path:
                continue
        except OSError:
            pass
        conn.execute(f"ATTACH DATABASE ? AS {quote_identifier(schema)}", (path,))
        seen.add(schema)


class sqlite_schema_lock:
    """Cross-process guard for SQLite schema migrations.

    SQLite serializes writers, but concurrent process startup can still race on
    check-then-ALTER migration code.  This file lock ensures only one process
    performs DDL for a database at a time, preventing duplicate-column failures
    and avoiding interrupted competing schema writes.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._fh = None

    def __enter__(self):
        ensure_parent_dir(self.db_path)
        lock_path = f"{self.db_path}.schema.lock"
        self._fh = open(lock_path, "a+")
        if os.name == "posix":
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fh is None:
            return False
        if os.name == "posix":
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None
        return False


def configure_sqlite_connection(conn: sqlite3.Connection, *, wal: bool = True) -> sqlite3.Connection:
    """Apply SQLite settings used by concurrent workers."""
    conn.execute("PRAGMA busy_timeout=30000;")
    if wal:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def connect_sqlite(db_path: str, *, timeout: int = 30, row_factory=None, wal: bool = True) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path, timeout=timeout)
    if row_factory is not None:
        conn.row_factory = row_factory
    return configure_sqlite_connection(conn, wal=wal)
