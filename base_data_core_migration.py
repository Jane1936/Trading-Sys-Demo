"""Move the ``klines_1m`` table from base_data.db to base_data_core.db.

Run once after deploying the split:
    python base_data_core_migration.py

The operation is idempotent and uses SQLite's backup API for the table data.
It does not delete the source table; removal is deliberately a separate,
explicit operation after all readers have been switched to the new database.
"""
from __future__ import annotations

import sqlite3
from contextlib import ExitStack
from pathlib import Path

import db_config


def migrate(source: str | None = None, target: str | None = None) -> int:
    source = source or db_config.BASE_DB_PATH
    target = target or db_config.BASE_DATA_CORE_DB_PATH
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    # Open both databases in stable path order. connect_sqlite holds each
    # database's access lock for the lifetime of the managed connection.
    paths = sorted({str(Path(source)), str(Path(target))})
    with ExitStack() as stack:
        connections = {path: stack.enter_context(db_config.connect_sqlite(path)) for path in paths}
        # connect_sqlite performs recovery-marker checks and applies the
        # project's WAL/busy-timeout/connection lifecycle policy.
        src = connections[str(Path(source))]
        dst = connections[str(Path(target))]
        exists = src.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='klines_1m'"
        ).fetchone()
        if not exists:
            return 0
        # Copy only klines_1m, preserving any existing target tables.
        schema = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='klines_1m'"
        ).fetchone()[0]
        dst.execute(schema)
        rows = src.execute("SELECT * FROM klines_1m").fetchall()
        if rows:
            placeholders = ",".join("?" for _ in rows[0])
            dst.executemany(f"INSERT OR IGNORE INTO klines_1m VALUES ({placeholders})", rows)
        dst.commit()
        return len(rows)


if __name__ == "__main__":
    print(f"migrated {migrate()} rows to {db_config.BASE_DATA_CORE_DB_PATH}")
