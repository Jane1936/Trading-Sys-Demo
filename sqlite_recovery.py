"""SQLite database health checks and automatic quarantine helpers."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Iterable

_checked_database_paths: set[str] = set()


def is_malformed_database_error(exc: BaseException) -> bool:
    """Return True when SQLite reports an unrecoverable malformed database image."""
    message = str(exc).lower()
    return "database disk image is malformed" in message or "file is not a database" in message


def quarantine_sqlite_database(db_path: str) -> list[str]:
    """Move a corrupt SQLite database and its WAL sidecar files out of the way.

    The files are renamed instead of deleted so operators can inspect or recover
    data manually.  Returns the paths created by the quarantine operation.
    """
    quarantined: list[str] = []
    path = Path(db_path)
    timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if not candidate.exists():
            continue
        target = candidate.with_name(f"{candidate.name}.corrupt-{timestamp}")
        counter = 1
        while target.exists():
            target = candidate.with_name(f"{candidate.name}.corrupt-{timestamp}.{counter}")
            counter += 1
        candidate.rename(target)
        quarantined.append(str(target))

    if path.parent and str(path.parent) != ".":
        os.makedirs(path.parent, exist_ok=True)
    return quarantined


def ensure_sqlite_database_usable(
    db_path: str, *, quick_check: bool = False, once_per_process: bool = True
) -> list[str]:
    """Optionally quarantine a malformed SQLite database before initialization.

    Startup callers skip ``PRAGMA quick_check`` by default to avoid scanning large
    database files on every deploy.  Callers that need proactive corruption
    detection can pass ``quick_check=True``; repeated checks for the same path are
    skipped within the current process unless ``once_per_process`` is false.
    A missing or empty database is considered usable because SQLite/table
    initialization code can create the schema on demand.
    """
    if not db_path:
        return []

    if not quick_check:
        return []

    normalized_path = str(Path(db_path).resolve())
    if once_per_process and normalized_path in _checked_database_paths:
        return []

    if once_per_process:
        _checked_database_paths.add(normalized_path)

    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return []

    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            result = conn.execute("PRAGMA quick_check").fetchone()
            if result and str(result[0]).lower() == "ok":
                return []
    except sqlite3.DatabaseError as exc:
        if is_malformed_database_error(exc):
            return quarantine_sqlite_database(db_path)
        raise

    return quarantine_sqlite_database(db_path)


def quick_check_sqlite_database(db_path: str) -> tuple[bool, str]:
    """Run PRAGMA quick_check and return (is_ok, detail) without mutating files."""
    if not db_path:
        return True, "empty path"
    if not os.path.exists(db_path):
        return True, "missing; will be created on demand"
    if os.path.getsize(db_path) == 0:
        return True, "empty; will be initialized on demand"
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
    except sqlite3.DatabaseError as exc:
        return False, str(exc)
    detail = str(row[0]) if row else "no quick_check result"
    return detail.lower() == "ok", detail


def quarantine_malformed_sqlite_databases(
    db_paths: Iterable[str],
) -> dict[str, list[str]]:
    """Quarantine every malformed SQLite DB in ``db_paths``.

    This is intended for dashboard/runtime recovery paths where an exception may
    come from either the main module database or an attached companion database.
    """
    quarantined_by_path: dict[str, list[str]] = {}
    seen: set[str] = set()
    for db_path in db_paths:
        if not db_path:
            continue
        normalized = str(Path(db_path).resolve())
        if normalized in seen:
            continue
        seen.add(normalized)
        ok, detail = quick_check_sqlite_database(db_path)
        if ok:
            continue
        detail_lower = detail.lower()
        if "database is locked" in detail_lower or "busy" in detail_lower:
            continue
        if is_malformed_database_error(Exception(detail)) or detail:
            quarantined = quarantine_sqlite_database(db_path)
            quarantined_by_path[db_path] = quarantined
    return quarantined_by_path
