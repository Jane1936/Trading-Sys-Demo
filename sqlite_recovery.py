"""SQLite database health checks and automatic quarantine helpers."""

from __future__ import annotations

import json
import os
import sqlite3
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

_checked_database_paths: set[str] = set()


def create_sqlite_failure_record(
    db_path: str,
    *,
    detail: str,
    source: str,
    exc: BaseException | None = None,
) -> str:
    """Persist evidence before a damaged database is moved or rebuilt.

    One JSON document is created for every detected failure.  It lives outside
    the database directory by default, so replacing a database cannot erase the
    evidence operators need for root-cause analysis.
    """
    from sqlite_diagnostics import inspect_database

    captured_at = datetime.now(timezone.utc)
    log_dir = Path(os.getenv("SQLITE_RECOVERY_LOG_DIR", "logs/sqlite-recovery"))
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(db_path).name.replace(os.sep, "_") or "sqlite"
    record_path = log_dir / (
        f"{captured_at.strftime('%Y%m%dT%H%M%S.%fZ')}-{safe_name}-{uuid.uuid4().hex[:8]}.json"
    )
    try:
        database_evidence = inspect_database(db_path)
    except Exception as evidence_exc:
        database_evidence = {
            "path": str(Path(db_path).resolve()),
            "evidence_collection_error": {
                "type": type(evidence_exc).__name__,
                "message": str(evidence_exc),
            },
        }
    record = {
        "captured_at_utc": captured_at.isoformat(),
        "source": source,
        "status": "detected",
        "pid": os.getpid(),
        "detail": detail,
        "exception": (
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": "".join(traceback.format_exception(exc)),
            }
            if exc is not None
            else None
        ),
        "database": database_evidence,
        "quarantined_files": [],
    }
    _write_failure_record(record_path, record)
    return str(record_path)


def finish_sqlite_failure_record(
    record_path: str,
    *,
    status: str,
    quarantined_files: Sequence[str] = (),
    error: BaseException | None = None,
) -> None:
    """Add the recovery outcome to an existing incident document."""
    path = Path(record_path)
    record = json.loads(path.read_text(encoding="utf-8"))
    record["status"] = status
    record["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    record["quarantined_files"] = list(quarantined_files)
    if error is not None:
        record["recovery_error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": "".join(traceback.format_exception(error)),
        }
    _write_failure_record(path, record)


def _write_failure_record(path: Path, record: dict[str, object]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def is_malformed_database_error(exc: BaseException) -> bool:
    """Return True when SQLite reports an unrecoverable malformed database image."""
    message = str(exc).lower()
    return "database disk image is malformed" in message or "file is not a database" in message


def is_sqlite_integrity_failure(detail: str) -> bool:
    """Distinguish corruption findings from transient/storage probe failures.

    ``PRAGMA quick_check`` can fail before it produces an integrity result (for
    example with ``disk I/O error``).  Replacing a database in that situation
    is both destructive and unlikely to succeed on the same unhealthy storage.
    Actual quick-check findings are returned as text. Page errors are commonly
    prefixed by ``*** in database ... ***``, while index errors use messages
    such as ``row ... missing from index ...``; malformed headers are raised.
    """
    normalized = detail.strip().lower()
    integrity_markers = (
        "*** in database ",
        "row ",
        "wrong # of entries in index ",
        "missing from index ",
        "malformed database schema",
    )
    return is_malformed_database_error(Exception(normalized)) or any(
        marker in normalized for marker in integrity_markers
    )


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
            rows = conn.execute("PRAGMA quick_check").fetchall()
    except sqlite3.DatabaseError as exc:
        return False, str(exc)
    detail = "\n".join(str(row[0]) for row in rows) if rows else "no quick_check result"
    return detail.lower() == "ok", detail


def reindex_sqlite_database(db_path: str) -> tuple[bool, str]:
    """Rebuild all indexes and verify the database without replacing its data.

    An inconsistent index is repairable and should not cause the recovery path
    to discard an otherwise healthy database. The caller must fence business
    connections and hold the database's exclusive access lock while this runs.
    """
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("REINDEX")
        return quick_check_sqlite_database(db_path)
    except sqlite3.DatabaseError as exc:
        return False, str(exc)


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
        if is_sqlite_integrity_failure(detail):
            quarantined = quarantine_sqlite_database(db_path)
            quarantined_by_path[db_path] = quarantined
    return quarantined_by_path
