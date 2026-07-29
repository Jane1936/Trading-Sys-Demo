"""Central SQLite database layout.

The system uses four independent SQLite files so a corrupted/deleted module DB
only disables that module instead of taking down the whole app.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from contextvars import ContextVar
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

_recovery_local = threading.local()
_connection_local = threading.local()
_wal_init_lock = threading.Lock()
_wal_initialized_files: dict[str, tuple[int, int]] = {}
_scoped_connections: ContextVar[dict[str, sqlite3.Connection] | None] = ContextVar(
    "sqlite_scoped_connections", default=None
)


class DatabaseRecoveringError(sqlite3.OperationalError):
    """Raised when a database has been fenced for automatic recovery."""


class sqlite_access_lock:
    """Cross-process shared/exclusive admission lock for one SQLite file."""

    def __init__(self, db_path: str, *, exclusive: bool = False):
        self.db_path = db_path
        self.exclusive = exclusive
        self._fh = None

    def __enter__(self):
        ensure_parent_dir(self.db_path)
        self._fh = open(f"{self.db_path}.access.lock", "a+")
        if os.name == "posix":
            import fcntl

            fcntl.flock(
                self._fh.fileno(), fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
            )
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fh is not None:
            if os.name == "posix":
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None
        return False


def database_recovery_marker(db_path: str) -> str:
    return f"{db_path}.recovering"


def _recovery_bypass_paths() -> set[str]:
    paths = getattr(_recovery_local, "paths", None)
    if paths is None:
        paths = set()
        _recovery_local.paths = paths
    return paths


class sqlite_recovery_bypass:
    """Allow the recovery thread to initialize a fenced database."""

    def __init__(self, db_path: str):
        self.path = str(Path(db_path).resolve())

    def __enter__(self):
        _recovery_bypass_paths().add(self.path)

    def __exit__(self, exc_type, exc, tb):
        _recovery_bypass_paths().discard(self.path)
        return False


class ManagedSQLiteConnection(sqlite3.Connection):
    """Connection that owns access locks and closes them on context exit."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._access_locks = []
        self._managed_closed = False
        self._db_path = ""

    def add_access_lock(self, lock) -> None:
        self._access_locks.append(lock)

    def close(self) -> None:
        if self._managed_closed:
            return
        try:
            super().close()
        finally:
            self._managed_closed = True
            _unregister_connection(self)
            while self._access_locks:
                self._access_locks.pop().__exit__(None, None, None)

    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def _thread_connections() -> list[ManagedSQLiteConnection]:
    connections = getattr(_connection_local, "connections", None)
    if connections is None:
        connections = []
        _connection_local.connections = connections
    return connections


def _register_connection(conn: ManagedSQLiteConnection, db_path: str) -> None:
    conn._db_path = str(Path(db_path).resolve())
    _thread_connections().append(conn)


def _unregister_connection(conn: ManagedSQLiteConnection) -> None:
    connections = _thread_connections()
    try:
        connections.remove(conn)
    except ValueError:
        pass


def assert_no_active_sqlite_transaction(operation: str = "network request") -> None:
    """Reject slow external I/O while this thread owns a SQLite transaction."""
    active_paths = [
        conn._db_path or "<unknown>"
        for conn in tuple(_thread_connections())
        if not conn._managed_closed and conn.in_transaction
    ]
    if active_paths:
        joined = ", ".join(sorted(set(active_paths)))
        raise RuntimeError(
            f"cannot perform {operation} during an active SQLite transaction: {joined}"
        )


class BorrowedSQLiteConnection:
    """A non-owning lease for a connection managed by a round scope.

    Existing modules can keep using ``with connect_sqlite(...) as conn``.  The
    nested context still commits or rolls back its unit of work, but closing the
    lease does not close the round-owned connection.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False

    def close(self) -> None:
        """Release this lease; the owning round scope closes the connection."""


@contextmanager
def sqlite_connection_scope(
    db_path: str, *, row_factory=None, wal: bool = True
):
    """Reuse one SQLite connection for repeated opens in one logical round.

    Scopes are context-local, so worker threads and scoring subprocesses never
    share a SQLite connection.  A nested scope for the same database borrows the
    existing connection; the outermost scope owns and closes it.
    """
    normalized = str(Path(db_path).resolve())
    current = _scoped_connections.get()
    if current is not None and normalized in current:
        yield BorrowedSQLiteConnection(current[normalized])
        return

    conn = connect_sqlite(db_path, row_factory=row_factory, wal=wal, _allow_borrow=False)
    scoped = dict(current or {})
    scoped[normalized] = conn
    token = _scoped_connections.set(scoped)
    try:
        yield conn
    finally:
        _scoped_connections.reset(token)
        conn.close()


def close_scoped_connection(db_path: str) -> bool:
    """Close and evict a round connection before malformed-DB recovery."""
    normalized = str(Path(db_path).resolve())
    current = _scoped_connections.get()
    if current is None or normalized not in current:
        return False
    conn = current[normalized]
    remaining = dict(current)
    del remaining[normalized]
    _scoped_connections.set(remaining)
    conn.close()
    return True


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
    managed_conn = conn._conn if isinstance(conn, BorrowedSQLiteConnection) else conn
    main_path = Path(conn.execute("PRAGMA database_list").fetchone()[2] or "").resolve()
    seen = {str(row[1]) for row in conn.execute("PRAGMA database_list").fetchall()}
    for schema, path in attachments:
        if not path or schema in seen:
            continue
        ensure_parent_dir(path)
        try:
            if Path(path).resolve() == main_path:
                continue
        except OSError:
            pass
        lock = _acquire_database_access(path)
        try:
            conn.execute(f"ATTACH DATABASE ? AS {quote_identifier(schema)}", (path,))
        except Exception:
            lock.__exit__(None, None, None)
            if isinstance(managed_conn, ManagedSQLiteConnection):
                managed_conn.close()
            raise
        if isinstance(managed_conn, ManagedSQLiteConnection):
            managed_conn.add_access_lock(lock)
        else:
            lock.__exit__(None, None, None)
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


def configure_sqlite_connection(
    conn: sqlite3.Connection, *, wal: bool = True, initialize_wal: bool = True
) -> sqlite3.Connection:
    """Apply SQLite settings used by concurrent workers."""
    conn.execute("PRAGMA busy_timeout=30000;")
    if wal:
        if initialize_wal:
            conn.execute("PRAGMA journal_mode=WAL;")
        # FULL adds an fsync for each WAL transaction.  It costs some write
        # throughput but is the safer default for bind-mounted production data.
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute("PRAGMA wal_autocheckpoint=1000;")
    return conn


def connect_sqlite(
    db_path: str,
    *,
    timeout: int = 30,
    row_factory=None,
    wal: bool = True,
    _allow_borrow: bool = True,
) -> sqlite3.Connection:
    normalized = str(Path(db_path).resolve())
    scoped = _scoped_connections.get()
    if _allow_borrow and scoped is not None and normalized in scoped:
        conn = scoped[normalized]
        if row_factory is not None:
            conn.row_factory = row_factory
        return BorrowedSQLiteConnection(conn)
    ensure_parent_dir(db_path)
    lock = _acquire_database_access(db_path)
    try:
        conn = sqlite3.connect(db_path, timeout=timeout, factory=ManagedSQLiteConnection)
    except Exception:
        lock.__exit__(None, None, None)
        raise
    _register_connection(conn, db_path)
    conn.add_access_lock(lock)
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        initialize_wal = False
        if wal:
            normalized = str(Path(db_path).resolve())
            stat_result = os.stat(db_path)
            identity = (stat_result.st_dev, stat_result.st_ino)
            with _wal_init_lock:
                initialize_wal = _wal_initialized_files.get(normalized) != identity
                configured = configure_sqlite_connection(
                    conn, wal=True, initialize_wal=initialize_wal
                )
                if initialize_wal:
                    _wal_initialized_files[normalized] = identity
                return configured
        return configure_sqlite_connection(conn, wal=False)
    except Exception:
        conn.close()
        raise


def _acquire_database_access(db_path: str) -> sqlite_access_lock:
    normalized = str(Path(db_path).resolve())
    if os.path.exists(database_recovery_marker(db_path)) and normalized not in _recovery_bypass_paths():
        raise DatabaseRecoveringError(f"database is recovering: {db_path}")
    lock = sqlite_access_lock(db_path)
    lock.__enter__()
    # Recovery may have fenced the DB while this process waited for the lock.
    if os.path.exists(database_recovery_marker(db_path)) and normalized not in _recovery_bypass_paths():
        lock.__exit__(None, None, None)
        raise DatabaseRecoveringError(f"database is recovering: {db_path}")
    return lock
