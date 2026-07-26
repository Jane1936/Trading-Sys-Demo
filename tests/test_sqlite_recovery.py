import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector
import db_config
import app as worker_app
import web_app
from scoring_system import ScoringSystem
from sqlite_recovery import ensure_sqlite_database_usable, is_malformed_database_error


def test_connect_sqlite_initializes_wal_once_per_database_file(tmp_path, monkeypatch):
    db_path = str(tmp_path / "wal-once.db")
    calls = []
    original = db_config.configure_sqlite_connection

    def record_configuration(conn, *, wal=True, initialize_wal=True):
        calls.append((wal, initialize_wal))
        return original(conn, wal=wal, initialize_wal=initialize_wal)

    monkeypatch.setattr(db_config, "configure_sqlite_connection", record_configuration)
    db_config._wal_initialized_files.pop(str(Path(db_path).resolve()), None)

    with db_config.connect_sqlite(db_path):
        pass
    with db_config.connect_sqlite(db_path):
        pass

    assert calls == [(True, True), (True, False)]


def test_ensure_sqlite_database_usable_quarantines_malformed_database(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    wal_path = tmp_path / "klines.db-wal"
    wal_path.write_text("wal")

    quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True, once_per_process=False)

    assert not db_path.exists()
    assert not wal_path.exists()
    assert len(quarantined) >= 2
    assert any(path.endswith("klines.db" + path[path.index(".corrupt-"):]) for path in quarantined)
    assert all(".corrupt-" in path for path in quarantined)


def test_ensure_sqlite_database_usable_skips_quick_check_by_default(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")

    quarantined = ensure_sqlite_database_usable(str(db_path))

    assert quarantined == []
    assert db_path.read_bytes() == b"not a sqlite database"


def test_ensure_sqlite_database_usable_checks_path_once_per_process(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")

    first_quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True)
    db_path.write_bytes(b"not a sqlite database")
    second_quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True)

    assert first_quarantined
    assert second_quarantined == []
    assert db_path.read_bytes() == b"not a sqlite database"


def test_collector_init_db_recreates_after_quarantining_malformed_database(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))

    collector.init_db()

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (collector.BTC_5M_TABLE,)).fetchone()


def test_web_before_request_reports_without_replacing_live_database(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "_db_recovery_checked_path", None)
    monkeypatch.setattr(web_app, "WEB_SQLITE_QUICK_CHECK_ON_REQUEST", True)

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    assert db_path.read_bytes() == b"not a sqlite database"


def test_web_before_request_skips_quick_check_by_default(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "_db_recovery_checked_path", None)
    monkeypatch.setattr(web_app, "WEB_SQLITE_QUICK_CHECK_ON_REQUEST", False)

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    assert db_path.read_bytes() == b"not a sqlite database"


def test_is_malformed_database_error_matches_sqlite_message():
    assert is_malformed_database_error(sqlite3.DatabaseError("database disk image is malformed"))
    assert not is_malformed_database_error(sqlite3.DatabaseError("database is locked"))


def test_safe_page_module_does_not_quarantine_live_database(tmp_path, monkeypatch):
    base_db_path = tmp_path / "base.db"
    scoring_db_path = tmp_path / "scoring.db"
    scoring_db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(db_config, "BASE_DB_PATH", str(base_db_path))
    monkeypatch.setattr(
        db_config,
        "DB_LABELS",
        {"base": str(base_db_path), "scoring": str(scoring_db_path)},
    )
    scoring = ScoringSystem(db_path=str(scoring_db_path))

    result, error = web_app._safe_page_module("评分表初始化", scoring.init_table, None)

    assert result is None
    assert error is not None
    assert "已禁止新业务访问" in error["error"]
    assert scoring_db_path.read_bytes() == b"not a sqlite database"
    assert not list(tmp_path.glob("scoring.db.corrupt-*"))
    assert Path(db_config.database_recovery_marker(str(scoring_db_path))).exists()


def test_worker_health_check_fences_and_replaces_malformed_database(tmp_path, monkeypatch):
    db_paths = [tmp_path / f"db-{index}.sqlite" for index in range(4)]
    for db_path in db_paths:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE healthy (value INTEGER)")
    damaged_path = db_paths[2]
    damaged_path.write_bytes(b"not a sqlite database")

    def initialize():
        with db_config.connect_sqlite(str(damaged_path)) as conn:
            conn.execute("CREATE TABLE recovered (value INTEGER)")

    monkeypatch.setattr(worker_app, "_database_initializers", lambda: {
        str(path): (initialize if path == damaged_path else lambda: None) for path in db_paths
    })

    recovered = worker_app.check_worker_databases()

    assert list(recovered) == [str(damaged_path)]
    with sqlite3.connect(damaged_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT 1 FROM recovered").fetchall() == []
    assert list(tmp_path.glob(f"{damaged_path.name}.corrupt-*"))
    assert not Path(db_config.database_recovery_marker(str(damaged_path))).exists()


def test_worker_malformed_error_triggers_immediate_health_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_app, "check_worker_databases", lambda: calls.append(True)
    )

    assert worker_app.recover_after_worker_error(
        sqlite3.DatabaseError("database disk image is malformed")
    )
    assert calls == [True]
    assert not worker_app.recover_after_worker_error(sqlite3.DatabaseError("database is locked"))
    assert calls == [True]


def test_worker_database_health_check_interval_is_thirty_seconds():
    assert worker_app.DATABASE_HEALTH_CHECK_INTERVAL_SEC == 30


def test_recovery_marker_blocks_business_connections(tmp_path):
    db_path = str(tmp_path / "scoring.db")
    Path(db_config.database_recovery_marker(db_path)).write_text("recovering")

    with pytest.raises(db_config.DatabaseRecoveringError):
        db_config.connect_sqlite(db_path)

    with db_config.sqlite_recovery_bypass(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("CREATE TABLE recovered (value INTEGER)")


def test_exclusive_recovery_waits_for_existing_connection_to_close(tmp_path):
    db_path = str(tmp_path / "scoring.db")
    conn = db_config.connect_sqlite(db_path)
    acquired = threading.Event()

    def acquire_exclusive():
        with db_config.sqlite_access_lock(db_path, exclusive=True):
            acquired.set()

    thread = threading.Thread(target=acquire_exclusive)
    thread.start()
    time.sleep(0.05)
    assert not acquired.is_set()

    conn.close()
    thread.join(timeout=1)
    assert acquired.is_set()
