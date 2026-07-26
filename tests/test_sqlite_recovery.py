import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector
import db_config
import app as worker_app
import web_app
from scoring_system import ScoringSystem
from sqlite_recovery import ensure_sqlite_database_usable, is_malformed_database_error


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


def test_web_before_request_recovers_malformed_database(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "_db_recovery_checked_path", None)
    monkeypatch.setattr(web_app, "WEB_SQLITE_QUICK_CHECK_ON_REQUEST", True)

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


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


def test_safe_page_module_retries_schema_initialization_after_quarantine(tmp_path, monkeypatch):
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
    assert error is None
    with sqlite3.connect(scoring_db_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        expected_tables = {"symbol_scores", "symbol_total_scores", "scoring_ma20_skip_records"}
        actual_tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert expected_tables <= actual_tables
    assert list(tmp_path.glob("scoring.db.corrupt-*"))


def test_worker_health_check_recovers_malformed_database(tmp_path, monkeypatch):
    db_paths = [tmp_path / f"db-{index}.sqlite" for index in range(4)]
    for db_path in db_paths:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE healthy (value INTEGER)")
    damaged_path = db_paths[2]
    damaged_path.write_bytes(b"not a sqlite database")

    initialized = []

    def initialize_damaged_database():
        initialized.append(str(damaged_path))
        with sqlite3.connect(damaged_path) as conn:
            conn.execute("CREATE TABLE recovered (value INTEGER)")

    initializers = {
        str(path): (initialize_damaged_database if path == damaged_path else lambda: None)
        for path in db_paths
    }
    monkeypatch.setattr(worker_app, "_database_initializers", lambda: initializers)

    recovered = worker_app.check_and_recover_worker_databases()

    assert list(recovered) == [str(damaged_path)]
    assert initialized == [str(damaged_path)]
    with sqlite3.connect(damaged_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'recovered'"
        ).fetchone()
    assert list(tmp_path.glob(f"{damaged_path.name}.corrupt-*"))


def test_worker_malformed_error_triggers_immediate_health_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_app, "check_and_recover_worker_databases", lambda: calls.append(True)
    )

    assert worker_app.recover_after_worker_error(
        sqlite3.DatabaseError("database disk image is malformed")
    )
    assert calls == [True]
    assert not worker_app.recover_after_worker_error(sqlite3.DatabaseError("database is locked"))
    assert calls == [True]


def test_worker_database_health_check_interval_is_thirty_minutes():
    assert worker_app.DATABASE_HEALTH_CHECK_INTERVAL_SEC == 30 * 60
