import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector
import web_app
from sqlite_recovery import ensure_sqlite_database_usable, is_malformed_database_error


def test_ensure_sqlite_database_usable_quarantines_malformed_database(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    wal_path = tmp_path / "klines.db-wal"
    wal_path.write_text("wal")

    quarantined = ensure_sqlite_database_usable(str(db_path))

    assert not db_path.exists()
    assert not wal_path.exists()
    assert len(quarantined) >= 2
    assert any(path.endswith("klines.db" + path[path.index(".corrupt-"):]) for path in quarantined)
    assert all(".corrupt-" in path for path in quarantined)


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

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_is_malformed_database_error_matches_sqlite_message():
    assert is_malformed_database_error(sqlite3.DatabaseError("database disk image is malformed"))
    assert not is_malformed_database_error(sqlite3.DatabaseError("database is locked"))
