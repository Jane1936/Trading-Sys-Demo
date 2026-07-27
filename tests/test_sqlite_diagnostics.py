import sqlite3

from sqlite_diagnostics import inspect_database


def test_inspect_database_reports_integrity_and_sidecars(tmp_path):
    db_path = tmp_path / "base.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE samples (value INTEGER)")

    report = inspect_database(str(db_path))

    assert report["health"]["status"] == "ok"
    assert report["sqlite"]["quick_check"] == "ok"
    assert report["sqlite"]["page_count"] > 0
    assert report["files"]["main"]["exists"] is True
    assert report["disk"]["free_bytes"] > 0


def test_inspect_database_does_not_create_missing_database(tmp_path):
    db_path = tmp_path / "missing.db"
    report = inspect_database(str(db_path))
    assert report["health"]["status"] == "missing"
    assert not db_path.exists()


def test_inspect_database_preserves_corrupt_file(tmp_path):
    db_path = tmp_path / "base.db"
    contents = b"not a sqlite database"
    db_path.write_bytes(contents)
    report = inspect_database(str(db_path))
    assert report["health"]["status"] == "error"
    assert db_path.read_bytes() == contents
