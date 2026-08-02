import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db_config


def test_connection_scope_reuses_one_physical_connection(tmp_path, monkeypatch):
    db_path = str(tmp_path / "trading.db")
    physical_opens = []
    real_connect = db_config.sqlite3.connect

    def counted_connect(*args, **kwargs):
        physical_opens.append(args[0])
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(db_config.sqlite3, "connect", counted_connect)

    with db_config.sqlite_connection_scope(db_path):
        with db_config.connect_sqlite(db_path) as first:
            first.execute("CREATE TABLE events (value INTEGER)")
            first.execute("INSERT INTO events VALUES (1)")
        with db_config.connect_sqlite(db_path) as second:
            second.execute("INSERT INTO events VALUES (2)")

    assert physical_opens == [db_path]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT value FROM events ORDER BY value").fetchall() == [
            (1,),
            (2,),
        ]


def test_connection_scope_attaches_companion_database_only_once(tmp_path):
    trading_path = str(tmp_path / "trading.db")
    scoring_path = str(tmp_path / "scoring.db")
    with sqlite3.connect(scoring_path) as conn:
        conn.execute("CREATE TABLE scores (value INTEGER)")
        conn.execute("INSERT INTO scores VALUES (42)")

    with db_config.sqlite_connection_scope(trading_path):
        for _ in range(2):
            with db_config.connect_sqlite(trading_path) as conn:
                db_config.attach_databases(conn, [("scoring", scoring_path)])
                assert conn.execute("SELECT value FROM scoring.scores").fetchone() == (42,)


def test_connection_scope_rolls_back_failed_nested_unit(tmp_path):
    db_path = str(tmp_path / "trading.db")

    with db_config.sqlite_connection_scope(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("CREATE TABLE events (value INTEGER)")
        try:
            with db_config.connect_sqlite(db_path) as conn:
                conn.execute("INSERT INTO events VALUES (1)")
                raise RuntimeError("failed strategy")
        except RuntimeError:
            pass
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("INSERT INTO events VALUES (2)")

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT value FROM events").fetchall() == [(2,)]


def test_scoped_connection_can_be_released_before_recovery(tmp_path):
    db_path = str(tmp_path / "trading.db")

    with db_config.sqlite_connection_scope(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("CREATE TABLE events (value INTEGER)")
        assert db_config.close_scoped_connection(db_path)
        assert not db_config.close_scoped_connection(db_path)
        with db_config.connect_sqlite(db_path) as replacement:
            replacement.execute("INSERT INTO events VALUES (1)")

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT value FROM events").fetchall() == [(1,)]


def test_connection_scopes_deduplicates_duplicate_paths(tmp_path, monkeypatch):
    db_path = str(tmp_path / "trading.db")
    physical_opens = []
    real_connect = db_config.sqlite3.connect

    def counted_connect(*args, **kwargs):
        physical_opens.append(args[0])
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(db_config.sqlite3, "connect", counted_connect)

    with db_config.sqlite_connection_scopes(db_path, db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("CREATE TABLE events (value INTEGER)")
            conn.execute("INSERT INTO events VALUES (1)")
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("INSERT INTO events VALUES (2)")

    assert physical_opens == [db_path]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone() == (2,)


def test_connection_scopes_reuses_trading_and_core_connections(tmp_path, monkeypatch):
    trading_path = str(tmp_path / "trading.db")
    core_path = str(tmp_path / "trading_core.db")
    physical_opens = []
    real_connect = db_config.sqlite3.connect

    def counted_connect(*args, **kwargs):
        physical_opens.append(args[0])
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(db_config.sqlite3, "connect", counted_connect)

    with db_config.sqlite_connection_scopes(trading_path, core_path):
        for path, value in ((trading_path, 1), (core_path, 2), (trading_path, 3), (core_path, 4)):
            with db_config.connect_sqlite(path) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS events (value INTEGER)")
                conn.execute("INSERT INTO events VALUES (?)", (value,))

    assert physical_opens == [trading_path, core_path]
