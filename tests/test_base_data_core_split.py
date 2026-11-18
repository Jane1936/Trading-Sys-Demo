import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db_config
from base_data_core_migration import migrate


def test_connect_base_data_reads_1m_from_code_database(tmp_path, monkeypatch):
    base = str(tmp_path / "base_data.db")
    core = str(tmp_path / "base_data_core.db")
    monkeypatch.setattr(db_config, "BASE_DB_PATH", base)
    monkeypatch.setattr(db_config, "BASE_DATA_CORE_DB_PATH", core)
    with sqlite3.connect(base) as conn:
        conn.execute("CREATE TABLE klines_15m (symbol TEXT)")
        conn.execute("INSERT INTO klines_15m VALUES ('ETHUSDT')")
    with sqlite3.connect(core) as conn:
        conn.execute("CREATE TABLE klines_1m (symbol TEXT, open_time INTEGER)")
        conn.execute("INSERT INTO klines_1m VALUES ('BTCUSDT', 60)")

    with db_config.connect_base_data() as conn:
        assert conn.execute("SELECT symbol FROM core.klines_1m").fetchone() == ("BTCUSDT",)
        assert conn.execute("SELECT symbol FROM klines_15m").fetchone() == ("ETHUSDT",)


def test_migrate_copies_only_1m_table(tmp_path):
    source = str(tmp_path / "base_data.db")
    target = str(tmp_path / "base_data_core.db")
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE klines_1m (symbol TEXT, open_time INTEGER)")
        conn.execute("CREATE TABLE klines_15m (symbol TEXT)")
        conn.execute("INSERT INTO klines_1m VALUES ('BTCUSDT', 1)")
        conn.execute("INSERT INTO klines_15m VALUES ('ETHUSDT')")

    assert migrate(source, target) == 1
    with sqlite3.connect(target) as conn:
        assert conn.execute("SELECT * FROM klines_1m").fetchall() == [("BTCUSDT", 1)]
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='klines_15m'").fetchone() is None

