import sqlite3

import collector
import web_app


def _create_btc_table(path, rows=()):
    with sqlite3.connect(path) as conn:
        conn.execute(
            f"""
            CREATE TABLE {collector.BTC_5M_TABLE} (
                open_time INTEGER PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                close_time INTEGER
            )
            """
        )
        conn.executemany(
            f"INSERT INTO {collector.BTC_5M_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def test_btc_payload_reads_migrated_table_from_base_database(monkeypatch, tmp_path):
    base_db = tmp_path / "base_data.db"
    trading_db = tmp_path / "trading.db"
    recent_ms = 4_102_444_800_000
    btc_row = (recent_ms, 1.0, 2.0, 0.5, 1.5, 10.0, recent_ms + 299_999)
    _create_btc_table(base_db, [btc_row])
    _create_btc_table(trading_db)

    monkeypatch.setattr(web_app, "DB_PATH", str(base_db))
    monkeypatch.setattr(web_app, "BASE_DB_PATH", str(base_db))
    monkeypatch.setattr(web_app, "TRADING_DB_PATH", str(trading_db))

    payload = web_app._btc_5m_payload()

    assert payload["total_rows"] == 1
    assert payload["table_rows"] == [list(btc_row)]
