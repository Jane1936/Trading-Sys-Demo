import sqlite3

import collector


def test_process_symbol_atr_15m_persists_latest_closed_atr14(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE klines_15m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        collector.init_atr_15m_table(conn)
        rows = []
        for i in range(15):
            open_time = i * 900_000
            close = 100.0 + i
            rows.append(
                (
                    "BTC",
                    open_time,
                    close - 0.5,
                    close + 2.0,
                    close - 3.0,
                    close,
                    10.0,
                    open_time + 900_000 - 1,
                )
            )
        conn.executemany(
            """
            INSERT INTO klines_15m
            (symbol, open_time, open, high, low, close, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    symbol, changed, atr14 = collector.process_symbol_atr_15m("BTC")

    assert symbol == "BTC"
    assert changed == 1
    assert atr14 == 5.0

    with sqlite3.connect(db_path) as conn:
        saved = conn.execute(
            "SELECT symbol, open_time, close_time, atr14 FROM atr_15m_indicators"
        ).fetchone()

    assert saved == ("BTC", 14 * 900_000, 15 * 900_000 - 1, 5.0)


def test_process_symbol_atr_15m_skips_incomplete_latest_bar(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE klines_15m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        collector.init_atr_15m_table(conn)
        rows = []
        for i in range(15):
            open_time = i * 900_000
            rows.append(("BTC", open_time, 1.0, 3.0, 0.5, 2.0, 1.0, open_time + 1))
        conn.executemany(
            """
            INSERT INTO klines_15m
            (symbol, open_time, open, high, low, close, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    assert collector.process_symbol_atr_15m("BTC") == ("BTC", 0, None)


def test_run_atr_15m_main_continues_when_one_symbol_fails(monkeypatch, capsys):
    calls = []

    def fake_process_symbol_atr_15m(symbol):
        calls.append(symbol)
        if symbol == "BAD":
            raise RuntimeError("boom")
        return symbol, 1, 2.5

    monkeypatch.setattr(collector, "process_symbol_atr_15m", fake_process_symbol_atr_15m)

    collector.run_atr_15m_main(["BAD", "GOOD"])

    captured = capsys.readouterr().out
    assert sorted(calls) == ["BAD", "GOOD"]
    assert "❌ atr 15m symbol task failed: boom" in captured
    assert "✅ GOOD: atr14_15m=2.5, changed=1" in captured
