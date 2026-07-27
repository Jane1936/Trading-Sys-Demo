import collector
import db_config


def test_kline_job_runs_market_filter_sources_in_main_flow(monkeypatch):
    calls = []

    monkeypatch.setattr(collector, "UNIVERSE", ["ETH"])
    monkeypatch.setattr(collector, "kline_job_running", False)
    monkeypatch.setattr(collector, "run_kline_main", lambda universe: calls.append(("universe_klines", tuple(universe))))
    monkeypatch.setattr(collector, "run_btc_15m_main", lambda: calls.append(("btc_15m",)))
    monkeypatch.setattr(
        collector.allusdt_15m_ma20,
        "run",
        lambda db_path, db_write_lock=None: calls.append(("allusdt_15m_ma20", db_path, db_write_lock)),
    )

    collector.kline_job()

    assert calls == [
        ("universe_klines", ("ETH",)),
        ("btc_15m",),
        ("allusdt_15m_ma20", collector.DB_PATH, collector.db_write_lock),
    ]
    assert collector.kline_job_running is False


def test_allusdt_init_db_creates_1h_tables(tmp_path):
    import sqlite3
    import allusdt_15m_ma20

    db_path = tmp_path / "base_data.db"
    with sqlite3.connect(db_path) as conn:
        allusdt_15m_ma20.init_db(conn)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert allusdt_15m_ma20.H1_KLINE_TABLE in tables
    assert allusdt_15m_ma20.H1_MA20_TABLE in tables


def test_allusdt_1h_ma20_calculates_from_1h_klines(tmp_path):
    import sqlite3
    import allusdt_15m_ma20

    db_path = tmp_path / "base_data.db"
    with sqlite3.connect(db_path) as conn:
        allusdt_15m_ma20.init_db(conn)
        rows = [
            (
                i * allusdt_15m_ma20.H1_INTERVAL_MS,
                1,
                1,
                1,
                float(i + 1),
                1,
                (i + 1) * allusdt_15m_ma20.H1_INTERVAL_MS - 1,
            )
            for i in range(20)
        ]
        conn.executemany(
            f"INSERT INTO {allusdt_15m_ma20.H1_KLINE_TABLE} (open_time, open, high, low, close, volume, close_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        changed = allusdt_15m_ma20.save_ma20(
            conn,
            allusdt_15m_ma20.H1_KLINE_TABLE,
            allusdt_15m_ma20.H1_MA20_TABLE,
        )
        saved = conn.execute(
            f"SELECT close, ma20 FROM {allusdt_15m_ma20.H1_MA20_TABLE} ORDER BY open_time DESC LIMIT 1"
        ).fetchone()

    assert changed == 1
    assert saved == (20.0, 10.5)


def test_save_kline_round_uses_one_connection_for_all_symbols(tmp_path, monkeypatch):
    import sqlite3

    db_path = tmp_path / "base_data.db"
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    collector.init_db()
    connections = []

    def counted_connection():
        connections.append(True)
        return db_config.connect_sqlite(str(db_path))

    monkeypatch.setattr(collector, "get_db_conn", counted_connection)
    klines = {
        symbol: [
            [minute * 60_000, "1", "2", "0.5", str(minute + 1), "10", (minute + 1) * 60_000 - 1]
            for minute in range(5)
        ]
        for symbol in ("BTC", "ETH")
    }

    stats = collector.save_kline_round(klines)

    assert connections == [True]
    assert stats["BTC"][0] == 5
    assert stats["ETH"][0] == 5
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM klines_1m").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM klines_5m").fetchone()[0] == 2


def test_save_open_interest_round_uses_one_transaction(tmp_path, monkeypatch):
    import sqlite3

    db_path = tmp_path / "base_data.db"
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    collector.init_db()

    assert collector.save_open_interest_round({"BTC": 100.0, "ETH": 200.0}) == 2

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM open_interest_1m").fetchone()[0] == 2
