import sqlite3

import pytest

from scoring_system import ScoringSystem


def _insert_rule11_candles(conn, symbol, first_open, latest_close):
    conn.executemany(
        """
        INSERT INTO klines_15m (symbol, open_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (symbol, index, first_open if index == 1 else 100, 110, 90,
             latest_close if index == 4 else 100, 1)
            for index in range(1, 5)
        ],
    )


def _score_rule11(tmp_path, monkeypatch, symbol_close, market_close):
    db_path = tmp_path / "klines.db"
    base_db_path = tmp_path / "base_data.db"
    monkeypatch.setattr("db_config.BASE_DB_PATH", str(base_db_path))
    scoring = ScoringSystem(db_path=str(db_path), settings_db_path=str(db_path))
    scoring.init_table()
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
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        _insert_rule11_candles(conn, "BTCUSDT", 100, symbol_close)
        # A legacy ALLUSDT row in the symbol table must not affect rule 11.
        _insert_rule11_candles(conn, "ALLUSDT", 100, 150)
    with sqlite3.connect(base_db_path) as conn:
        conn.execute(
            """
            CREATE TABLE allusdt_15m_klines (
                open_time INTEGER NOT NULL PRIMARY KEY,
                open REAL NOT NULL,
                close REAL NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO allusdt_15m_klines (open_time, open, close) VALUES (?, ?, ?)",
            [
                (index, 100 if index == 1 else 100,
                 market_close if index == 4 else 100)
                for index in range(1, 5)
            ],
        )

    scoring._save_oi_loss_rate_240m_score(
        symbol="BTCUSDT", decision_round_ts=900_000, updated_at=900_001
    )
    _, rows = scoring.get_latest_round_scores_oi_loss_rate_240m()
    return rows[0]


def test_rule11_does_not_score_below_two_percent_relative_strength(tmp_path, monkeypatch):
    row = _score_rule11(tmp_path, monkeypatch, symbol_close=102.9, market_close=101)

    assert row["delta"] == pytest.approx(0.029)
    assert row["delta_all"] == pytest.approx(0.01)
    assert row["relative_strength"] == pytest.approx(0.019)
    assert row["score"] == 0
    assert row["reason"] == "rule11_not_met"


def test_rule11_scores_at_two_percent_relative_strength(tmp_path, monkeypatch):
    row = _score_rule11(tmp_path, monkeypatch, symbol_close=103, market_close=101)

    assert row["relative_strength"] == pytest.approx(0.02)
    assert row["score"] == 5
    assert row["reason"] == "relative_strength_gte_2pct"


def test_rule11_init_migrates_legacy_open_interest_schema(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE symbol_scores_oi_loss_rate_240m (
                symbol TEXT NOT NULL,
                decision_round_ts INTEGER NOT NULL,
                score INTEGER NOT NULL,
                reason TEXT NOT NULL,
                latest_open_interest REAL NOT NULL,
                open_interest_240m_ago REAL NOT NULL,
                oi_loss_rate REAL NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(symbol, decision_round_ts)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO symbol_scores_oi_loss_rate_240m
            VALUES ('BTCUSDT', 1, 5, 'legacy', 101, 100, 0, 2)
            """
        )

    scoring = ScoringSystem(db_path=str(db_path), settings_db_path=str(db_path))
    scoring.init_table()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(symbol_scores_oi_loss_rate_240m)"
            )
        }
        row = conn.execute(
            "SELECT * FROM symbol_scores_oi_loss_rate_240m WHERE symbol = 'BTCUSDT'"
        ).fetchone()

    assert {"delta", "delta_all", "relative_strength"}.issubset(columns)
    assert "oi_loss_rate" not in columns
    assert row["score"] == 5
    assert row["delta"] == 0
