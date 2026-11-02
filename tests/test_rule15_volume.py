import sqlite3

from scoring_system import ScoringSystem


def _seed_15m_volumes(db_path, volumes):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE klines_15m (
                symbol TEXT NOT NULL, open_time INTEGER NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL NOT NULL, funding_rate REAL
            )
            """
        )
        conn.executemany(
            """INSERT INTO klines_15m
                (symbol, open_time, open, high, low, close, volume, funding_rate)
             VALUES (?, ?, 1, 1, 1, 1, ?, 0)""",
            [("BTCUSDT", i, volume) for i, volume in enumerate(volumes)],
        )


def test_rule15_uses_two_latest_15m_bars_and_previous_48(tmp_path):
    db_path = tmp_path / "scores.db"
    # Newest rows have the largest open_time.  The latest two sum to 20;
    # the previous 48 sum to 240, so the average 30m baseline is 10.
    _seed_15m_volumes(db_path, [5] * 48 + [10, 10])
    scoring = ScoringSystem(db_path=str(db_path))
    scoring.init_table()

    scoring._save_1h_volume_spike_latest_score("BTCUSDT", 123, 456)
    _, rows = scoring.get_latest_round_scores_1h_volume_spike_latest()

    row = rows[0]
    assert row["volume_30m"] == 20
    assert row["volume_pre_12h"] == 240
    assert row["volume_avg_30m"] == 10
    assert row["score"] == scoring._score_weight(15)


def test_rule15_requires_all_50_bars(tmp_path):
    db_path = tmp_path / "scores.db"
    _seed_15m_volumes(db_path, [1] * 49)
    scoring = ScoringSystem(db_path=str(db_path))
    scoring.init_table()

    scoring._save_1h_volume_spike_latest_score("BTCUSDT", 123, 456)
    _, rows = scoring.get_latest_round_scores_1h_volume_spike_latest()

    assert rows == []
