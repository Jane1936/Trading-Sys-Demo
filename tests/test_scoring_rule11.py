import sqlite3

from scoring_system import ScoringSystem


def _score_rule11(tmp_path, latest_oi, oi_240m_ago):
    db_path = tmp_path / "klines.db"
    scoring = ScoringSystem(db_path=str(db_path), settings_db_path=str(db_path))
    scoring.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE open_interest_1m (
                symbol TEXT NOT NULL,
                snapshot_time INTEGER NOT NULL,
                open_interest REAL NOT NULL,
                PRIMARY KEY (symbol, snapshot_time)
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO open_interest_1m (symbol, snapshot_time, open_interest)
            VALUES (?, ?, ?)
            """,
            [
                (
                    "BTCUSDT",
                    snapshot_time,
                    oi_240m_ago if snapshot_time == 1 else latest_oi,
                )
                for snapshot_time in range(240, 0, -1)
            ],
        )

    scoring._save_oi_loss_rate_240m_score(
        symbol="BTCUSDT", decision_round_ts=900_000, updated_at=900_001
    )
    _, rows = scoring.get_latest_round_scores_oi_loss_rate_240m()
    return rows[0]


def test_rule11_does_not_score_when_oi_loss_is_within_three_percent(tmp_path):
    row = _score_rule11(tmp_path, latest_oi=98.0, oi_240m_ago=100.0)

    assert row["oi_loss_rate"] == 0.02
    assert row["score"] == 0
    assert row["reason"] == "rule11_not_met"


def test_rule11_scores_when_latest_oi_is_not_lower_than_240m_ago(tmp_path):
    row = _score_rule11(tmp_path, latest_oi=101.0, oi_240m_ago=100.0)

    assert row["oi_loss_rate"] == 0.0
    assert row["score"] == 5
    assert row["reason"] == "oi_1m_gte_240m"
