import sqlite3

from scoring_system import ScoringSystem


def _scoring(tmp_path):
    path = tmp_path / "scores.db"
    scoring = ScoringSystem(db_path=str(path))
    scoring.init_table()
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE klines_1m (symbol TEXT, open_time INTEGER, close REAL)")
        conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, low REAL, close REAL)")
        conn.execute("CREATE TABLE klines_1h (symbol TEXT, open_time INTEGER, high REAL)")
        conn.execute("CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)")
        conn.execute("CREATE TABLE ema_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ema20 REAL)")
    return scoring, path


def test_rule2_requires_ma20_breakout_and_ema20_distance_below_six_percent(tmp_path):
    scoring, path = _scoring(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO klines_1m VALUES ('BTC', 1, 105.99)")
        conn.execute("INSERT INTO ma20_indicators VALUES ('BTC', '15m', 1, 99)")
        conn.execute("INSERT INTO ema_indicators VALUES ('BTC', '15m', 1, 100)")
    scoring._save_close_gt_ma20_score("BTC", 1, 2)
    _, rows = scoring.get_latest_round_scores_close_gt_ma20()
    assert rows[0]["score"] == scoring.rule_score_weights[2]
    assert rows[0]["latest_15m_ema20"] == 100
    assert rows[0]["ema20_distance_ratio"] < 0.06

    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE klines_1m SET close = 106")
    scoring._save_close_gt_ma20_score("BTC", 2, 3)
    _, rows = scoring.get_latest_round_scores_close_gt_ma20()
    assert rows[0]["score"] == 0


def test_rule7_uses_point_sixty_five_close_position(tmp_path):
    scoring, path = _scoring(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT INTO klines_15m VALUES ('BTC', ?, 0, 10, 0, ?)",
            [(1, 6.5), (2, 6.5), (3, 6.49), (4, 6.49)],
        )
    scoring._save_15m_close_near_high_2of4_score("BTC", 1, 2)
    _, rows = scoring.get_latest_round_scores_15m_close_near_high_2of4()
    assert rows[0]["qualified_count"] == 2
    assert rows[0]["score"] == scoring.rule_score_weights[7]


def test_rule8_compares_latest_15m_high_with_all_previous_24_1h_highs(tmp_path):
    scoring, path = _scoring(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO klines_15m VALUES ('BTC', 100, 0, 25, 0, 0)")
        conn.executemany(
            "INSERT INTO klines_1h VALUES ('BTC', ?, ?)",
            [(i, float(i)) for i in range(1, 25)],
        )
    scoring._save_1h_latest_highest_24_score("BTC", 1, 2)
    _, rows = scoring.get_latest_round_scores_1h_latest_highest_24()
    assert rows[0]["latest_high"] == 25
    assert rows[0]["prev_24_max_high"] == 24
    assert rows[0]["score"] == scoring.rule_score_weights[8]
