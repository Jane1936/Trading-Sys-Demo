import sqlite3

from scoring_system import ScoringSystem


def test_15m_ma20_readiness_reports_ready_and_missing_symbols(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)"
        )
        conn.execute(
            "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
            ("BTCUSDT", "15m", 900_000, 100.0),
        )
        conn.execute(
            "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
            ("ETHUSDT", "5m", 900_000, 100.0),
        )

    scoring = ScoringSystem(db_path=str(db_path))

    readiness = scoring.get_15m_ma20_readiness_for_round(
        decision_round_ts=1_800_000,
        symbols=["ETHUSDT", "BTCUSDT", "BTCUSDT"],
    )

    assert readiness.target_open_time == 900_000
    assert readiness.ready_symbols == ["BTCUSDT"]
    assert readiness.missing_symbols == ["ETHUSDT"]
    assert not readiness.ready
    assert not scoring.is_15m_ma20_ready_for_round(1_800_000, ["BTCUSDT", "ETHUSDT"])
    assert scoring.is_15m_ma20_ready_for_round(1_800_000, ["BTCUSDT"])


def test_ma20_skip_record_round_trips_missing_symbols(tmp_path):
    db_path = tmp_path / "klines.db"
    scoring = ScoringSystem(db_path=str(db_path))
    scoring.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)"
        )
    readiness = scoring.get_15m_ma20_readiness_for_round(
        decision_round_ts=1_800_000,
        symbols=["BTCUSDT", "ETHUSDT"],
    )

    scoring.record_ma20_skip_for_round(
        decision_round_ts=1_800_000,
        readiness=readiness,
        universe_count=2,
        created_at=1_800_001,
    )

    record = scoring.get_latest_ma20_skip_record()

    assert record is not None
    assert record.decision_round_ts == 1_800_000
    assert record.target_open_time == 900_000
    assert record.universe_count == 2
    assert record.ready_count == 0
    assert record.missing_count == 2
    assert record.missing_symbols == ["BTCUSDT", "ETHUSDT"]
    assert record.created_at == 1_800_001


def test_wait_for_15m_ma20_readiness_does_not_retry_by_default(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)"
        )
        conn.execute(
            "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
            ("BTCUSDT", "15m", 900_000, 100.0),
        )

    scoring = ScoringSystem(db_path=str(db_path))
    original_get = scoring.get_15m_ma20_readiness_for_round
    calls = 0

    def wrapped_get(decision_round_ts, symbols):
        nonlocal calls
        if calls == 1:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
                    ("ETHUSDT", "15m", 900_000, 100.0),
                )
        calls += 1
        return original_get(decision_round_ts, symbols)

    monkeypatch.setattr(scoring, "get_15m_ma20_readiness_for_round", wrapped_get)

    readiness = scoring.wait_for_15m_ma20_readiness_for_round(
        decision_round_ts=1_800_000,
        symbols=["BTCUSDT", "ETHUSDT"],
        retry_delay_seconds=0,
    )

    assert calls == 1
    assert readiness.ready_symbols == ["BTCUSDT"]
    assert readiness.missing_symbols == ["ETHUSDT"]
    assert not readiness.ready


def test_wait_for_15m_ma20_readiness_can_retry_when_explicitly_requested(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)"
        )
        conn.execute(
            "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
            ("BTCUSDT", "15m", 900_000, 100.0),
        )

    scoring = ScoringSystem(db_path=str(db_path))
    original_get = scoring.get_15m_ma20_readiness_for_round
    calls = 0

    def wrapped_get(decision_round_ts, symbols):
        nonlocal calls
        if calls == 1:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO ma20_indicators (symbol, interval, open_time, ma20) VALUES (?, ?, ?, ?)",
                    ("ETHUSDT", "15m", 900_000, 100.0),
                )
        calls += 1
        return original_get(decision_round_ts, symbols)

    monkeypatch.setattr(scoring, "get_15m_ma20_readiness_for_round", wrapped_get)

    readiness = scoring.wait_for_15m_ma20_readiness_for_round(
        decision_round_ts=1_800_000,
        symbols=["BTCUSDT", "ETHUSDT"],
        retries=1,
        retry_delay_seconds=0,
    )

    assert calls == 2
    assert readiness.ready_symbols == ["BTCUSDT", "ETHUSDT"]
    assert readiness.missing_symbols == []
    assert readiness.ready


def test_total_score_round_updated_at_returns_latest_update_time(tmp_path):
    db_path = tmp_path / "klines.db"
    scoring = ScoringSystem(db_path=str(db_path))
    scoring.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO symbol_total_scores
            (symbol, decision_round_ts, rule1_score, rule2_score, rule3_score, rule4_score, rule5_score, rule6_score, rule7_score, rule8_score, rule9_score, rule10_score, rule11_score, rule12_score, rule13_score, rule14_score, rule15_score, rule16_score, rule17_score, rule18_score, total_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("BTCUSDT", 1_800_000, *([0] * 18), 0, 1_830_500),
        )
        conn.execute(
            """
            INSERT INTO symbol_total_scores
            (symbol, decision_round_ts, rule1_score, rule2_score, rule3_score, rule4_score, rule5_score, rule6_score, rule7_score, rule8_score, rule9_score, rule10_score, rule11_score, rule12_score, rule13_score, rule14_score, rule15_score, rule16_score, rule17_score, rule18_score, total_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("ETHUSDT", 1_800_000, *([0] * 18), 0, 1_831_000),
        )

    assert scoring.get_total_score_round_updated_at(1_800_000) == 1_831_000
    assert scoring.get_total_score_round_updated_at(None) is None


def test_ma20_skip_record_for_round_only_returns_requested_round(tmp_path):
    db_path = tmp_path / "klines.db"
    scoring = ScoringSystem(db_path=str(db_path))
    scoring.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE ma20_indicators (symbol TEXT, interval TEXT, open_time INTEGER, ma20 REAL)"
        )
    older = scoring.get_15m_ma20_readiness_for_round(
        decision_round_ts=1_800_000,
        symbols=["BTCUSDT"],
    )
    scoring.record_ma20_skip_for_round(
        decision_round_ts=1_800_000,
        readiness=older,
        universe_count=1,
        created_at=1_800_001,
    )

    assert scoring.get_ma20_skip_record_for_round(1_800_000) is not None
    assert scoring.get_ma20_skip_record_for_round(2_700_000) is None
    assert scoring.get_ma20_skip_record_for_round(None) is None
