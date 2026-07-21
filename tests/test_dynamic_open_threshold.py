import sqlite3

from dynamic_open_threshold import DynamicOpenThresholdModule
from openable_symbol_module import OpenableSymbolModule


def _create_total_scores(conn):
    conn.execute(
        """
        CREATE TABLE symbol_total_scores (
            symbol TEXT NOT NULL,
            decision_round_ts INTEGER NOT NULL,
            total_score INTEGER NOT NULL,
            PRIMARY KEY (symbol, decision_round_ts)
        )
        """
    )


def test_dynamic_open_threshold_policies(tmp_path):
    db_path = tmp_path / "klines.db"
    module = DynamicOpenThresholdModule(db_path=str(db_path))
    with sqlite3.connect(db_path) as conn:
        _create_total_scores(conn)
        conn.executemany(
            "INSERT INTO symbol_total_scores(symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
            [("OLD", -50_000_000, 99), ("MID", 60_000, 80), ("HIGH", 120_000, 85)],
        )

    high = module.run_round(decision_round_ts=120_000, evaluated_at=130_000)
    assert high.highest_total_score == 85
    assert high.allow_new_positions is True
    assert high.min_open_total_score is None
    assert high.policy == "no_min_open_threshold"

    mid = module.run_round(decision_round_ts=60_000, evaluated_at=70_000)
    assert mid.highest_total_score == 80
    assert mid.allow_new_positions is True
    assert mid.min_open_total_score == 81
    assert mid.policy == "trend_standard_or_above_only"

    low = module.run_round(decision_round_ts=-1, evaluated_at=1)
    assert low.allow_new_positions is False
    assert low.policy == "no_new_positions"


def test_openable_respects_dynamic_threshold(tmp_path):
    db_path = tmp_path / "klines.db"
    module = OpenableSymbolModule(db_path=str(db_path))
    module.init_table()
    with sqlite3.connect(db_path) as conn:
        _create_total_scores(conn)
        conn.execute("CREATE TABLE current_round_cooldown_symbols (symbol TEXT, decision_round_ts INTEGER)")
        conn.execute(
            "CREATE TABLE symbol_scores_structural_stop_loss_distance (symbol TEXT, decision_round_ts INTEGER, stop_loss_distance_ratio REAL)"
        )
        conn.executemany(
            "INSERT INTO symbol_total_scores(symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
            [("A", 900_000, 80), ("B", 900_000, 81)],
        )
        conn.executemany(
            "INSERT INTO symbol_scores_structural_stop_loss_distance(symbol, decision_round_ts, stop_loss_distance_ratio) VALUES (?, ?, ?)",
            [("A", 900_000, 0.02), ("B", 900_000, 0.02)],
        )

    rows = module.run_round(decision_round_ts=900_000, min_total_score=81, threshold_reason="highest_score_73_to_84")
    assert [row.symbol for row in rows] == ["B"]
    assert rows[0].qualified is True
    assert rows[0].reason == "highest_score_73_to_84"

    assert module.run_round(decision_round_ts=900_000, allow_new_positions=False) == []


def test_current_open_block_notice_prefers_independent_market_filter_reason():
    from types import SimpleNamespace
    import web_app

    notice = web_app._current_open_block_notice(
        900_000,
        [SimpleNamespace(decision_round_ts=900_000, allow_new_positions=False, reason="btc_siphon")],
        [SimpleNamespace(decision_round_ts=900_000, allow_new_positions=True, reason="highest_score_gte_85")],
    )

    assert notice == "独立市场过滤模块：btc_siphon"
