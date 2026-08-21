import sqlite3

from market_filter_module import MarketFilterModule
from market_filter_settings import get_settings, set_settings
import allusdt_15m_ma20
import collector


def _init_source_tables(conn):
    allusdt_15m_ma20.init_db(conn)
    collector.init_btc_15m_table(conn)


def _insert_rows(conn, table, closes):
    for idx, close in enumerate(closes):
        open_time = idx * 900_000
        conn.execute(
            f"""
            INSERT INTO {table} (open_time, open, high, low, close, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (open_time, close, close, close, close, 1, open_time + 899_999),
        )


def test_market_filter_allows_when_market_data_missing(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)

    result = MarketFilterModule(db_path=str(db_path)).run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.allow_new_positions is True
    assert result.btc_siphon is False
    assert result.market_crash is False
    assert result.reason == "insufficient_market_data_allow_open"

    with sqlite3.connect(db_path) as conn:
        saved = conn.execute(
            "SELECT allow_new_positions, reason FROM market_filter_rounds WHERE decision_round_ts = ?",
            (900_000,),
        ).fetchone()
    assert saved == (1, "insufficient_market_data_allow_open")


def test_market_filter_blocks_btc_siphon_above_half_percent_delta_gap(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100.6])

    result = MarketFilterModule(db_path=str(db_path)).run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.allow_new_positions is False
    assert result.btc_siphon is True
    assert result.market_crash is False
    assert result.reason == "btc_siphon"
    assert result.block_until == 4_500_001


def test_market_filter_blocks_for_one_hour_after_trigger(tmp_path):
    db_path = tmp_path / "klines.db"
    module = MarketFilterModule(db_path=str(db_path))
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100.6])

    triggered = module.run_round(decision_round_ts=900_000, evaluated_at=900_001)

    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DELETE FROM {allusdt_15m_ma20.KLINE_TABLE}")
        conn.execute(f"DELETE FROM {collector.BTC_15M_TABLE}")
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100])

    cooldown = module.run_round(decision_round_ts=1_800_000, evaluated_at=1_800_001)
    expired = module.run_round(decision_round_ts=4_500_000, evaluated_at=4_500_001)

    assert triggered.allow_new_positions is False
    assert cooldown.allow_new_positions is False
    assert cooldown.reason == "market_filter_cooldown_until_4500001"
    assert cooldown.block_until == 4_500_001
    assert expired.allow_new_positions is True
    assert expired.reason == "market_filter_passed"
    assert expired.block_until is None


def test_market_filter_recent_results_can_filter_to_recent_days(tmp_path):
    db_path = tmp_path / "klines.db"
    module = MarketFilterModule(db_path=str(db_path))
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        module.init_table()
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, btc_siphon, market_crash, allow_new_positions, reason, evaluated_at, block_until) VALUES (?, 0, 0, 1, 'old', ?, NULL)",
            (1_000, 1_000),
        )
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, btc_siphon, market_crash, allow_new_positions, reason, evaluated_at, block_until) VALUES (?, 0, 0, 1, 'recent', ?, NULL)",
            (2_000, 8 * 24 * 60 * 60_000),
        )

    results = module.recent_results(days=7, now_ms=9 * 24 * 60 * 60_000)

    assert [row.reason for row in results] == ["recent"]


def test_market_filter_can_load_saved_round_result(tmp_path):
    db_path = tmp_path / "klines.db"
    module = MarketFilterModule(db_path=str(db_path))
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100.6])

    module.run_round(decision_round_ts=900_000, evaluated_at=900_001)
    result = module.get_result_for_round(900_000)

    assert result is not None
    assert result.allow_new_positions is False
    assert result.reason == "btc_siphon"
    assert module.get_result_for_round(1_800_000) is None


def test_market_filter_uses_configured_thresholds_and_block_duration(tmp_path):
    db_path = tmp_path / "klines.db"
    settings_path = tmp_path / "base.db"
    set_settings({
        "btc_siphon_threshold": 0.01,
        "market_crash_threshold": 0.02,
        "block_duration_minutes": 30,
    }, str(settings_path))
    module = MarketFilterModule(str(db_path), settings_db_path=str(settings_path))
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 97.5])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 98])

    result = module.run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.btc_siphon is False
    assert result.market_crash is True
    assert result.block_until == 2_700_001


def test_market_filter_settings_defaults_and_validation(tmp_path):
    settings_path = str(tmp_path / "base.db")

    assert get_settings(settings_path)["block_duration_minutes"] == 60

    try:
        set_settings({
            "btc_siphon_threshold": 0.005,
            "market_crash_threshold": 0.03,
            "block_duration_minutes": 1.5,
        }, settings_path)
    except ValueError as exc:
        assert "整数分钟" in str(exc)
    else:
        raise AssertionError("fractional block duration should be rejected")
