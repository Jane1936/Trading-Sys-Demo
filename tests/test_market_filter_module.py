import sqlite3

from market_filter_module import MarketFilterModule
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


def test_market_filter_still_blocks_btc_siphon_with_valid_data(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 106])

    result = MarketFilterModule(db_path=str(db_path)).run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.allow_new_positions is False
    assert result.btc_siphon is True
    assert result.market_crash is False
    assert result.reason == "btc_siphon"
