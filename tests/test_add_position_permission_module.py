import sqlite3

import allusdt_15m_ma20
import collector
from add_position_permission_module import AddPositionPermissionModule


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


def test_add_position_permission_allows_when_alt_outperforms_btc(tmp_path):
    db_path = tmp_path / "market.db"
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 101])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100])

    result = AddPositionPermissionModule(db_path=str(db_path)).run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.allow_add_positions is True
    assert result.alt_outperform_btc is True
    assert result.reason == "alt_outperform_btc_allow_add_position"


def test_add_position_permission_blocks_without_alt_outperformance(tmp_path):
    db_path = tmp_path / "market.db"
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        _insert_rows(conn, allusdt_15m_ma20.KLINE_TABLE, [100, 100, 100, 100])
        _insert_rows(conn, collector.BTC_15M_TABLE, [100, 100, 100, 100])

    result = AddPositionPermissionModule(db_path=str(db_path)).run_round(decision_round_ts=900_000, evaluated_at=900_001)

    assert result.allow_add_positions is False
    assert result.alt_outperform_btc is False
    assert result.reason == "alt_not_outperform_btc_block_add_position"


def test_add_position_permission_recent_results_can_filter_to_recent_days(tmp_path):
    db_path = tmp_path / "market.db"
    module = AddPositionPermissionModule(db_path=str(db_path))
    with sqlite3.connect(db_path) as conn:
        _init_source_tables(conn)
        module.init_table()
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, alt_outperform_btc, allow_add_positions, reason, evaluated_at) VALUES (?, 0, 0, 'old', ?)",
            (1_000, 1_000),
        )
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, alt_outperform_btc, allow_add_positions, reason, evaluated_at) VALUES (?, 1, 1, 'recent', ?)",
            (2_000, 8 * 24 * 60 * 60_000),
        )

    results = module.recent_results(days=7, now_ms=9 * 24 * 60 * 60_000)

    assert [row.reason for row in results] == ["recent"]
