import sqlite3

from dynamic_add_position_threshold import DynamicAddPositionThresholdModule
from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment


def _insert_trade(conn, symbol, entry_price, created_at, status="opened"):
    conn.execute(
        f"""
        INSERT INTO {TradingExperiment.TRADES_TABLE}
        (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
         required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
         notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, reason,
         created_at, updated_at)
        VALUES (?, ?, 'BUY', ?, 80, 3, '10', '10', '1000', '10', ?, '1', '30', '120', '90', '', 'test', ?, ?)
        """,
        (symbol, created_at, status, entry_price, created_at, created_at),
    )


def test_dynamic_add_position_threshold_counts_submitted_partial_take_profit(tmp_path):
    db_path = str(tmp_path / "trading.db")
    module = DynamicAddPositionThresholdModule(db_path=db_path)
    module.init_table()

    with sqlite3.connect(db_path) as conn:
        for idx in range(45):
            _insert_trade(conn, f"S{idx}", str(100 + idx), 1_000 + idx)
        conn.execute(
            f"""
            INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE}
            (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price,
             account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl,
             take_profit_order_id, status, reason, raw_response)
            VALUES (?, ?, 'SELL', '1', '0.3', ?, '1000', '10', '20', '25', 'oid', 'submitted', 'test', '')
            """,
            ("S44", 2_000, "144"),
        )
        conn.execute(
            f"""
            INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE}
            (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price,
             account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl,
             take_profit_order_id, status, reason, raw_response)
            VALUES (?, ?, 'SELL', '1', '0.3', ?, '1000', '10', '20', '25', 'oid', 'failed', 'test', '')
            """,
            ("S43", 2_000, "143"),
        )
        conn.execute(
            f"""
            INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE}
            (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price,
             account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl,
             take_profit_order_id, status, reason, raw_response)
            VALUES (?, ?, 'SELL', '1', '0.3', ?, '1000', '10', '20', '25', 'oid', 'submitted', 'old', '')
            """,
            ("S0", 2_000, "100"),
        )

    result = module.run_round(decision_round_ts=9_000_000, evaluated_at=10_000_000)

    assert result.sample_size == 40
    assert result.success_count == 1
    assert result.success_rate == 1 / 40
    assert result.latest_trade_created_at == 1_044
    assert result.earliest_trade_created_at == 1_005


def test_dynamic_add_position_threshold_recent_results_filters_to_seven_days(tmp_path):
    db_path = str(tmp_path / "trading.db")
    module = DynamicAddPositionThresholdModule(db_path=db_path)
    module.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, sample_size, success_count, success_rate, evaluated_at, reason) VALUES (?, 1, 1, 1.0, ?, 'old')",
            (1, 1_000),
        )
        conn.execute(
            f"INSERT INTO {module.TABLE_NAME} (decision_round_ts, sample_size, success_count, success_rate, evaluated_at, reason) VALUES (?, 1, 0, 0.0, ?, 'new')",
            (2, 8 * 24 * 60 * 60_000),
        )

    rows = module.recent_results(limit=10, days=7, now_ms=8 * 24 * 60 * 60_000)

    assert [row.decision_round_ts for row in rows] == [2]


def test_dynamic_add_position_threshold_maps_success_rate_to_r_multiple():
    module = DynamicAddPositionThresholdModule

    assert module.threshold_r_multiple_for_success_rate(None) == 2.3
    assert module.threshold_r_multiple_for_success_rate(0.41) == 1.3
    assert module.threshold_r_multiple_for_success_rate(0.4) == 1.8
    assert module.threshold_r_multiple_for_success_rate(0.2) == 1.8
    assert module.threshold_r_multiple_for_success_rate(0.19) == 2.3


def test_dynamic_add_position_threshold_persists_threshold_r_multiple(tmp_path):
    db_path = str(tmp_path / "trading.db")
    module = DynamicAddPositionThresholdModule(db_path=db_path)
    module.init_table()

    with sqlite3.connect(db_path) as conn:
        for idx in range(10):
            _insert_trade(conn, f"S{idx}", str(100 + idx), 1_000 + idx)
            for status in ("submitted",):
                conn.execute(
                    f"""
                    INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE}
                    (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price,
                     account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl,
                     take_profit_order_id, status, reason, raw_response)
                    VALUES (?, ?, 'SELL', '1', '0.3', ?, '1000', '10', '20', '25', 'oid', ?, 'test', '')
                    """,
                    (f"S{idx}", 2_000, str(100 + idx), status),
                )

    result = module.run_round(decision_round_ts=9_000_000, evaluated_at=10_000_000)
    rows = module.recent_results(limit=1)

    assert result.success_rate == 1.0
    assert result.threshold_r_multiple == 1.3
    assert rows[0].threshold_r_multiple == 1.3
