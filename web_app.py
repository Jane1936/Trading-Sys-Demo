"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app web_app run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request
import requests

from binance_account_manager import BinanceAccountConfigError, BinanceAccountManager
from break_even_take_profit import BreakEvenTakeProfitStrategy
import collector
from cooldown_module import CooldownModule
from openable_symbol_module import OpenableSymbolModule
from pre_safety_module import PreSafetyModule
from partial_take_profit import PartialTakeProfitStrategy
from holding_position_scoring import HoldingPositionScoringSystem
from scoring_system import ScoringSystem
from trading_experiment import TradingExperiment

app = Flask(__name__)

DB_PATH = os.getenv("DB_PATH", collector.DB_PATH)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _experiment_equity_trend_rows(since_ms: int) -> list[sqlite3.Row]:
    """Return one experiment USDT equity point per recorded scan/open timestamp."""
    sources = [
        (TradingExperiment.TRADES_TABLE, "created_at"),
        (BreakEvenTakeProfitStrategy.CHECKS_TABLE, "checked_at"),
        (PartialTakeProfitStrategy.CHECKS_TABLE, "checked_at"),
    ]
    try:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            union_queries = []
            params: list[int] = []
            for table_name, ts_column in sources:
                if not _table_exists(conn, table_name):
                    continue
                union_queries.append(
                    f"""
                    SELECT {ts_column} AS recorded_at,
                           CAST(account_equity_usdt AS REAL) AS account_equity_usdt
                    FROM {table_name}
                    WHERE {ts_column} >= ?
                      AND account_equity_usdt IS NOT NULL
                      AND account_equity_usdt != ''
                    """
                )
                params.append(int(since_ms))
            if not union_queries:
                return []
            rows = conn.execute(
                f"""
                SELECT recorded_at, AVG(account_equity_usdt) AS account_equity_usdt
                FROM ({' UNION ALL '.join(union_queries)})
                WHERE account_equity_usdt IS NOT NULL
                GROUP BY recorded_at
                ORDER BY recorded_at ASC
                """,
                tuple(params),
            ).fetchall()
            return rows
    except sqlite3.OperationalError:
        return []


@app.get("/")
def index():
    return "<a href='/safety/abnormal-wicks'>abnormal wick events</a>"


@app.get("/api/safety/score-trend")
def score_trend_api():
    symbol = request.args.get("symbol", default="", type=str).strip()
    days = request.args.get("days", default=3, type=int)
    days = max(1, min(days, 30))

    scoring = ScoringSystem(db_path=DB_PATH)
    scoring.init_table()
    rows = scoring.get_total_score_trend(symbol, days=days) if symbol else []
    return jsonify(
        {
            "symbol": symbol,
            "days": days,
            "count": len(rows),
            "rows": [
                {
                    "decision_round_ts": int(row["decision_round_ts"]),
                    "total_score": int(row["total_score"]),
                }
                for row in rows
            ],
        }
    )


@app.get("/api/account/balance")
def account_balance_api():
    try:
        payload = BinanceAccountManager().futures_balance()
        return jsonify(payload)
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance balance request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/trading-experiment/run")
def trading_experiment_run_api():
    try:
        result = TradingExperiment(db_path=DB_PATH).run_latest_round()
        return jsonify(result)
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Trading experiment request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/safety/abnormal-wicks")
def abnormal_wicks():
    limit = request.args.get("limit", default=100, type=int)
    symbol = request.args.get("symbol", default="", type=str).strip()
    btc_page = request.args.get("btc_page", default=1, type=int)
    limit = max(1, min(limit, 1000))
    btc_page = max(1, btc_page)
    btc_page_size = 24

    module = PreSafetyModule(db_path=DB_PATH)
    module.init_table()
    cooldown = CooldownModule(db_path=DB_PATH)
    cooldown.init_table()
    events = module.get_recent_events_by_symbol(symbol=symbol, limit=limit) if symbol else module.get_recent_events(limit=limit)
    symbols = module.get_event_symbols()
    current_round_ts = module._decision_round_ts_ms()
    latest_round_ts, latest_round_symbols = module.get_latest_round_abnormal_symbols(decision_round_ts=current_round_ts)
    cooldown_round_ts, cooldown_symbols = cooldown.get_latest_round_symbols(decision_round_ts=current_round_ts)
    scoring = ScoringSystem(db_path=DB_PATH)
    scoring.init_table()
    score_round_ts, round_scores = scoring.get_latest_round_scores()
    score_rule2_round_ts, round_scores_rule2 = scoring.get_latest_round_scores_close_gt_ma20()
    score_rule3_round_ts, round_scores_rule3 = scoring.get_latest_round_scores_1h_close_gt_prev()
    score_rule4_round_ts, round_scores_rule4 = scoring.get_latest_round_scores_15m_bullish_3of4()
    score_rule5_round_ts, round_scores_rule5 = scoring.get_latest_round_scores_15m_close_increasing_3of4()
    score_rule6_round_ts, round_scores_rule6 = scoring.get_latest_round_scores_1m_close_gt_5m_ma20()
    score_rule7_round_ts, round_scores_rule7 = scoring.get_latest_round_scores_15m_close_near_high_2of4()
    score_rule8_round_ts, round_scores_rule8 = scoring.get_latest_round_scores_1h_latest_highest_24()
    score_rule9_round_ts, round_scores_rule9 = scoring.get_latest_round_scores_15m_close_desc_3_with_oi_45m()
    score_rule10_round_ts, round_scores_rule10 = scoring.get_latest_round_scores_1m_close_gt_60m_open_with_oi_60m()
    score_rule11_round_ts, round_scores_rule11 = scoring.get_latest_round_scores_oi_loss_rate_240m()
    score_rule12_round_ts, round_scores_rule12 = scoring.get_latest_round_scores_15m_funding_rate_4bars()
    score_rule13_round_ts, round_scores_rule13 = scoring.get_latest_round_scores_15m_bullish_volume_breakout()
    score_rule14_round_ts, round_scores_rule14 = scoring.get_latest_round_scores_15m_volume_spike_2of3()
    score_rule15_round_ts, round_scores_rule15 = scoring.get_latest_round_scores_1h_volume_spike_latest()
    score_rule16_round_ts, round_scores_rule16 = scoring.get_latest_round_scores_15m_pullback_low_volume()
    score_rule17_round_ts, round_scores_rule17 = scoring.get_latest_round_scores_15m_low_rebound_3bars()
    score_rule18_round_ts, round_scores_rule18 = scoring.get_latest_round_scores_structural_stop_loss_distance()
    score_total_round_ts, round_scores_total = scoring.get_latest_round_total_scores()
    openable = OpenableSymbolModule(db_path=DB_PATH)
    openable.init_table()
    openable_round_ts = score_total_round_ts
    openable_symbols = openable.run_round(decision_round_ts=openable_round_ts) if openable_round_ts else []
    score_trend_symbols = scoring.get_total_score_symbols()
    requested_score_trend_symbol = request.args.get("score_trend_symbol", default="", type=str).strip()
    default_score_trend_symbol = round_scores_total[0].symbol if round_scores_total else ""
    score_trend_symbol = requested_score_trend_symbol or default_score_trend_symbol
    if score_trend_symbol and score_trend_symbol not in score_trend_symbols:
        score_trend_symbols = sorted(set(score_trend_symbols) | {score_trend_symbol})
    score_trend_rows = scoring.get_total_score_trend(score_trend_symbol, days=3) if score_trend_symbol else []
    trading_experiment = TradingExperiment(db_path=DB_PATH)
    trading_records_since_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    trading_trade_records = trading_experiment.recent_trade_records(limit=100, since_ms=trading_records_since_ms)
    trading_position_snapshots = trading_experiment.latest_position_snapshots(limit=100)
    trading_error_records = trading_experiment.recent_error_records(limit=100, since_ms=trading_records_since_ms)
    trading_equity_trend_rows = _experiment_equity_trend_rows(trading_records_since_ms)
    holding_scoring = HoldingPositionScoringSystem(db_path=DB_PATH)
    holding_stop_loss_round_ts, holding_stop_loss_checks = holding_scoring.get_latest_round_checks()
    holding_stop_loss_records = holding_scoring.recent_stop_loss_records(limit=100)
    break_even_strategy = BreakEvenTakeProfitStrategy(db_path=DB_PATH)
    break_even_round_ts, break_even_checks = break_even_strategy.get_latest_round_checks()
    break_even_records = break_even_strategy.recent_records(limit=100)
    partial_take_profit_strategy = PartialTakeProfitStrategy(db_path=DB_PATH)
    partial_take_profit_round_ts, partial_take_profit_checks = partial_take_profit_strategy.get_latest_round_checks()
    partial_take_profit_records = partial_take_profit_strategy.recent_records(limit=100)

    active_tab = request.args.get("active_tab", default="", type=str).strip()
    if requested_score_trend_symbol:
        active_tab = "tab-score-trend"

    btc_5m_rows = []
    btc_chart_rows = []
    btc_total_rows = 0
    seven_days_ago_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    try:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            btc_total_rows = conn.execute(
                f"""
                SELECT COUNT(1)
                FROM {collector.BTC_5M_TABLE}
                WHERE open_time >= ?
                """,
                (seven_days_ago_ms,),
            ).fetchone()[0]
            btc_total_pages = max(1, (btc_total_rows + btc_page_size - 1) // btc_page_size)
            btc_page = min(btc_page, btc_total_pages)
            btc_offset = (btc_page - 1) * btc_page_size
            btc_5m_rows = conn.execute(
                f"""
                SELECT open_time, open, high, low, close, volume, close_time
                FROM {collector.BTC_5M_TABLE}
                WHERE open_time >= ?
                ORDER BY open_time DESC
                LIMIT ? OFFSET ?
                """,
                (seven_days_ago_ms, btc_page_size, btc_offset),
            ).fetchall()
            btc_chart_rows = conn.execute(
                f"""
                SELECT open_time, open, high, low, close, volume, close_time
                FROM {collector.BTC_5M_TABLE}
                WHERE open_time >= ?
                ORDER BY open_time DESC
                """,
                (seven_days_ago_ms,),
            ).fetchall()
    except sqlite3.OperationalError:
        # table may not exist before collector.init_db() first run
        btc_5m_rows = []
        btc_chart_rows = []
        btc_total_rows = 0
        btc_total_pages = 1

    return render_template(
        "abnormal_wicks.html",
        events=events,
        limit=limit,
        symbols=symbols,
        latest_round_ts=latest_round_ts,
        latest_round_symbols=latest_round_symbols,
        cooldown_round_ts=cooldown_round_ts,
        cooldown_symbols=cooldown_symbols,
        score_round_ts=score_round_ts,
        round_scores=round_scores,
        score_rule2_round_ts=score_rule2_round_ts,
        round_scores_rule2=round_scores_rule2,
        score_rule3_round_ts=score_rule3_round_ts,
        round_scores_rule3=round_scores_rule3,
        score_rule4_round_ts=score_rule4_round_ts,
        round_scores_rule4=round_scores_rule4,
        score_rule5_round_ts=score_rule5_round_ts,
        round_scores_rule5=round_scores_rule5,
        score_rule6_round_ts=score_rule6_round_ts,
        round_scores_rule6=round_scores_rule6,
        score_rule7_round_ts=score_rule7_round_ts,
        round_scores_rule7=round_scores_rule7,
        score_rule8_round_ts=score_rule8_round_ts,
        round_scores_rule8=round_scores_rule8,
        score_rule9_round_ts=score_rule9_round_ts,
        round_scores_rule9=round_scores_rule9,
        score_rule10_round_ts=score_rule10_round_ts,
        round_scores_rule10=round_scores_rule10,
        score_rule11_round_ts=score_rule11_round_ts,
        round_scores_rule11=round_scores_rule11,
        score_rule12_round_ts=score_rule12_round_ts,
        round_scores_rule12=round_scores_rule12,
        score_rule13_round_ts=score_rule13_round_ts,
        round_scores_rule13=round_scores_rule13,
        score_rule14_round_ts=score_rule14_round_ts,
        round_scores_rule14=round_scores_rule14,
        score_rule15_round_ts=score_rule15_round_ts,
        round_scores_rule15=round_scores_rule15,
        score_rule16_round_ts=score_rule16_round_ts,
        round_scores_rule16=round_scores_rule16,
        score_rule17_round_ts=score_rule17_round_ts,
        round_scores_rule17=round_scores_rule17,
        score_rule18_round_ts=score_rule18_round_ts,
        round_scores_rule18=round_scores_rule18,
        structural_stop_loss_coefficient=scoring.structural_stop_loss_coefficient,
        score_total_round_ts=score_total_round_ts,
        round_scores_total=round_scores_total,
        openable_round_ts=openable_round_ts,
        openable_symbols=openable_symbols,
        trading_trade_records=trading_trade_records,
        trading_position_snapshots=trading_position_snapshots,
        trading_error_records=trading_error_records,
        trading_equity_trend_rows=trading_equity_trend_rows,
        holding_stop_loss_round_ts=holding_stop_loss_round_ts,
        holding_stop_loss_checks=holding_stop_loss_checks,
        holding_stop_loss_records=holding_stop_loss_records,
        break_even_round_ts=break_even_round_ts,
        break_even_checks=break_even_checks,
        break_even_records=break_even_records,
        partial_take_profit_round_ts=partial_take_profit_round_ts,
        partial_take_profit_checks=partial_take_profit_checks,
        partial_take_profit_records=partial_take_profit_records,
        rule_score_weights=scoring.rule_score_weights,
        score_trend_symbols=score_trend_symbols,
        score_trend_symbol=score_trend_symbol,
        score_trend_rows=score_trend_rows,
        active_tab=active_tab,
        selected_symbol=symbol,
        btc_5m_rows=btc_5m_rows,
        btc_chart_rows=btc_chart_rows,
        btc_page=btc_page,
        btc_page_size=btc_page_size,
        btc_total_rows=btc_total_rows,
        btc_total_pages=btc_total_pages,
    )


@app.template_filter("fmt_ms_datetime")
def fmt_ms_datetime(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
