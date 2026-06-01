"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app web_app run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template, request

import collector
from pre_safety_module import PreSafetyModule
from scoring_system import ScoringSystem

app = Flask(__name__)

DB_PATH = os.getenv("DB_PATH", collector.DB_PATH)


@app.get("/")
def index():
    return "<a href='/safety/abnormal-wicks'>abnormal wick events</a>"


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
    events = module.get_recent_events_by_symbol(symbol=symbol, limit=limit) if symbol else module.get_recent_events(limit=limit)
    symbols = module.get_event_symbols()
    current_round_ts = module._decision_round_ts_ms()
    latest_round_ts, latest_round_symbols = module.get_latest_round_abnormal_symbols(decision_round_ts=current_round_ts)
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
    score_total_round_ts, round_scores_total = scoring.get_latest_round_total_scores()

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
        score_total_round_ts=score_total_round_ts,
        round_scores_total=round_scores_total,
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
