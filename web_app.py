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

    btc_5m_rows = []
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
    except sqlite3.OperationalError:
        # table may not exist before collector.init_db() first run
        btc_5m_rows = []
        btc_total_rows = 0
        btc_total_pages = 1

    return render_template(
        "abnormal_wicks.html",
        events=events,
        limit=limit,
        symbols=symbols,
        selected_symbol=symbol,
        btc_5m_rows=btc_5m_rows,
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
