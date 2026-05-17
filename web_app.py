"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app web_app run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

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
    limit = max(1, min(limit, 1000))

    module = PreSafetyModule(db_path=DB_PATH)
    module.init_table()
    events = module.get_recent_events_by_symbol(symbol=symbol, limit=limit) if symbol else module.get_recent_events(limit=limit)
    symbols = module.get_event_symbols()

    return render_template("abnormal_wicks.html", events=events, limit=limit, symbols=symbols, selected_symbol=symbol)


@app.template_filter("fmt_ms_datetime")
def fmt_ms_datetime(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
