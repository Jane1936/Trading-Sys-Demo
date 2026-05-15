"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app web_app run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import os

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
    limit = max(1, min(limit, 1000))

    module = PreSafetyModule(db_path=DB_PATH)
    module.init_table()
    events = module.get_recent_events(limit=limit)

    return render_template("abnormal_wicks.html", events=events, limit=limit)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
