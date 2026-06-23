"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app web_app run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict
from decimal import Decimal
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
from trailing_stop_tracker import TrailingStopTracker
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


def _base_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    return normalized[:-4] if normalized.endswith("USDT") else normalized


def _decimal_text_equal(left: object, right: object) -> bool:
    try:
        from decimal import Decimal

        return Decimal(str(left)).normalize() == Decimal(str(right)).normalize()
    except Exception:
        return str(left or "").strip() == str(right or "").strip()


def _filled_order_exit_reason_matches(conn: sqlite3.Connection, order: dict, time_tolerance_ms: int = 5 * 60 * 1000) -> list[dict[str, str]]:
    """Match a filled SELL order to local stop-loss / partial take-profit records.

    Local strategy tables store symbols without the USDT suffix, while Binance
    userTrades returns symbols like BTCUSDT.  The audit rows are written at order
    submission time, so a small time tolerance is used around the fill time.
    """
    if str(order.get("side", "")).upper() != "SELL":
        return []
    symbol = _base_symbol(str(order.get("symbol", "")))
    order_time = int(order.get("time") or 0)
    quantity = order.get("quantity", "")
    if not symbol or order_time <= 0 or quantity == "":
        return []

    matches: list[dict[str, str]] = []
    if _table_exists(conn, HoldingPositionScoringSystem.RECORDS_TABLE):
        rows = conn.execute(
            f"""
            SELECT created_at AS matched_at, quantity
            FROM {HoldingPositionScoringSystem.RECORDS_TABLE}
            WHERE symbol = ?
              AND side = 'SELL'
              AND created_at BETWEEN ? AND ?
            ORDER BY ABS(created_at - ?) ASC, id DESC
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        for row in rows:
            if _decimal_text_equal(row["quantity"], quantity):
                matches.append({"type": "结构止损", "matched_at": str(row["matched_at"] or "")})
                break

    if _table_exists(conn, TrailingStopTracker.CHECKS_TABLE):
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, close_quantity
            FROM {TrailingStopTracker.CHECKS_TABLE}
            WHERE symbol = ?
              AND trailing_stop_triggered = 1
              AND close_status = 'submitted'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, id DESC
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        for row in rows:
            if _decimal_text_equal(row["close_quantity"], quantity):
                matches.append({"type": "移动追踪止盈", "matched_at": str(row["matched_at"] or "")})
                break

    if _table_exists(conn, PartialTakeProfitStrategy.RECORDS_TABLE):
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, take_profit_quantity
            FROM {PartialTakeProfitStrategy.RECORDS_TABLE}
            WHERE symbol = ?
              AND side = 'SELL'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, id DESC
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        for row in rows:
            if _decimal_text_equal(row["take_profit_quantity"], quantity):
                matches.append({"type": "分批止盈", "matched_at": str(row["matched_at"] or "")})
                break
    return matches


def _filled_order_exit_reason_label(order: dict, matches: list[dict[str, str]]) -> str:
    """Return the UI label for a filled order's take-profit / stop-loss reason."""
    if str(order.get("side", "")).upper() != "SELL":
        return ""

    match_types = {match.get("type", "") for match in matches}
    if "结构止损" in match_types:
        return "结构止损"
    if "移动追踪止盈" in match_types:
        return "移动追踪止盈"
    if "分批止盈" in match_types:
        return "分批止盈"

    try:
        realized_pnl = Decimal(str(order.get("realized_pnl", "0") or "0"))
    except Exception:
        realized_pnl = Decimal("0")
    return "硬止盈" if realized_pnl > 0 else "硬止损"


def _annotate_filled_order_exit_reasons(payload: dict) -> dict:
    orders = payload.get("orders")
    if not isinstance(orders, list) or not orders:
        return payload
    try:
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            for order in orders:
                if not isinstance(order, dict):
                    continue
                matches = _filled_order_exit_reason_matches(conn, order)
                order["exit_reason"] = _filled_order_exit_reason_label(order, matches)
                order["exit_reason_matches"] = matches
    except sqlite3.OperationalError:
        for order in orders:
            if isinstance(order, dict):
                matches = []
                order.setdefault("exit_reason_matches", matches)
                order["exit_reason"] = _filled_order_exit_reason_label(order, matches)
    return payload

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


@app.get("/api/account/filled-sell-orders")
def account_filled_sell_orders_api():
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=1000, type=int)
    try:
        payload = BinanceAccountManager().futures_recent_filled_sell_orders(days=days, limit=limit)
        return jsonify(_annotate_filled_order_exit_reasons(payload))
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance filled sell orders request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


def _trailing_stop_payload() -> dict:
    trailing_stop_tracker = TrailingStopTracker(db_path=DB_PATH)
    round_ts, checks = trailing_stop_tracker.get_latest_round_checks()
    records = trailing_stop_tracker.recent_action_records(limit=100)
    return {
        "round_ts": round_ts,
        "checks": [asdict(row) for row in checks],
        "records": [asdict(row) for row in records],
    }


@app.get("/api/trailing-stop/summary")
def trailing_stop_summary_api():
    try:
        return jsonify(_trailing_stop_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


def _btc_5m_payload(page: int = 1) -> dict:
    page = max(1, page)
    page_size = 24
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        total_rows = conn.execute(
            f"""
            SELECT COUNT(1)
            FROM {collector.BTC_5M_TABLE}
            WHERE open_time >= ?
            """,
            (since_ms,),
        ).fetchone()[0]
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        page = min(page, total_pages)
        offset = (page - 1) * page_size
        table_rows = conn.execute(
            f"""
            SELECT open_time, open, high, low, close, volume, close_time
            FROM {collector.BTC_5M_TABLE}
            WHERE open_time >= ?
            ORDER BY open_time DESC
            LIMIT ? OFFSET ?
            """,
            (since_ms, page_size, offset),
        ).fetchall()
        chart_rows = conn.execute(
            f"""
            SELECT open_time, open, high, low, close, volume, close_time
            FROM {collector.BTC_5M_TABLE}
            WHERE open_time >= ?
            ORDER BY open_time DESC
            """,
            (since_ms,),
        ).fetchall()

    return {
        "days": 3,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "table_rows": [list(row) for row in table_rows],
        "chart_rows": [list(row) for row in chart_rows],
        "queried_at": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


@app.get("/api/btc/5m")
def btc_5m_api():
    page = request.args.get("page", default=1, type=int)
    try:
        return jsonify(_btc_5m_payload(page=page))
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return jsonify(
                {
                    "days": 3,
                    "page": 1,
                    "page_size": 24,
                    "total_rows": 0,
                    "total_pages": 1,
                    "table_rows": [],
                    "chart_rows": [],
                    "queried_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                }
            )
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
    abnormal_events_since_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
    cooldown = CooldownModule(db_path=DB_PATH)
    cooldown.init_table()
    should_load_abnormal_events = request.args.get("wick_refresh") == "1"
    if should_load_abnormal_events:
        events = (
            module.get_recent_events_by_symbol(symbol=symbol, limit=limit, since_ms=abnormal_events_since_ms)
            if symbol
            else module.get_recent_events(limit=limit, since_ms=abnormal_events_since_ms)
        )
    else:
        events = []
    symbols = module.get_event_symbols(since_ms=abnormal_events_since_ms)
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
    scoring_ma20_skip_record = scoring.get_latest_ma20_skip_record()
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
    score_trend_rows = []
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
    trailing_stop_tracker = TrailingStopTracker(db_path=DB_PATH)
    trailing_stop_round_ts, trailing_stop_checks = trailing_stop_tracker.get_latest_round_checks()
    trailing_stop_records = trailing_stop_tracker.recent_action_records(limit=100)

    active_tab = request.args.get("active_tab", default="", type=str).strip()
    if requested_score_trend_symbol:
        active_tab = "tab-score-trend"

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
        scoring_ma20_skip_record=scoring_ma20_skip_record,
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
        trailing_stop_round_ts=trailing_stop_round_ts,
        trailing_stop_checks=trailing_stop_checks,
        trailing_stop_records=trailing_stop_records,
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
        should_load_abnormal_events=should_load_abnormal_events,
        btc_total_pages=btc_total_pages,
    )


@app.template_filter("fmt_ms_datetime")
def fmt_ms_datetime(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
