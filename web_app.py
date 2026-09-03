"""Minimal Flask web app for abnormal wick events.

Run:
    flask --app 'web_app:create_app()' run --host 0.0.0.0 --port 5000
"""

from __future__ import annotations

import ast
from io import BytesIO
import os
import sqlite3
from zipfile import ZIP_DEFLATED, ZipFile
from dataclasses import asdict
from decimal import Decimal
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request, send_file
import requests

from binance_account_manager import BinanceAccountConfigError, BinanceAccountManager
from break_even_take_profit import BreakEvenTakeProfitStrategy
import collector
import db_config
import feature_flags
from config_database import initialize_config_database
from cooldown_module import CooldownModule
from openable_symbol_module import OpenableSymbolModule
from openable_symbol_settings import get_settings as get_openable_symbol_settings, set_settings as set_openable_symbol_settings
from pre_safety_module import PreSafetyModule
from partial_take_profit import PartialTakeProfitStrategy
from dynamic_profit_protection import DynamicProfitProtection
from hard_take_profit import HardTakeProfit
from hard_take_profit_settings import (
    get_settings as get_hard_take_profit_settings,
    set_settings as set_hard_take_profit_settings,
)
from dynamic_profit_protection_settings import (
    get_settings as get_dynamic_profit_protection_settings,
    set_settings as set_dynamic_profit_protection_settings,
)
from trailing_stop_tracker import TrailingStopTracker
from trailing_reduction_tracker import TrailingReductionTracker
from holding_position_scoring import HoldingPositionScoringSystem
from scoring_system import (
    ScoringSystem,
    get_rule_score_weight_settings,
    set_rule_score_weight_settings,
)
from scoring_rule_election import get_settings as get_rule_election_settings, set_settings as set_rule_election_settings
from trading_experiment import TradingExperiment
import real_trading
from market_filter_module import MarketFilterModule
from market_filter_settings import (
    get_settings as get_market_filter_settings,
    set_settings as set_market_filter_settings,
)
from weak_market_profit_adjustment import (
    WeakMarketProfitAdjustmentModule,
    get_settings as get_weak_market_profit_settings,
    set_settings as set_weak_market_profit_settings,
)
from reduction_module_settings import (
    get_settings as get_reduction_module_settings,
    set_settings as set_reduction_module_settings,
)
from position_limit_settings import (
    get_settings as get_position_limit_settings,
    set_settings as set_position_limit_settings,
)
from add_position_permission_module import AddPositionPermissionModule
from dynamic_open_threshold import (
    DynamicOpenThresholdModule,
    get_settings as get_dynamic_open_threshold_settings,
    set_settings as set_dynamic_open_threshold_settings,
)
from dynamic_add_position_threshold import DynamicAddPositionThresholdModule
from zombie_force_liquidation import ZombieForceLiquidationModule
from sqlite_recovery import (
    is_malformed_database_error,
    quick_check_sqlite_database,
)

app = Flask(__name__)

DB_PATH = db_config.BASE_DB_PATH
BASE_DB_PATH = db_config.BASE_DB_PATH
CONFIG_DB_PATH = db_config.CONFIG_DB_PATH
SCORING_DB_PATH = db_config.SCORING_DB_PATH
TRADING_DB_PATH = db_config.TRADING_DB_PATH
MARKET_DB_PATH = db_config.MARKET_DB_PATH
DEFAULT_TRADING_EQUITY_USDT = Decimal("1000")
WEB_SQLITE_QUICK_CHECK_ON_REQUEST = (
    os.getenv("WEB_SQLITE_QUICK_CHECK_ON_REQUEST", "").strip().lower()
    in {"1", "true", "yes", "on"}
)
_db_recovery_checked_path: str | None = None


def create_app() -> Flask:
    """Initialize and verify all schemas before exposing any Web endpoint.

    Gunicorn must use this factory rather than importing the global ``app``
    directly.  Keeping initialization in the process factory also avoids
    mutating production databases when test code merely imports this module.
    """
    from app import initialize_worker_databases

    initialize_worker_databases()
    return app


def _base_db_path() -> str:
    return DB_PATH

def _scoring_db_path() -> str:
    return DB_PATH if DB_PATH != BASE_DB_PATH else SCORING_DB_PATH

def _trading_db_path() -> str:
    return DB_PATH if DB_PATH != BASE_DB_PATH else TRADING_DB_PATH

def _market_db_path() -> str:
    return DB_PATH if DB_PATH != BASE_DB_PATH else MARKET_DB_PATH



def _current_open_block_notice(openable_round_ts, market_filter_results, dynamic_open_threshold_results):
    """Build the current-round opening restriction shown above openable symbols.

    A dynamic threshold can leave the candidate list empty while still reporting
    ``allow_new_positions=True``.  Surface that minimum explicitly so an empty
    list is not mistaken for an account-balance problem.
    """
    if not openable_round_ts:
        return None
    market_result = next(
        (row for row in market_filter_results if row.decision_round_ts == openable_round_ts),
        None,
    )
    dynamic_result = next(
        (row for row in dynamic_open_threshold_results if row.decision_round_ts == openable_round_ts),
        None,
    )
    reasons = []
    if market_result is not None and not market_result.allow_new_positions:
        reasons.append(f"独立市场过滤模块：{market_result.reason}")
    if dynamic_result is not None and not dynamic_result.allow_new_positions:
        reasons.append(f"动态开仓门槛：{dynamic_result.reason}")
    elif dynamic_result is not None and getattr(dynamic_result, "min_open_total_score", None) is not None:
        reasons.append(
            f"动态开仓门槛：本轮总分需≥{dynamic_result.min_open_total_score}"
            f"（{dynamic_result.reason}）"
        )
    if not reasons:
        return None
    return "；".join(reasons)

def _ensure_web_database_usable() -> None:
    global _db_recovery_checked_path
    if _db_recovery_checked_path == DB_PATH:
        return
    ok, detail = quick_check_sqlite_database(DB_PATH)
    if not ok:
        app.logger.error(
            "SQLite quick_check failed for %s: %s; live quarantine disabled",
            DB_PATH,
            detail,
        )
    _db_recovery_checked_path = DB_PATH


@app.before_request
def _recover_malformed_database_before_request() -> None:
    if WEB_SQLITE_QUICK_CHECK_ON_REQUEST:
        _ensure_web_database_usable()


@app.errorhandler(sqlite3.DatabaseError)
def _handle_sqlite_database_error(exc: sqlite3.DatabaseError):
    if not is_malformed_database_error(exc):
        return jsonify({"error": str(exc)}), 502
    _fence_malformed_databases()
    app.logger.exception("Malformed SQLite database detected and fenced for worker recovery")
    return jsonify({"error": "SQLite database is malformed and is being automatically recovered."}), 503


def _fence_malformed_databases() -> list[str]:
    fenced = []
    for path in db_config.DB_LABELS.values():
        ok, detail = quick_check_sqlite_database(path)
        if ok:
            continue
        marker = db_config.database_recovery_marker(path)
        with open(marker, "w", encoding="utf-8") as fh:
            fh.write(f"web detected: {detail}\n")
        fenced.append(path)
    return fenced



def _safe_page_module(label: str, loader, default):
    """Load one dashboard module without letting its failure break the page."""
    try:
        return loader(), None
    except Exception as exc:
        error_text = str(exc)
        if is_malformed_database_error(exc):
            _fence_malformed_databases()
            checks = []
            for path in db_config.DB_LABELS.values():
                ok, detail = quick_check_sqlite_database(path)
                if not ok:
                    checks.append(f"{path}: {detail}")
            error_text += "; 已禁止新业务访问，worker 正在自动恢复"
            if checks:
                error_text += "；检查失败：" + "; ".join(checks)
        app.logger.exception("Dashboard module failed: %s", label)
        return default, {"label": label, "error": error_text}

def _score_band_context() -> tuple[list[dict], str, str, int]:
    module = OpenableSymbolModule(db_path=_scoring_db_path())
    bands = [
        {
            "label": band.label,
            "lower": band.lower,
            "upper": band.upper,
            "distance_threshold": band.distance_threshold,
            "tier_leverages": band.tier_leverages,
            "css_class": band.css_class,
            "chart_color": band.chart_color,
            "chart_border_color": band.chart_border_color,
        }
        for band in module.configured_score_bands()
    ]
    threshold_text = "，".join(
        f"{band['lower']}-{band['upper']}分≤{band['distance_threshold'] * 100:.0f}%" for band in bands
    )
    leverage_text = "；".join(
        f"{band['lower']}-{band['upper']}：A/B/C/D="
        f"{band['tier_leverages']['A档']}/{band['tier_leverages']['B档']}/"
        f"{band['tier_leverages']['C档']}/{band['tier_leverages']['D档']}"
        for band in bands
    )
    return bands, threshold_text, leverage_text, min(band["lower"] for band in bands)


def _table_exists(
    conn: sqlite3.Connection, table_name: str, *, schema: str = "main"
) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {db_config.quote_identifier(schema)}.sqlite_master "
        "WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _qualified_table(schema: str, table_name: str) -> str:
    return (
        f"{db_config.quote_identifier(schema)}."
        f"{db_config.quote_identifier(table_name)}"
    )


def _experiment_equity_trend_rows(
    since_ms: int, db_path: str | None = None
) -> list[sqlite3.Row]:
    """Return one experiment USDT equity point per recorded scan/open timestamp."""
    sources = [
        (TradingExperiment.TRADES_TABLE, "created_at"),
        (BreakEvenTakeProfitStrategy.CHECKS_TABLE, "checked_at"),
        (PartialTakeProfitStrategy.CHECKS_TABLE, "checked_at"),
        (DynamicProfitProtection.CHECKS_TABLE, "checked_at"),
    ]
    try:
        with db_config.connect_sqlite(db_path or _trading_db_path()) as conn:
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
    except sqlite3.DatabaseError as exc:
        app.logger.warning("Skipping experiment equity trend query due to SQLite error: %s", exc)
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


def _format_decimal_display(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _trading_used_margin(position_snapshots: list[object]) -> Decimal:
    return TradingExperiment()._reserved_margin_from_positions(position_snapshots)


def _trading_unrealized_pnl(position_snapshots: list[object]) -> Decimal:
    return TradingExperiment()._unrealized_pnl_from_positions(position_snapshots)


def _trading_used_margin_text(position_snapshots: list[object]) -> str:
    return _format_decimal_display(_trading_used_margin(position_snapshots))


def _trading_open_increase_blocked(account_equity_usdt: object, position_snapshots: list[object]) -> bool:
    try:
        account_equity = Decimal(str(account_equity_usdt))
    except Exception:
        return False
    if not account_equity.is_finite() or account_equity <= 0:
        return False
    return _trading_used_margin(position_snapshots) > account_equity + _trading_unrealized_pnl(position_snapshots)


def _latest_trading_equity_usdt(equity_trend_rows: list[object]) -> object:
    if not equity_trend_rows:
        return DEFAULT_TRADING_EQUITY_USDT
    latest_row = equity_trend_rows[-1]
    try:
        equity = latest_row["account_equity_usdt"]
    except Exception:
        equity = getattr(latest_row, "account_equity_usdt", None)
    return equity if equity not in (None, "") else DEFAULT_TRADING_EQUITY_USDT


def _raw_response_contains_order_id(raw_response: object, order_id: object) -> bool:
    """Return whether a stored strategy raw response mentions a Binance order id."""
    expected = str(order_id or "").strip()
    if not expected:
        return False
    raw_text = str(raw_response or "")
    if not raw_text:
        return False

    def iter_values(value: object):
        if isinstance(value, dict):
            for key, item in value.items():
                yield key, item
                yield from iter_values(item)
        elif isinstance(value, list):
            for item in value:
                yield from iter_values(item)

    for part in raw_text.split(" | "):
        try:
            parsed = ast.literal_eval(part)
        except Exception:
            continue
        for key, value in iter_values(parsed):
            if str(key) == "orderId" and str(value).strip() == expected:
                return True
    return False


def _filled_order_exit_reason_matches(
    conn: sqlite3.Connection,
    order: dict,
    time_tolerance_ms: int = 5 * 60 * 1000,
    *,
    core_schema: str = "main",
    info_schema: str = "main",
) -> list[dict[str, str]]:
    """Match a filled order to local strategy records.

    Local strategy tables store symbols without the USDT suffix, while Binance
    userTrades returns symbols like BTCUSDT.  The audit rows are written at order
    submission time, so a small time tolerance is used around the fill time.
    Force-liquidation records are matched before BUY fills are checked against
    position increase records.  Automated hard take-profit and force-liquidation
    records prefer stored Binance order ids, with symbol/time/quantity retained
    as the fallback for legacy or incomplete rows.
    """
    side = str(order.get("side", "")).upper()
    if side not in {"SELL", "BUY"}:
        return []
    symbol = _base_symbol(str(order.get("symbol", "")))
    order_time = int(order.get("time") or 0)
    quantity = order.get("quantity", "")
    if not symbol or order_time <= 0 or quantity == "":
        return []

    matches: list[dict[str, str]] = []
    if _table_exists(conn, ZombieForceLiquidationModule.RECORDS_TABLE, schema=core_schema):
        zombie_table = _qualified_table(core_schema, ZombieForceLiquidationModule.RECORDS_TABLE)
        zombie_columns = {
            row["name"]
            for row in conn.execute(
                f"PRAGMA {db_config.quote_identifier(core_schema)}.table_info("
                f"{db_config.quote_identifier(ZombieForceLiquidationModule.RECORDS_TABLE)})"
            ).fetchall()
        }
        zombie_order_id_select = "order_id" if "order_id" in zombie_columns else "'' AS order_id"
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, quantity, {zombie_order_id_select}, raw_response
            FROM {zombie_table}
            WHERE symbol = ?
              AND side = ?
              AND status = 'submitted'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, id DESC
            """,
            (symbol, side, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        order_id = order.get("order_id", "")
        for row in rows:
            stored_order_id = str(row["order_id"] or "").strip()
            expected_order_id = str(order_id or "").strip()
            if (stored_order_id and stored_order_id == expected_order_id) or _raw_response_contains_order_id(row["raw_response"], order_id) or _decimal_text_equal(row["quantity"], quantity):
                matches.append({"type": "僵尸强平", "matched_at": str(row["matched_at"] or "")})
                break

    if _table_exists(
        conn,
        HoldingPositionScoringSystem.REDUCTION_STOP_FAILURE_LIQUIDATIONS_TABLE,
        schema=core_schema,
    ):
        liquidation_table = _qualified_table(
            core_schema,
            HoldingPositionScoringSystem.REDUCTION_STOP_FAILURE_LIQUIDATIONS_TABLE,
        )
        rows = conn.execute(
            f"""
            SELECT created_at AS matched_at, quantity, liquidation_market_order_id
            FROM {liquidation_table}
            WHERE symbol = ?
              AND side = ?
              AND status = 'submitted'
              AND created_at BETWEEN ? AND ?
            ORDER BY ABS(created_at - ?) ASC, id DESC
            """,
            (symbol, side, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        expected_order_id = str(order.get("order_id", "") or "").strip()
        for row in rows:
            stored_order_id = str(row["liquidation_market_order_id"] or "").strip()
            if (stored_order_id and stored_order_id == expected_order_id) or _decimal_text_equal(row["quantity"], quantity):
                matches.append({"type": "减仓失败强平", "matched_at": str(row["matched_at"] or "")})
                break

    if side == "BUY":
        if _table_exists(
            conn, HoldingPositionScoringSystem.INCREASE_RECORDS_TABLE, schema=core_schema
        ):
            increase_table = _qualified_table(
                core_schema, HoldingPositionScoringSystem.INCREASE_RECORDS_TABLE
            )
            rows = conn.execute(
                f"""
                SELECT created_at AS matched_at, increased_quantity
                FROM {increase_table}
                WHERE symbol = ?
                  AND status = 'submitted'
                  AND created_at BETWEEN ? AND ?
                ORDER BY ABS(created_at - ?) ASC, id DESC
                """,
                (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
            ).fetchall()
            for row in rows:
                if _decimal_text_equal(row["increased_quantity"], quantity):
                    matches.append({"type": "加仓", "matched_at": str(row["matched_at"] or "")})
                    break
        return matches

    if _table_exists(conn, HardTakeProfit.RECORDS_TABLE, schema=info_schema):
        hard_take_profit_table = _qualified_table(
            info_schema, HardTakeProfit.RECORDS_TABLE
        )
        # ``CREATE TABLE IF NOT EXISTS`` does not migrate databases created by
        # older deployments.  In particular, production may already have this
        # table without ``close_order_id``.  Keep the analysis query compatible
        # with those rows instead of letting one optional column abort all
        # filled-order annotations.
        hard_take_profit_columns = {
            row["name"]
            for row in conn.execute(
                f"PRAGMA {db_config.quote_identifier(info_schema)}.table_info("
                f"{db_config.quote_identifier(HardTakeProfit.RECORDS_TABLE)})"
            ).fetchall()
        }
        hard_take_profit_order_id_select = (
            "close_order_id" if "close_order_id" in hard_take_profit_columns
            else "'' AS close_order_id"
        )
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, close_quantity, {hard_take_profit_order_id_select}
            FROM {hard_take_profit_table}
            WHERE symbol = ?
              AND triggered = 1
              AND close_status = 'submitted'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, id DESC
            """,
            (
                symbol,
                order_time - time_tolerance_ms,
                order_time + time_tolerance_ms,
                order_time,
            ),
        ).fetchall()
        expected_order_id = str(order.get("order_id", "") or "").strip()
        for row in rows:
            stored_order_id = str(row["close_order_id"] or "").strip()
            if (stored_order_id and stored_order_id == expected_order_id) or _decimal_text_equal(
                row["close_quantity"], quantity
            ):
                matches.append(
                    {"type": "自动化硬止盈", "matched_at": str(row["matched_at"] or "")}
                )
                break

    if _table_exists(conn, HoldingPositionScoringSystem.RECORDS_TABLE, schema=core_schema):
        stop_loss_table = _qualified_table(core_schema, HoldingPositionScoringSystem.RECORDS_TABLE)
        rows = conn.execute(
            f"""
            SELECT created_at AS matched_at, quantity, reason
            FROM {stop_loss_table}
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

    if _table_exists(conn, HoldingPositionScoringSystem.REDUCTION_RECORDS_TABLE, schema=core_schema):
        reduction_table = _qualified_table(
            core_schema, HoldingPositionScoringSystem.REDUCTION_RECORDS_TABLE
        )
        rows = conn.execute(
            f"""
            SELECT created_at AS matched_at, reduced_quantity
            FROM {reduction_table}
            WHERE symbol = ?
              AND side = 'SELL'
              AND created_at BETWEEN ? AND ?
            ORDER BY ABS(created_at - ?) ASC, id DESC
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        for row in rows:
            if _decimal_text_equal(row["reduced_quantity"], quantity):
                matches.append({"type": "减仓", "matched_at": str(row["matched_at"] or "")})
                break

    if _table_exists(conn, TrailingReductionTracker.RECORDS_TABLE, schema=info_schema):
        trailing_reduction_table = _qualified_table(
            info_schema, TrailingReductionTracker.RECORDS_TABLE
        )
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, reduced_quantity, market_order_id
            FROM {trailing_reduction_table}
            WHERE symbol = ?
              AND status = 'submitted'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, id DESC
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        order_id = str(order.get("order_id", "") or "").strip()
        for row in rows:
            stored_order_id = str(row["market_order_id"] or "").strip()
            if (stored_order_id and stored_order_id == order_id) or _decimal_text_equal(row["reduced_quantity"], quantity):
                matches.append({"type": "移动追踪减仓", "matched_at": str(row["matched_at"] or "")})
                break

    if _table_exists(conn, DynamicProfitProtection.RECORDS_TABLE, schema=info_schema):
        dynamic_profit_table = _qualified_table(
            info_schema, DynamicProfitProtection.RECORDS_TABLE
        )
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, close_quantity
            FROM {dynamic_profit_table}
            WHERE symbol = ?
              AND triggered = 1
              AND close_status = 'submitted'
              AND checked_at BETWEEN ? AND ?
            ORDER BY ABS(checked_at - ?) ASC, checked_at DESC
            LIMIT 5
            """,
            (symbol, order_time - time_tolerance_ms, order_time + time_tolerance_ms, order_time),
        ).fetchall()
        for row in rows:
            if _decimal_text_equal(row["close_quantity"], quantity):
                matches.append({"type": "动态利润保护", "matched_at": str(row["matched_at"] or "")})

    if _table_exists(conn, TrailingStopTracker.RECORDS_TABLE, schema=info_schema):
        trailing_stop_table = _qualified_table(
            info_schema, TrailingStopTracker.RECORDS_TABLE
        )
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, close_quantity
            FROM {trailing_stop_table}
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

    if _table_exists(conn, PartialTakeProfitStrategy.RECORDS_TABLE, schema=info_schema):
        partial_take_profit_table = _qualified_table(
            info_schema, PartialTakeProfitStrategy.RECORDS_TABLE
        )
        rows = conn.execute(
            f"""
            SELECT checked_at AS matched_at, take_profit_quantity
            FROM {partial_take_profit_table}
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
    side = str(order.get("side", "")).upper()
    match_types = {match.get("type", "") for match in matches}
    if side not in {"SELL", "BUY"}:
        return ""

    if "自动化硬止盈" in match_types:
        return "自动化硬止盈"
    if "减仓失败强平" in match_types:
        return "减仓失败强平"
    if "僵尸强平" in match_types:
        return "僵尸强平"
    if "结构止损" in match_types:
        return "结构止损"
    if "减仓" in match_types:
        return "减仓"
    if "移动追踪减仓" in match_types:
        return "移动追踪减仓"
    if "动态利润保护" in match_types:
        return "动态利润保护"
    if "移动追踪止盈" in match_types:
        return "移动追踪止盈"
    if "分批止盈" in match_types:
        return "分批止盈"
    if side == "BUY" and "加仓" in match_types:
        return "加仓"

    try:
        realized_pnl = Decimal(str(order.get("realized_pnl", "0") or "0"))
    except Exception:
        realized_pnl = Decimal("0")
    if side == "BUY":
        return ""
    return "硬止盈" if realized_pnl > 0 else "硬止损"


def _filled_order_open_details(
    conn: sqlite3.Connection,
    order: dict,
    *,
    core_schema: str = "main",
    scoring_schema: str = "main",
) -> dict:
    """Return experiment and per-rule scoring details for a Binance fill."""
    empty = {
        "open_total_score": None,
        "open_leverage": None,
        "open_score_matched_at": "",
        **{f"open_rule{i}_score": None for i in range(1, 19)},
    }
    if not _table_exists(conn, TradingExperiment.TRADES_TABLE, schema=core_schema):
        return empty
    trades_table = _qualified_table(core_schema, TradingExperiment.TRADES_TABLE)
    trade_columns = {
        row["name"]
        for row in conn.execute(f"PRAGMA {core_schema}.table_info({TradingExperiment.TRADES_TABLE})").fetchall()
    }
    symbol = _base_symbol(str(order.get("symbol", "")))
    order_time = int(order.get("time") or 0)
    if not symbol or order_time <= 0:
        return empty

    optional_columns = [name for name in ("decision_round_ts", "leverage") if name in trade_columns]
    selected_columns = ", ".join(["total_score", "created_at", *optional_columns])

    side = str(order.get("side", "")).upper()
    if side == "BUY":
        row = conn.execute(
            f"""
            SELECT {selected_columns}
            FROM {trades_table}
            WHERE symbol = ?
              AND status = 'opened'
              AND created_at BETWEEN ? AND ?
              AND total_score IS NOT NULL
            ORDER BY ABS(created_at - ?) ASC, id DESC
            LIMIT 1
            """,
            (symbol, order_time - 5 * 60 * 1000, order_time + 5 * 60 * 1000, order_time),
        ).fetchone()
    else:
        row = conn.execute(
            f"""
            SELECT {selected_columns}
            FROM {trades_table}
            WHERE symbol = ?
              AND status = 'opened'
              AND created_at <= ?
              AND total_score IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (symbol, order_time),
        ).fetchone()
    if row is None:
        return empty

    details = dict(empty)
    details["open_total_score"] = int(row["total_score"])
    details["open_leverage"] = row["leverage"] if "leverage" in optional_columns else None
    details["open_score_matched_at"] = str(row["created_at"] or "")
    decision_round_ts = row["decision_round_ts"] if "decision_round_ts" in optional_columns else None
    if decision_round_ts is not None and _table_exists(conn, "symbol_total_scores", schema=scoring_schema):
        score_row = conn.execute(
            f"SELECT * FROM {_qualified_table(scoring_schema, 'symbol_total_scores')} "
            "WHERE symbol = ? AND decision_round_ts = ? LIMIT 1",
            (symbol, int(decision_round_ts)),
        ).fetchone()
        if score_row is not None:
            for rule_id in range(1, 19):
                details[f"open_rule{rule_id}_score"] = int(score_row[f"rule{rule_id}_score"])
    return details


def _score_band_label(total_score: int | None) -> str:
    if total_score is None:
        return ""
    band = OpenableSymbolModule.score_band_config_for_total(int(total_score))
    return band.label if band is not None else "未命中开仓档位"


def _annotate_filled_order_exit_reasons(
    payload: dict, *, trading_db_path: str | None = None
) -> dict:
    """Annotate fills from the strategy databases belonging to their account."""
    orders = payload.get("orders")
    if not isinstance(orders, list) or not orders:
        return payload
    try:
        trading_db_path = trading_db_path or _trading_db_path()
        core_db_path = db_config.trading_core_path(trading_db_path)
        info_db_path = db_config.trading_info_path(trading_db_path)
        with db_config.connect_sqlite(trading_db_path) as conn:
            conn.row_factory = sqlite3.Row
            core_schema = "main"
            if os.path.realpath(core_db_path) != os.path.realpath(trading_db_path):
                db_config.attach_databases(conn, [("trading_core", core_db_path)])
                core_schema = "trading_core"
            info_schema = "main"
            if os.path.realpath(info_db_path) != os.path.realpath(trading_db_path):
                db_config.attach_databases(conn, [("trading_info", info_db_path)])
                info_schema = "trading_info"
            scoring_db_path = _scoring_db_path()
            if os.path.realpath(scoring_db_path) != os.path.realpath(trading_db_path):
                db_config.attach_databases(conn, [("scoring", scoring_db_path)])
                scoring_schema = "scoring"
            else:
                scoring_schema = "main"
            for order in orders:
                if not isinstance(order, dict):
                    continue
                matches = _filled_order_exit_reason_matches(
                    conn, order, core_schema=core_schema, info_schema=info_schema
                )
                order["exit_reason"] = _filled_order_exit_reason_label(order, matches)
                order["exit_reason_matches"] = matches
                open_details = _filled_order_open_details(
                    conn, order, core_schema=core_schema, scoring_schema=scoring_schema
                )
                order.update(open_details)
                order["open_score_band"] = _score_band_label(open_details["open_total_score"])
    except sqlite3.DatabaseError:
        for order in orders:
            if isinstance(order, dict):
                matches = []
                order.setdefault("exit_reason_matches", matches)
                order["exit_reason"] = _filled_order_exit_reason_label(order, matches)
                order.setdefault("open_total_score", None)
                order.setdefault("open_score_band", "")
                order.setdefault("open_score_matched_at", "")
                order.setdefault("open_leverage", None)
                for rule_id in range(1, 19):
                    order.setdefault(f"open_rule{rule_id}_score", None)
    return payload

@app.get("/")
def index():
    return "<a href='/safety/abnormal-wicks'>abnormal wick events</a>"


@app.get("/api/safety/score-trend")
def score_trend_api():
    symbol = request.args.get("symbol", default="", type=str).strip()
    days = request.args.get("days", default=3, type=int)
    days = max(1, min(days, 30))

    scoring = ScoringSystem(db_path=_scoring_db_path())
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


@app.get("/api/live/account/balance")
def live_account_balance_api():
    """Read production account balances without changing demo trading mode."""
    try:
        return jsonify(BinanceAccountManager.live().futures_balance())
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance live balance request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/api/account/filled-sell-orders")
def account_filled_sell_orders_api():
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=1000, type=int)
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    range_requested = "start_time" in request.args or "end_time" in request.args
    try:
        manager = BinanceAccountManager()
        if range_requested:
            if start_time is None or end_time is None:
                return jsonify({"error": "valid start_time and end_time must be provided together"}), 400
            payload = manager.futures_filled_orders(start_time=start_time, end_time=end_time, limit=limit)
        else:
            payload = manager.futures_recent_filled_sell_orders(days=days, limit=limit)
        return jsonify(_annotate_filled_order_exit_reasons(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance filled sell orders request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/api/live/account/filled-orders")
def live_account_filled_orders_api():
    """Read production fills using only the production API credentials."""
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=1000, type=int)
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    range_requested = "start_time" in request.args or "end_time" in request.args
    try:
        manager = BinanceAccountManager.live()
        if range_requested:
            if start_time is None or end_time is None:
                return jsonify({"error": "valid start_time and end_time must be provided together"}), 400
            payload = manager.futures_filled_orders(start_time=start_time, end_time=end_time, limit=limit)
        else:
            payload = manager.futures_recent_filled_orders(days=days, limit=limit)
        return jsonify(
            _annotate_filled_order_exit_reasons(
                payload, trading_db_path=db_config.REAL_TRADING_DB_PATH
            )
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance live filled orders request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502
    except Exception as exc:
        app.logger.exception("Unexpected live filled-orders query failure")
        return jsonify({"error": f"Unexpected live filled-orders query failure: {exc}"}), 500


FILLED_ORDER_EXPORT_COLUMNS = (
    ("成交时间", "time"),
    ("symbol", "symbol"),
    ("开仓评分档位", "open_score_band"),
    ("开仓杠杆大小", "open_leverage"),
    ("开仓总分", "open_total_score"),
    ("止盈/止损原因", "exit_reason"),
    ("方向", "side"),
    ("order_id", "order_id"),
    ("成交价格", "price"),
    ("成交数量", "quantity"),
    ("成交额", "quote_quantity"),
    ("已实现盈亏", "realized_pnl"),
    ("手续费", "commission"),
    *((f"评分规则{i}", f"open_rule{i}_score") for i in range(1, 19)),
    ("手续费资产", "commission_asset"),
    ("maker", "maker"),
    ("trade_id", "trade_id"),
)


def _filled_orders_excel(orders: list[dict]) -> BytesIO:
    """Build an Excel workbook containing the filled orders visible in the UI."""
    def excel_column_name(number: int) -> str:
        name = ""
        while number:
            number, remainder = divmod(number - 1, 26)
            name = chr(65 + remainder) + name
        return name

    rows = [[label for label, _ in FILLED_ORDER_EXPORT_COLUMNS]]
    for order in orders:
        values = []
        for _, key in FILLED_ORDER_EXPORT_COLUMNS:
            value = order.get(key, "")
            if key == "time":
                try:
                    value = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                except (TypeError, ValueError, OSError):
                    value = ""
            elif key == "maker":
                value = "是" if value else "否"
            values.append(value if value is not None else "")
        rows.append(values)

    from xml.sax.saxutils import escape

    xml_rows = []
    for row_number, row in enumerate(rows, start=1):
        cells = []
        for column_number, value in enumerate(row, start=1):
            column_name = excel_column_name(column_number)
            style = ' s="1"' if row_number == 1 else ""
            safe_value = escape(str(value), {'"': "&quot;"})
            cells.append(
                f'<c r="{column_name}{row_number}" t="inlineStr"{style}><is><t>{safe_value}</t></is></c>'
            )
        xml_rows.append(f'<row r="{row_number}">{"".join(cells)}</row>')

    last_row = len(rows)
    worksheet = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>
  <cols><col min="1" max="1" width="23" customWidth="1"/><col min="2" max="15" width="18" customWidth="1"/></cols>
  <sheetData>{''.join(xml_rows)}</sheetData><autoFilter ref="A1:O{last_row}"/>
</worksheet>'''

    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>''')
        archive.writestr("_rels/.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>''')
        archive.writestr("xl/workbook.xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="已成交订单" sheetId="1" r:id="rId1"/></sheets></workbook>''')
        archive.writestr("xl/_rels/workbook.xml.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>''')
        archive.writestr("xl/styles.xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="2"><font/><font><b/></font></fonts><fills count="1"><fill><patternFill patternType="none"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="2"><xf xfId="0"/><xf xfId="0" fontId="1" applyFont="1"/></cellXfs></styleSheet>''')
        archive.writestr("xl/worksheets/sheet1.xml", worksheet)
    output.seek(0)
    return output


@app.post("/api/account/filled-orders/export")
def account_filled_orders_export_api():
    payload = request.get_json(silent=True)
    orders = payload.get("orders") if isinstance(payload, dict) else None
    if not isinstance(orders, list) or not orders:
        return jsonify({"error": "没有可导出的已成交订单数据"}), 400
    if len(orders) > 10000 or any(not isinstance(order, dict) for order in orders):
        return jsonify({"error": "导出数据格式无效或超过 10000 条限制"}), 400

    filename = f"filled_orders_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        _filled_orders_excel(orders),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _break_even_payload() -> dict:
    strategy = BreakEvenTakeProfitStrategy(db_path=_trading_db_path())
    round_ts, checks = strategy.get_latest_round_checks()
    records = strategy.recent_records(limit=100)
    return {
        "round_ts": round_ts,
        "checks": [asdict(row) for row in checks],
        "records": [asdict(row) for row in records],
    }


@app.get("/api/break-even/summary")
def break_even_summary_api():
    try:
        return jsonify(_break_even_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


def _partial_take_profit_payload() -> dict:
    strategy = PartialTakeProfitStrategy(db_path=_trading_db_path())
    round_ts, checks = strategy.get_latest_round_checks()
    records = strategy.recent_records(limit=100)
    errors = strategy.recent_errors(limit=100)
    return {
        "round_ts": round_ts,
        "checks": [asdict(row) for row in checks],
        "records": [asdict(row) for row in records],
        "errors": errors,
    }


@app.get("/api/partial-take-profit/summary")
def partial_take_profit_summary_api():
    try:
        return jsonify(_partial_take_profit_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


def _trailing_reduction_payload() -> dict:
    return TrailingReductionTracker(db_path=_trading_db_path()).summary_payload()


@app.get("/api/trailing-reduction/summary")
def trailing_reduction_summary_api():
    try:
        return jsonify(_trailing_reduction_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/trailing-reduction/refresh-pretrigger")
def trailing_reduction_refresh_pretrigger_api():
    try:
        return jsonify(TrailingReductionTracker(db_path=_trading_db_path()).refresh_pretriggered_symbols())
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


def _dynamic_profit_protection_payload() -> dict:
    return DynamicProfitProtection(db_path=_trading_db_path()).summary_payload()


@app.get("/api/dynamic-profit-protection/summary")
def dynamic_profit_protection_summary_api():
    try:
        return jsonify(_dynamic_profit_protection_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


def _hard_take_profit_payload() -> dict:
    return HardTakeProfit(db_path=_trading_db_path()).summary_payload()


@app.get("/api/hard-take-profit/summary")
def hard_take_profit_summary_api():
    try:
        return jsonify(_hard_take_profit_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


def _trailing_stop_payload() -> dict:
    return TrailingStopTracker(db_path=_trading_db_path()).summary_payload()


@app.get("/api/trailing-stop/summary")
def trailing_stop_summary_api():
    try:
        return jsonify(_trailing_stop_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/trailing-stop/refresh-pretrigger")
def trailing_stop_refresh_pretrigger_api():
    if not feature_flags.is_feature_enabled(feature_flags.TRAILING_STOP, CONFIG_DB_PATH):
        return jsonify({"error": "移动追踪止盈规则功能开关已关闭"}), 409
    try:
        return jsonify(TrailingStopTracker(db_path=_trading_db_path()).refresh_pretriggered_symbols())
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


def _holding_increase_payload() -> dict:
    holding_scoring = HoldingPositionScoringSystem(db_path=_trading_db_path())
    round_ts, checks = holding_scoring.get_latest_increase_checks()
    latest_pretrigger_rounds = holding_scoring.latest_pretrigger_increase_rounds()
    annotated_checks = []
    for row in checks:
        item = dict(row)
        item["latest_pretrigger_round_ts"] = latest_pretrigger_rounds.get(str(item.get("symbol", "")))
        annotated_checks.append(item)
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    records = holding_scoring.recent_increase_records(limit=100, since_ms=since_ms)
    return {
        "round_ts": round_ts,
        "checks": annotated_checks,
        "records": [dict(row) for row in records],
    }


def _live_holding_increase_payload() -> dict:
    """Return only the latest live-account increase module data."""
    holding_scoring = real_trading.holding_scoring()
    round_ts, checks = holding_scoring.get_latest_increase_checks()
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    records = holding_scoring.recent_increase_records(limit=100, since_ms=since_ms)
    snapshots = real_trading.experiment().latest_position_snapshots(limit=100)
    return {
        "round_ts": round_ts,
        "checks": _sync_live_module_checks(checks, snapshots),
        "records": [dict(row) for row in records],
    }


@app.get("/api/live/holding-increase/summary")
def live_holding_increase_summary_api():
    try:
        return jsonify(_live_holding_increase_payload())
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


LIVE_HIGH_FREQUENCY_MODULES = {
    "break-even": (0, "保本止盈", "recent_records"),
    "partial-take-profit": (1, "分批止盈", "recent_records"),
    "trailing-reduction": (2, "移动追踪减仓", "recent_action_records"),
    "dynamic-profit-protection": (3, "动态利润保护", "recent_action_records"),
    "hard-take-profit": (4, "硬止盈", "recent_action_records"),
    "trailing-stop": (5, "移动追踪止盈", "recent_action_records"),
}

LIVE_MODULE_TABLES = {
    "break-even": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("实验组USDT净值", "account_equity_usdt", "text"), ("R", "r_usdt", "text"), ("未变现盈亏", "unrealized_pnl", "text"), ("开仓价", "entry_price", "text"), ("持仓数量", "position_amt", "text"), ("触发保本", "triggered", "bool"), ("状态", "status", "text"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("方向", "side", "text"), ("持仓数量", "position_amt", "text"), ("开仓价", "entry_price", "text"), ("R", "r_usdt", "text"), ("未变现盈亏", "unrealized_pnl", "text"), ("原止损单", "old_stop_loss_order_id", "text"), ("新止损单", "new_stop_loss_order_id", "text"), ("止损价", "stop_loss_price", "text"), ("状态", "status", "text"), ("原因", "reason", "text")),
    },
    "partial-take-profit": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("实验组USDT净值", "account_equity_usdt", "text"), ("R", "r_usdt", "text"), ("触发R", "trigger_r_usdt", "text"), ("未变现盈亏", "unrealized_pnl", "text"), ("开仓价", "entry_price", "text"), ("持仓数量", "position_amt", "text"), ("触发分批止盈", "triggered", "bool"), ("状态", "status", "text"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("方向", "side", "text"), ("原持仓", "position_amt", "text"), ("止盈数量", "take_profit_quantity", "text"), ("开仓价", "entry_price", "text"), ("R", "r_usdt", "text"), ("触发R", "trigger_r_usdt", "text"), ("未变现盈亏", "unrealized_pnl", "text"), ("订单ID", "take_profit_order_id", "text"), ("触发档位", "trigger_label", "text"), ("状态", "status", "text"), ("原因", "reason", "text")),
    },
    "trailing-reduction": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("tag", "tag", "text"), ("R", "r_usdt", "text"), ("触发R", "trigger_r_usdt", "text"), ("未变现盈亏", "unrealized_pnl", "text"), ("当前价", "current_price", "text"), ("最近15m low", "latest_15m_low", "text"), ("次近15m low", "second_15m_low", "text"), ("最低价", "lowest_15m_low", "text"), ("ATR(14)", "atr14", "text"), ("最新1m high", "latest_1m_high", "text"), ("最新1m close", "latest_1m_close", "text"), ("开仓以来最高价", "highest_since_open", "text"), ("回撤", "price_drawdown", "text"), ("预触发", "pretriggered", "bool"), ("结构破位", "structure_break_triggered", "bool"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("最高价", "highest_since_open", "text"), ("ATR", "atr14", "text"), ("回撤", "price_drawdown", "text"), ("减仓比例", "reduction_percent", "text"), ("原数量", "original_quantity", "text"), ("减仓数量", "reduced_quantity", "text"), ("剩余数量", "remaining_quantity", "text"), ("市价单", "market_order_id", "text"), ("止盈单", "take_profit_order_id", "text"), ("止损单", "stop_loss_order_id", "text"), ("状态", "status", "text"), ("原因", "reason", "text")),
    },
    "dynamic-profit-protection": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("开仓价", "entry_price", "text"), ("持仓数量", "position_amt", "text"), ("浮盈", "unrealized_pnl", "text"), ("新开仓后已实现盈亏", "realized_pnl_since_open", "text"), ("周期累计盈亏", "cycle_total_pnl", "text"), ("R", "r_usdt", "text"), ("最新high", "latest_1m_high", "text"), ("最新close", "latest_1m_close", "text"), ("开仓以来最高价", "highest_since_open", "text"), ("周期累积盈亏历史最高", "highest_cycle_total_pnl", "text"), ("历史最高出现时间", "highest_profit_at", "time"), ("当前档位", "current_tier", "text"), ("回撤", "profit_drawdown_ratio", "text"), ("阈值", "drawdown_threshold", "text"), ("触发", "triggered", "bool"), ("满足前提", "eligible", "bool"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("持仓数量", "position_amt", "text"), ("开仓价", "entry_price", "text"), ("R", "r_usdt", "text"), ("周期盈亏倍数", "profit_r_multiple", "text"), ("新开仓后已实现盈亏", "realized_pnl_since_open", "text"), ("周期累计盈亏", "cycle_total_pnl", "text"), ("close", "latest_1m_close", "text"), ("最高价", "highest_since_open", "text"), ("周期累积盈亏历史最高", "highest_cycle_total_pnl", "text"), ("当前档位", "current_tier", "text"), ("回撤", "profit_drawdown_ratio", "text"), ("阈值", "drawdown_threshold", "text"), ("平仓数量", "close_quantity", "text"), ("订单ID", "close_order_id", "text"), ("状态", "close_status", "text"), ("原因", "reason", "text")),
    },
    "hard-take-profit": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("开仓价", "entry_price", "text"), ("持仓数量", "position_amt", "text"), ("未变现盈利", "unrealized_pnl", "text"), ("持仓名义价值", "position_notional", "text"), ("盈利率", "profit_ratio", "text"), ("阈值", "profit_threshold", "text"), ("触发", "triggered", "bool"), ("全部平仓单", "close_order_id", "text"), ("状态", "close_status", "text"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("持仓数量", "position_amt", "text"), ("开仓价", "entry_price", "text"), ("未变现盈利", "unrealized_pnl", "text"), ("持仓名义价值", "position_notional", "text"), ("盈利率", "profit_ratio", "text"), ("阈值", "profit_threshold", "text"), ("平仓数量", "close_quantity", "text"), ("订单ID", "close_order_id", "text"), ("状态", "close_status", "text"), ("原因", "reason", "text")),
    },
    "trailing-stop": {
        "check_columns": (("检查时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("开仓价", "entry_price", "text"), ("持仓数量", "position_amt", "text"), ("持仓小时", "holding_hours", "text"), ("1m high", "kline_high", "text"), ("1m close", "latest_1m_close", "text"), ("开仓以来最高价", "highest_since_open", "text"), ("价格回撤", "price_drawdown", "text"), ("回撤阈值", "drawdown_threshold", "text"), ("ATR(14)", "atr14", "text"), ("波动率", "volatility", "text"), ("tag", "tag", "text"), ("触发止盈", "trailing_stop_triggered", "bool"), ("满足前提", "eligible", "bool"), ("原因", "reason", "text")),
        "record_columns": (("操作时间", "checked_at", "time"), ("symbol", "symbol", "text"), ("持仓数量", "position_amt", "text"), ("开仓价", "entry_price", "text"), ("ATR(14)", "atr14", "text"), ("波动率", "volatility", "text"), ("当下浮盈", "unrealized_pnl_at_high", "text"), ("最大浮盈", "max_unrealized_pnl", "text"), ("价格回撤", "price_drawdown", "text"), ("回撤阈值", "drawdown_threshold", "text"), ("取消止盈单", "cancel_take_profit_order_id", "text"), ("取消状态", "cancel_status", "text"), ("平仓数量", "close_quantity", "text"), ("平仓单", "close_order_id", "text"), ("平仓状态", "close_status", "text"), ("原因", "reason", "text")),
    },
}


def _serialize_live_module_row(row) -> dict:
    return asdict(row) if hasattr(row, "__dataclass_fields__") else dict(row)


def _sync_live_module_checks(checks, position_snapshots) -> list[dict]:
    """Align a module's latest check list with the current live positions.

    A position can be opened after the module's most recent minute scan.  Keep
    it visible as pending until that module writes its first real check, while
    dropping checks for positions which are no longer open.
    """
    serialized_checks = [_serialize_live_module_row(row) for row in checks]
    snapshots = [_serialize_live_module_row(row) for row in position_snapshots]
    active = {}
    for row in snapshots:
        try:
            amount = Decimal(str(row.get("position_amt", row.get("positionAmt", "0")) or "0"))
        except (ArithmeticError, ValueError):
            continue
        if amount != 0:
            symbol = str(row.get("symbol", "")).strip().upper().removesuffix("USDT")
            if symbol:
                active[symbol] = row
    checks_by_symbol = {
        str(row.get("symbol", "")).strip().upper().removesuffix("USDT"): row
        for row in serialized_checks
    }
    synchronized = []
    for symbol, snapshot in sorted(active.items()):
        if symbol in checks_by_symbol:
            synchronized.append(checks_by_symbol[symbol])
            continue
        synchronized.append({
            **snapshot,
            "symbol": symbol,
            "decision_round_ts": snapshot.get("updated_at"),
            "checked_at": snapshot.get("updated_at"),
            "calculated_at": snapshot.get("updated_at"),
            "current_price": snapshot.get("mark_price", snapshot.get("markPrice", "")),
            "unrealized_pnl": snapshot.get("unrealized_pnl", snapshot.get("unRealizedProfit", "")),
            # Increase-check columns do not exist on a position snapshot.  Keep
            # the pending row template-compatible until the first scan writes
            # a real PositionIncreaseCheck instead of exposing Jinja Undefined
            # values (numeric filters such as ``float`` cannot consume them).
            "one_r_usdt": "",
            "latest_total_score": "",
            "previous_total_score": "",
            "latest_reduction_price": "",
            "open_trade_created_at": snapshot.get("opened_at"),
            # Stop-loss checks also pass through this synchronizer.  A newly
            # opened position has no candle/structural-stop values until the
            # next scan, so provide explicit nulls rather than letting Jinja
            # receive Undefined values (which fail when formatted as floats).
            "latest_15m_open_time": None,
            "latest_15m_close": None,
            "latest_structural_stop_loss": None,
            "prev_15m_open_time": None,
            "prev_15m_close": None,
            "prev_structural_stop_loss": None,
            # Reduction checks use several values with Jinja's ``float``
            # filter.  Pending positions do not have a reduction-check row
            # yet, so give those numeric columns neutral values until the
            # first scan completes.
            "open_entry_price": "",
            "ema16": "",
            "ema21": "",
            "score_drawdown": "",
            "atr14": "",
            "two_r_usdt": "",
            "open_total_score": "",
            "latest_15m_open": "",
            "second_15m_open": "",
            "second_15m_close": "",
            "third_15m_open": "",
            "third_15m_close": "",
            "latest_macd": "",
            "second_macd": "",
            "third_macd": "",
            "rule_name": "",
            "tag": "新开仓待扫描",
            "status": "待扫描",
            "triggered": False,
            "reason": "该symbol在模块最近一轮扫描后新开仓，等待下一轮扫描",
        })
    return synchronized


def _sync_live_portfolio_risk(summary, position_snapshots):
    """Keep the portfolio-risk symbol list aligned with live positions."""
    snapshots = [_serialize_live_module_row(row) for row in position_snapshots]
    current_rows = [] if summary is None else [
        _serialize_live_module_row(row) for row in summary.positions
    ]
    synchronized = _sync_live_module_checks(current_rows, snapshots)
    if summary is None:
        summary_data = {
            "decision_round_ts": 0,
            "total_risk": "0",
            "account_equity_usdt": "0",
            "calculated_at": 0,
        }
    else:
        summary_data = _serialize_live_module_row(summary)
    summary_data["positions"] = synchronized
    summary_data["position_count"] = len(synchronized)
    summary_data["pending_count"] = sum(row.get("status") == "待扫描" for row in synchronized)
    return summary_data


@app.get("/api/live/high-frequency/<module_key>/summary")
def live_high_frequency_summary_api(module_key: str):
    config = LIVE_HIGH_FREQUENCY_MODULES.get(module_key)
    if config is None:
        return jsonify({"error": "unknown live module"}), 404
    module_index, label, records_method = config
    try:
        module = real_trading.high_frequency_modules()[module_index]
        round_ts, checks = module.get_latest_round_checks()
        records = getattr(module, records_method)(limit=100)
        snapshots = real_trading.experiment().latest_position_snapshots(limit=100)
        return jsonify({
            "key": module_key,
            "label": label,
            "round_ts": round_ts,
            "checks": _sync_live_module_checks(checks, snapshots),
            "records": [_serialize_live_module_row(row) for row in records],
            "tables": LIVE_MODULE_TABLES[module_key],
        })
    except sqlite3.Error as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/holding-increase/refresh-pretrigger")
def holding_increase_refresh_pretrigger_api():
    try:
        holding_scoring = HoldingPositionScoringSystem(db_path=_trading_db_path())
        result = holding_scoring.refresh_pretrigger_increase_checks()
        payload = _holding_increase_payload()
        payload["action_records"] = payload["records"]
        payload.update(result)
        payload["created_records"] = result.get("records", 0)
        payload["refresh_result"] = result
        return jsonify(payload)
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Binance holding increase refresh failed: {exc}"}), 502
    except (RuntimeError, sqlite3.Error) as exc:
        return jsonify({"error": str(exc)}), 502


def _btc_5m_payload(page: int = 1) -> dict:
    page = max(1, page)
    page_size = 24
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
    # BTC candles are collector-owned base data.  They were previously read via
    # the trading DB route, which still pointed at the pre-split database.
    with db_config.connect_sqlite(_base_db_path()) as conn:
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


@app.get("/api/feature-flags")
def feature_flags_api():
    initialize_config_database(CONFIG_DB_PATH, BASE_DB_PATH)
    flags = feature_flags.list_feature_flags(CONFIG_DB_PATH)
    return jsonify({"flags": feature_flags.flags_to_dict(flags)})


@app.post("/api/feature-flags/<key>")
def update_feature_flag_api(key: str):
    payload = request.get_json(silent=True) or {}
    if "enabled" not in payload:
        return jsonify({"error": "enabled is required"}), 400
    try:
        flag = feature_flags.set_feature_flag(
            key=key,
            enabled=bool(payload["enabled"]),
            db_path=CONFIG_DB_PATH,
        )
    except KeyError:
        return jsonify({"error": f"Unknown feature flag: {key}"}), 404
    return jsonify({"flag": feature_flags.flags_to_dict([flag])[0]})


@app.get("/api/position-limit-settings")
def position_limit_settings_api():
    return jsonify(get_position_limit_settings(CONFIG_DB_PATH))


@app.put("/api/position-limit-settings")
def update_position_limit_settings_api():
    try:
        settings = set_position_limit_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(settings)


@app.get("/api/dynamic-profit-protection-settings")
def dynamic_profit_protection_settings_api():
    return jsonify(get_dynamic_profit_protection_settings(CONFIG_DB_PATH))


@app.put("/api/dynamic-profit-protection-settings")
def update_dynamic_profit_protection_settings_api():
    try:
        return jsonify(set_dynamic_profit_protection_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        ))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/hard-take-profit-settings")
def hard_take_profit_settings_api():
    return jsonify(get_hard_take_profit_settings(CONFIG_DB_PATH))


@app.put("/api/hard-take-profit-settings")
def update_hard_take_profit_settings_api():
    try:
        return jsonify(set_hard_take_profit_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        ))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/dynamic-open-threshold-settings")
def dynamic_open_threshold_settings_api():
    return jsonify(get_dynamic_open_threshold_settings(CONFIG_DB_PATH))


@app.put("/api/dynamic-open-threshold-settings")
def update_dynamic_open_threshold_settings_api():
    try:
        settings = set_dynamic_open_threshold_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(settings)


@app.get("/api/market-filter-settings")
def market_filter_settings_api():
    return jsonify(get_market_filter_settings(CONFIG_DB_PATH))


@app.put("/api/market-filter-settings")
def update_market_filter_settings_api():
    try:
        return jsonify(set_market_filter_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        ))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/scoring-rule-weights")
def scoring_rule_weights_api():
    return jsonify({"rules": get_rule_score_weight_settings(CONFIG_DB_PATH)})


@app.put("/api/scoring-rule-weights")
def update_scoring_rule_weights_api():
    payload = request.get_json(silent=True) or {}
    raw_rules = payload.get("rules")
    if not isinstance(raw_rules, list):
        return jsonify({"error": "rules must be an array containing all 18 rules"}), 400
    try:
        weights = {
            int(item["rule_id"]): item["weight"]
            for item in raw_rules
            if isinstance(item, dict)
        }
        if len(weights) != len(raw_rules):
            raise ValueError("Each rule must have a unique rule_id and weight")
        rules = set_rule_score_weight_settings(weights, CONFIG_DB_PATH)
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"rules": rules})


@app.get("/api/scoring-rule-election")
def scoring_rule_election_api():
    return jsonify(get_rule_election_settings(CONFIG_DB_PATH))


@app.put("/api/scoring-rule-election")
def update_scoring_rule_election_api():
    try:
        return jsonify(set_rule_election_settings(request.get_json(silent=True) or {}, CONFIG_DB_PATH))
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/openable-symbol-settings")
def openable_symbol_settings_api():
    return jsonify(get_openable_symbol_settings(CONFIG_DB_PATH))


@app.put("/api/openable-symbol-settings")
def update_openable_symbol_settings_api():
    try:
        return jsonify(set_openable_symbol_settings(request.get_json(silent=True) or {}, CONFIG_DB_PATH))
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/weak-market-profit-settings")
def weak_market_profit_settings_api():
    return jsonify(get_weak_market_profit_settings(CONFIG_DB_PATH))


@app.put("/api/weak-market-profit-settings")
def update_weak_market_profit_settings_api():
    try:
        return jsonify(set_weak_market_profit_settings(
            request.get_json(silent=True) or {}, CONFIG_DB_PATH
        ))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/reduction-module-settings")
def reduction_module_settings_api():
    return jsonify(get_reduction_module_settings(CONFIG_DB_PATH))


@app.put("/api/reduction-module-settings")
def update_reduction_module_settings_api():
    try:
        return jsonify(
            set_reduction_module_settings(
                request.get_json(silent=True) or {}, CONFIG_DB_PATH
            )
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.post("/api/trading-experiment/run")
def trading_experiment_run_api():
    try:
        zombie_result = ZombieForceLiquidationModule(db_path=_trading_db_path()).run_round()
        if not feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM, CONFIG_DB_PATH):
            return jsonify(
                {
                    "opened": 0,
                    "skipped": 0,
                    "reason": "模拟盘交易系统功能开关已关闭，模拟盘不再开新仓；已有模拟盘仓位风控保护继续运行。",
                    "zombie_force_liquidation": zombie_result,
                }
            )
        result = TradingExperiment(db_path=_trading_db_path()).run_latest_round()
        result["zombie_force_liquidation"] = zombie_result
        return jsonify(result)
    except BinanceAccountConfigError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.exceptions.RequestException as exc:
        return jsonify({"error": f"Trading experiment request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502


@app.get("/safety/abnormal-wicks")
def abnormal_wicks():
    initialize_config_database(CONFIG_DB_PATH, BASE_DB_PATH)
    limit = request.args.get("limit", default=100, type=int)
    symbol = request.args.get("symbol", default="", type=str).strip()
    btc_page = request.args.get("btc_page", default=1, type=int)
    limit = max(1, min(limit, 1000))
    btc_page = max(1, btc_page)
    btc_page_size = 24
    module_errors = []

    def load_module(label: str, loader, default):
        value, error = _safe_page_module(label, loader, default)
        if error:
            module_errors.append(error)
        return value

    module = PreSafetyModule(db_path=_scoring_db_path())
    load_module("异常插针表初始化", module.init_table, None)
    abnormal_events_since_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
    cooldown = CooldownModule(db_path=_scoring_db_path())
    load_module("冷却表初始化", cooldown.init_table, None)
    should_load_abnormal_events = request.args.get("wick_refresh") == "1"
    if should_load_abnormal_events:
        events = load_module(
            "异常插针事件",
            lambda: module.get_recent_events_by_symbol(symbol=symbol, limit=limit, since_ms=abnormal_events_since_ms)
            if symbol
            else module.get_recent_events(limit=limit, since_ms=abnormal_events_since_ms),
            [],
        )
    else:
        events = []
    symbols = load_module("异常插针 Symbol 列表", lambda: module.get_event_symbols(since_ms=abnormal_events_since_ms), [])
    current_round_ts = load_module("当前决策轮次", module._decision_round_ts_ms, 0)
    latest_round_ts, latest_round_symbols = load_module("最新异常插针轮次", lambda: module.get_latest_round_abnormal_symbols(decision_round_ts=current_round_ts), (0, []))
    cooldown_round_ts, cooldown_symbols = load_module("冷却 Symbol 轮次", lambda: cooldown.get_latest_round_symbols(decision_round_ts=current_round_ts), (0, []))
    scoring = ScoringSystem(db_path=_scoring_db_path())
    load_module("评分表初始化", scoring.init_table, None)
    score_round_ts, round_scores = load_module("评分规则1", scoring.get_latest_round_scores, (0, []))
    score_rule2_round_ts, round_scores_rule2 = load_module("评分规则2 rule2", scoring.get_latest_round_scores_close_gt_ma20, (0, []))
    score_rule3_round_ts, round_scores_rule3 = load_module("评分规则3 rule3", scoring.get_latest_round_scores_1h_close_gt_prev, (0, []))
    score_rule4_round_ts, round_scores_rule4 = load_module("评分规则4 rule4", scoring.get_latest_round_scores_15m_bullish_3of4, (0, []))
    score_rule5_round_ts, round_scores_rule5 = load_module("评分规则5 rule5", scoring.get_latest_round_scores_15m_close_increasing_3of4, (0, []))
    score_rule6_round_ts, round_scores_rule6 = load_module("评分规则6 rule6", scoring.get_latest_round_scores_1m_close_gt_5m_ma20, (0, []))
    score_rule7_round_ts, round_scores_rule7 = load_module("评分规则7 rule7", scoring.get_latest_round_scores_15m_close_near_high_2of4, (0, []))
    score_rule8_round_ts, round_scores_rule8 = load_module("评分规则8 rule8", scoring.get_latest_round_scores_15m_latest_highest_prev_96, (0, []))
    score_rule9_round_ts, round_scores_rule9 = load_module("评分规则9 rule9", scoring.get_latest_round_scores_15m_close_desc_3_with_oi_45m, (0, []))
    score_rule10_round_ts, round_scores_rule10 = load_module("评分规则10 rule10", scoring.get_latest_round_scores_1m_close_gt_60m_open_with_oi_60m, (0, []))
    score_rule11_round_ts, round_scores_rule11 = load_module("评分规则11 rule11", scoring.get_latest_round_scores_oi_loss_rate_240m, (0, []))
    score_rule12_round_ts, round_scores_rule12 = load_module("评分规则12 rule12", scoring.get_latest_round_scores_15m_funding_rate_4bars, (0, []))
    score_rule13_round_ts, round_scores_rule13 = load_module("评分规则13 rule13", scoring.get_latest_round_scores_15m_bullish_volume_breakout, (0, []))
    score_rule14_round_ts, round_scores_rule14 = load_module("评分规则14 rule14", scoring.get_latest_round_scores_15m_volume_spike_2of3, (0, []))
    score_rule15_round_ts, round_scores_rule15 = load_module("评分规则15 rule15", scoring.get_latest_round_scores_1h_volume_spike_latest, (0, []))
    score_rule16_round_ts, round_scores_rule16 = load_module("评分规则16 rule16", scoring.get_latest_round_scores_15m_pullback_low_volume, (0, []))
    score_rule17_round_ts, round_scores_rule17 = load_module("评分规则17 rule17", scoring.get_latest_round_scores_15m_low_rebound_3bars, (0, []))
    score_rule18_round_ts, round_scores_rule18 = load_module("评分规则18 rule18", scoring.get_latest_round_scores_structural_stop_loss_distance, (0, []))
    score_total_round_ts, round_scores_total = load_module("评分总分", scoring.get_latest_round_total_scores, (0, []))
    score_total_updated_at = load_module("评分更新时间", lambda: scoring.get_total_score_round_updated_at(score_total_round_ts), 0)
    scoring_ma20_skip_record = load_module("MA20 评分跳过记录", lambda: scoring.get_ma20_skip_record_for_round(score_total_round_ts), None)
    scoring_symbol_error_round_ts = score_total_round_ts
    # Keep this lookup pinned to the current score round:
    # scoring_symbol_errors = scoring.get_symbol_errors_for_round(score_total_round_ts)
    scoring_symbol_errors = load_module("评分 Symbol 错误", lambda: scoring.get_symbol_errors_for_round(score_total_round_ts), [])
    score_band_configs, score_distance_threshold_text, score_leverage_mapping_text, openable_min_total_score = _score_band_context()
    openable = OpenableSymbolModule(db_path=_scoring_db_path())
    load_module("可开仓表初始化", openable.init_table, None)
    openable_round_ts = score_total_round_ts
    _, openable_symbols = load_module(
        "可开仓模块",
        lambda: openable.get_latest_round_symbols(decision_round_ts=openable_round_ts)
        if openable_round_ts
        else (None, []),
        (None, []),
    )
    openable_round_history = load_module(
        "可开仓 Symbol 情况记录",
        lambda: openable.recent_round_summaries(limit=100),
        [],
    )
    market_filter = MarketFilterModule(db_path=_market_db_path())
    market_filter_results = load_module("市场行情过滤", lambda: market_filter.recent_results(limit=100, days=7), [])
    weak_market_profit_adjustment = WeakMarketProfitAdjustmentModule(db_path=_market_db_path())
    weak_market_profit_adjustment_results = load_module("弱势市场止盈动态调整", lambda: weak_market_profit_adjustment.recent_results(limit=100, days=7), [])
    add_position_permission = AddPositionPermissionModule(db_path=_market_db_path())
    add_position_permission_results = load_module("加仓权限", lambda: add_position_permission.recent_results(limit=100, days=7), [])
    dynamic_add_position_threshold = DynamicAddPositionThresholdModule(db_path=_trading_db_path())
    dynamic_add_position_threshold_results = load_module("动态加仓阈值", lambda: dynamic_add_position_threshold.recent_results(limit=100, days=7), [])
    dynamic_open_threshold = DynamicOpenThresholdModule(db_path=_scoring_db_path())
    dynamic_open_threshold_results = load_module("动态开仓门槛", lambda: dynamic_open_threshold.recent_results(limit=100, days=7), [])
    dynamic_open_threshold_errors = load_module(
        "动态开仓门槛错误",
        lambda: DynamicOpenThresholdModule.recent_errors(
            error_db_path=_market_db_path(), limit=20, days=7
        ),
        [],
    )
    open_block_notice = _current_open_block_notice(
        openable_round_ts, market_filter_results, dynamic_open_threshold_results
    )
    score_trend_symbols = load_module("评分趋势 Symbol 列表", scoring.get_total_score_symbols, [])
    requested_score_trend_symbol = request.args.get("score_trend_symbol", default="", type=str).strip()
    default_score_trend_symbol = round_scores_total[0].symbol if round_scores_total else ""
    score_trend_symbol = requested_score_trend_symbol or default_score_trend_symbol
    if score_trend_symbol and score_trend_symbol not in score_trend_symbols:
        score_trend_symbols = sorted(set(score_trend_symbols) | {score_trend_symbol})
    score_trend_rows = []
    trading_experiment = TradingExperiment(db_path=_trading_db_path())
    trading_records_since_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    trading_trade_records = load_module("交易实验记录", lambda: trading_experiment.recent_trade_records(limit=100, since_ms=trading_records_since_ms), [])
    trading_new_open_symbols = sorted({
        row.symbol
        for row in trading_trade_records
        if row.status == "opened" and row.decision_round_ts == openable_round_ts
    })
    trading_position_snapshots = load_module("交易持仓快照", lambda: trading_experiment.latest_position_snapshots(limit=100), [])
    trading_used_margin_usdt = _trading_used_margin_text(trading_position_snapshots)
    trading_equity_trend_rows = load_module("交易权益曲线", lambda: _experiment_equity_trend_rows(trading_records_since_ms), [])
    trading_equity = _latest_trading_equity_usdt(trading_equity_trend_rows)
    trading_open_increase_blocked = _trading_open_increase_blocked(trading_equity, trading_position_snapshots)
    trading_error_records = load_module("交易错误记录", lambda: trading_experiment.recent_error_records(limit=100, since_ms=trading_records_since_ms), [])
    zombie_force_liquidation = ZombieForceLiquidationModule(db_path=_trading_db_path())
    zombie_force_liquidation_records = load_module("僵尸强平记录", lambda: zombie_force_liquidation.recent_records(limit=100, since_ms=trading_records_since_ms), [])
    live_experiment = real_trading.experiment()
    load_module("实盘交易表初始化", real_trading.initialize, None)
    live_trade_records = load_module("实盘交易实验记录", lambda: live_experiment.recent_trade_records(limit=100, since_ms=trading_records_since_ms), [])
    live_new_open_symbols = sorted({
        row.symbol
        for row in live_trade_records
        if row.status == "opened" and row.decision_round_ts == openable_round_ts
    })
    live_position_snapshots = load_module("实盘交易持仓快照", lambda: live_experiment.latest_position_snapshots(limit=100), [])
    live_error_records = load_module("实盘交易错误记录", lambda: live_experiment.recent_error_records(limit=100, since_ms=trading_records_since_ms), [])
    live_zombie_records = load_module("实盘僵尸强平记录", lambda: real_trading.zombie_module().recent_records(limit=100, since_ms=trading_records_since_ms), [])
    live_equity_trend_rows = load_module("实盘交易权益曲线", lambda: _experiment_equity_trend_rows(trading_records_since_ms, db_config.REAL_TRADING_CORE_DB_PATH), [])
    live_trading_equity = _latest_trading_equity_usdt(live_equity_trend_rows) if live_equity_trend_rows else real_trading.config().initial_equity_usdt
    live_used_margin_usdt = _trading_used_margin_text(live_position_snapshots)
    live_open_increase_blocked = _trading_open_increase_blocked(live_trading_equity, live_position_snapshots)
    live_holding_scoring = real_trading.holding_scoring()
    live_holding_stop_loss_round_ts, live_holding_stop_loss_checks = load_module("实盘持仓结构止损检查", live_holding_scoring.get_latest_round_checks, (0, []))
    live_holding_portfolio_risk = load_module("实盘持仓组合风险", live_holding_scoring.get_latest_portfolio_risk, None)
    live_holding_reduction_round_ts, live_holding_reduction_checks = load_module("实盘持仓减仓检查", live_holding_scoring.get_latest_reduction_checks, (0, []))
    live_holding_increase_round_ts, live_holding_increase_checks = load_module("实盘持仓加仓检查", live_holding_scoring.get_latest_increase_checks, (0, []))
    live_holding_increase_pretrigger_rounds = load_module(
        "实盘持仓加仓预触发", live_holding_scoring.latest_pretrigger_increase_rounds, {}
    )
    live_holding_stop_loss_records = load_module("实盘持仓结构止损记录", lambda: live_holding_scoring.recent_stop_loss_records(limit=100), [])
    live_holding_reduction_records = load_module("实盘持仓减仓记录", lambda: live_holding_scoring.recent_reduction_records(limit=100), [])
    live_holding_reduction_stop_failure_liquidations = load_module(
        "实盘重挂止损失败后强平记录",
        lambda: live_holding_scoring.recent_reduction_stop_failure_liquidations(
            limit=100, since_ms=trading_records_since_ms
        ),
        [],
    )
    live_holding_increase_records = load_module("实盘持仓加仓记录", lambda: live_holding_scoring.recent_increase_records(limit=100, since_ms=trading_records_since_ms), [])
    live_holding_stop_loss_checks = _sync_live_module_checks(live_holding_stop_loss_checks, live_position_snapshots)
    live_holding_reduction_checks = _sync_live_module_checks(live_holding_reduction_checks, live_position_snapshots)
    live_holding_increase_checks = _sync_live_module_checks(live_holding_increase_checks, live_position_snapshots)
    live_holding_portfolio_risk = _sync_live_portfolio_risk(live_holding_portfolio_risk, live_position_snapshots)
    live_break_even, live_partial, live_trailing_reduction, live_dynamic, live_hard, live_trailing_stop = real_trading.high_frequency_modules()
    live_high_frequency_modules = []
    for key, label, module, records_loader in (
        ("break-even", "保本止盈", live_break_even, lambda m: m.recent_records(limit=100)),
        ("partial-take-profit", "分批止盈", live_partial, lambda m: m.recent_records(limit=100)),
        ("trailing-reduction", "移动追踪减仓", live_trailing_reduction, lambda m: m.recent_action_records(limit=100)),
        ("dynamic-profit-protection", "动态利润保护", live_dynamic, lambda m: m.recent_action_records(limit=100)),
        ("hard-take-profit", "硬止盈", live_hard, lambda m: m.recent_action_records(limit=100)),
        ("trailing-stop", "移动追踪止盈", live_trailing_stop, lambda m: m.recent_action_records(limit=100)),
    ):
        round_ts, checks = load_module(f"实盘{label}检查", module.get_latest_round_checks, (0, []))
        records = load_module(f"实盘{label}记录", lambda m=module, loader=records_loader: loader(m), [])
        live_high_frequency_modules.append({
            "key": f"live-high-frequency-{key}", "api_key": key,
            "label": label, "round_ts": round_ts,
            "checks": _sync_live_module_checks(checks, live_position_snapshots),
            "records": [asdict(row) if hasattr(row, "__dataclass_fields__") else dict(row) for row in records],
            "tables": LIVE_MODULE_TABLES[key],
        })
    holding_scoring = HoldingPositionScoringSystem(db_path=_trading_db_path())
    holding_stop_loss_round_ts, holding_stop_loss_checks = load_module("持仓结构止损检查", holding_scoring.get_latest_round_checks, (0, []))
    holding_portfolio_risk = load_module("持仓组合风险", holding_scoring.get_latest_portfolio_risk, None)
    holding_reduction_round_ts, holding_reduction_checks = load_module("持仓减仓检查", holding_scoring.get_latest_reduction_checks, (0, []))
    holding_increase_round_ts, holding_increase_checks = load_module("持仓加仓检查", holding_scoring.get_latest_increase_checks, (0, []))
    holding_increase_pretrigger_rounds = load_module("持仓加仓预触发", holding_scoring.latest_pretrigger_increase_rounds, {})
    holding_stop_loss_records = load_module("持仓结构止损记录", lambda: holding_scoring.recent_stop_loss_records(limit=100), [])
    holding_reduction_records = load_module("持仓减仓记录", lambda: holding_scoring.recent_reduction_records(limit=100), [])
    holding_reduction_stop_failure_liquidations = load_module(
        "重挂止损失败后强平记录",
        lambda: holding_scoring.recent_reduction_stop_failure_liquidations(limit=100, since_ms=trading_records_since_ms),
        [],
    )
    holding_increase_records = load_module("持仓加仓记录", lambda: holding_scoring.recent_increase_records(limit=100, since_ms=trading_records_since_ms), [])
    break_even_payload = load_module("保本止盈", _break_even_payload, {"round_ts": 0, "checks": [], "records": []})
    break_even_round_ts = break_even_payload["round_ts"]
    break_even_checks = break_even_payload["checks"]
    break_even_records = break_even_payload["records"]
    partial_take_profit_strategy = PartialTakeProfitStrategy(db_path=_trading_db_path())
    partial_take_profit_round_ts, partial_take_profit_checks = load_module("分批止盈检查", partial_take_profit_strategy.get_latest_round_checks, (0, []))
    partial_take_profit_records = load_module("分批止盈记录", lambda: partial_take_profit_strategy.recent_records(limit=100), [])
    partial_take_profit_errors = load_module("分批止盈错误记录", lambda: partial_take_profit_strategy.recent_errors(limit=100), [])
    trailing_reduction_payload = load_module("移动追踪减仓", _trailing_reduction_payload, {"round_ts": 0, "checks": [], "records": []})
    trailing_reduction_round_ts = trailing_reduction_payload["round_ts"]
    trailing_reduction_checks = trailing_reduction_payload["checks"]
    trailing_reduction_records = trailing_reduction_payload["records"]
    dynamic_profit_protection_payload = load_module("动态利润保护", _dynamic_profit_protection_payload, {"round_ts": 0, "checks": [], "records": []})
    dynamic_profit_protection_round_ts = dynamic_profit_protection_payload["round_ts"]
    dynamic_profit_protection_checks = dynamic_profit_protection_payload["checks"]
    dynamic_profit_protection_records = dynamic_profit_protection_payload["records"]
    hard_take_profit_payload = load_module("硬止盈", _hard_take_profit_payload, {"round_ts": 0, "checks": [], "records": []})
    hard_take_profit_round_ts = hard_take_profit_payload["round_ts"]
    hard_take_profit_checks = hard_take_profit_payload["checks"]
    hard_take_profit_records = hard_take_profit_payload["records"]
    trailing_stop_tracker = TrailingStopTracker(db_path=_trading_db_path())
    trailing_stop_round_ts, trailing_stop_checks = load_module("移动追踪止盈检查", trailing_stop_tracker.get_latest_round_checks, (0, []))
    trailing_stop_records = load_module("移动追踪止盈记录", lambda: trailing_stop_tracker.recent_action_records(limit=100), [])

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
        score_total_updated_at=score_total_updated_at,
        round_scores_total=round_scores_total,
        scoring_ma20_skip_record=scoring_ma20_skip_record,
        scoring_symbol_error_round_ts=scoring_symbol_error_round_ts,
        scoring_symbol_errors=scoring_symbol_errors,
        openable_round_ts=openable_round_ts,
        openable_symbols=openable_symbols,
        openable_round_history=openable_round_history,
        score_band_configs=score_band_configs,
        score_distance_threshold_text=score_distance_threshold_text,
        score_leverage_mapping_text=score_leverage_mapping_text,
        openable_min_total_score=openable_min_total_score,
        market_filter_results=market_filter_results,
        weak_market_profit_adjustment_results=weak_market_profit_adjustment_results,
        add_position_permission_results=add_position_permission_results,
        dynamic_add_position_threshold_results=dynamic_add_position_threshold_results,
        dynamic_open_threshold_results=dynamic_open_threshold_results,
        dynamic_open_threshold_errors=dynamic_open_threshold_errors,
        open_block_notice=open_block_notice,
        trading_trade_records=trading_trade_records,
        trading_new_open_symbols=trading_new_open_symbols,
        trading_position_snapshots=trading_position_snapshots,
        trading_used_margin_usdt=trading_used_margin_usdt,
        trading_open_increase_blocked=trading_open_increase_blocked,
        trading_equity_usdt=trading_equity,
        trading_error_records=trading_error_records,
        trading_equity_trend_rows=trading_equity_trend_rows,
        zombie_force_liquidation_records=zombie_force_liquidation_records,
        live_trade_records=live_trade_records,
        live_new_open_symbols=live_new_open_symbols,
        live_position_snapshots=live_position_snapshots,
        live_used_margin_usdt=live_used_margin_usdt,
        live_open_increase_blocked=live_open_increase_blocked,
        live_error_records=live_error_records,
        live_zombie_records=live_zombie_records,
        live_equity_trend_rows=live_equity_trend_rows,
        live_trading_equity=live_trading_equity,
        live_holding_stop_loss_round_ts=live_holding_stop_loss_round_ts,
        live_holding_stop_loss_checks=live_holding_stop_loss_checks,
        live_holding_portfolio_risk=live_holding_portfolio_risk,
        live_holding_reduction_round_ts=live_holding_reduction_round_ts,
        live_holding_reduction_checks=live_holding_reduction_checks,
        live_holding_increase_round_ts=live_holding_increase_round_ts,
        live_holding_increase_checks=live_holding_increase_checks,
        live_holding_increase_pretrigger_rounds=live_holding_increase_pretrigger_rounds,
        live_holding_stop_loss_records=live_holding_stop_loss_records,
        live_holding_reduction_records=live_holding_reduction_records,
        live_holding_reduction_stop_failure_liquidations=live_holding_reduction_stop_failure_liquidations,
        live_holding_increase_records=live_holding_increase_records,
        live_high_frequency_modules=live_high_frequency_modules,
        holding_stop_loss_round_ts=holding_stop_loss_round_ts,
        holding_stop_loss_checks=holding_stop_loss_checks,
        holding_portfolio_risk=holding_portfolio_risk,
        holding_reduction_round_ts=holding_reduction_round_ts,
        holding_reduction_checks=holding_reduction_checks,
        holding_increase_round_ts=holding_increase_round_ts,
        holding_increase_checks=holding_increase_checks,
        holding_increase_pretrigger_rounds=holding_increase_pretrigger_rounds,
        holding_stop_loss_records=holding_stop_loss_records,
        holding_reduction_records=holding_reduction_records,
        holding_reduction_stop_failure_liquidations=holding_reduction_stop_failure_liquidations,
        holding_increase_records=holding_increase_records,
        break_even_round_ts=break_even_round_ts,
        break_even_checks=break_even_checks,
        break_even_records=break_even_records,
        partial_take_profit_round_ts=partial_take_profit_round_ts,
        partial_take_profit_checks=partial_take_profit_checks,
        partial_take_profit_records=partial_take_profit_records,
        partial_take_profit_errors=partial_take_profit_errors,
        trailing_reduction_round_ts=trailing_reduction_round_ts,
        trailing_reduction_checks=trailing_reduction_checks,
        trailing_reduction_records=trailing_reduction_records,
        dynamic_profit_protection_round_ts=dynamic_profit_protection_round_ts,
        dynamic_profit_protection_checks=dynamic_profit_protection_checks,
        dynamic_profit_protection_records=dynamic_profit_protection_records,
        hard_take_profit_round_ts=hard_take_profit_round_ts,
        hard_take_profit_checks=hard_take_profit_checks,
        hard_take_profit_records=hard_take_profit_records,
        trailing_stop_round_ts=trailing_stop_round_ts,
        trailing_stop_checks=trailing_stop_checks,
        trailing_stop_records=trailing_stop_records,
        rule_score_weights=scoring.rule_score_weights,
        score_trend_symbols=score_trend_symbols,
        score_trend_symbol=score_trend_symbol,
        score_trend_rows=score_trend_rows,
        active_tab=active_tab,
        module_errors=module_errors,
        selected_symbol=symbol,
        btc_5m_rows=btc_5m_rows,
        btc_chart_rows=btc_chart_rows,
        btc_page=btc_page,
        btc_page_size=btc_page_size,
        btc_total_rows=btc_total_rows,
        should_load_abnormal_events=should_load_abnormal_events,
        btc_total_pages=btc_total_pages,
        feature_flags=feature_flags.list_feature_flags(CONFIG_DB_PATH),
        position_limit_settings=get_position_limit_settings(CONFIG_DB_PATH),
        dynamic_profit_protection_settings=get_dynamic_profit_protection_settings(CONFIG_DB_PATH),
        hard_take_profit_settings=get_hard_take_profit_settings(CONFIG_DB_PATH),
        market_filter_settings=get_market_filter_settings(CONFIG_DB_PATH),
        dynamic_open_threshold_settings=get_dynamic_open_threshold_settings(CONFIG_DB_PATH),
        scoring_rule_weight_settings=get_rule_score_weight_settings(CONFIG_DB_PATH),
        scoring_rule_election_settings=get_rule_election_settings(CONFIG_DB_PATH),
        openable_symbol_settings=get_openable_symbol_settings(CONFIG_DB_PATH),
        weak_market_profit_settings=get_weak_market_profit_settings(CONFIG_DB_PATH),
        reduction_module_settings=get_reduction_module_settings(CONFIG_DB_PATH),
    )


@app.template_filter("fmt_ms_datetime")
def fmt_ms_datetime(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000)
