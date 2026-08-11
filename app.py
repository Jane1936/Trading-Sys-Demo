"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
- pre-safety task: detect abnormal wick events and cooldown symbols every 15m decision round
"""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, List

import collector
import db_config
import feature_flags
from data_processor import (
    MA20Processor,
    MA20Scheduler,
    MACalcResult,
    init_ema_table,
    init_macd_table,
    init_ma20_table,
    run_loop,
    save_indicator_results,
)
from break_even_take_profit import BreakEvenTakeProfitStrategy
from cooldown_module import CooldownModule
from openable_symbol_module import OpenableSymbol, OpenableSymbolModule
from pre_safety_module import PreSafetyModule
from partial_take_profit import PartialTakeProfitStrategy
from dynamic_profit_protection import DynamicProfitProtection
from trailing_stop_tracker import TrailingStopTracker
from trailing_reduction_tracker import TrailingReductionTracker
from holding_position_scoring import HoldingPositionScoringSystem
from scoring_system import ScoringSystem
from trading_experiment import TradingExperiment
from market_filter_module import MarketFilterModule
from weak_market_profit_adjustment import WeakMarketProfitAdjustmentModule
from add_position_permission_module import AddPositionPermissionModule
from dynamic_open_threshold import DynamicOpenThresholdModule
from dynamic_add_position_threshold import DynamicAddPositionThresholdModule
from zombie_force_liquidation import ZombieForceLiquidationModule
from sqlite_recovery import (
    is_malformed_database_error,
    quarantine_sqlite_database,
    quick_check_sqlite_database,
)

_universe_lock = threading.Lock()
_universe_refresh_interval_sec = 12 * 60 * 60
_universe_last_refresh_ts = 0.0
DATABASE_HEALTH_CHECK_INTERVAL_SEC = 5 * 60
PROFIT_MARKET_CONVERGENCE_TIMEOUT_SEC = float(
    os.getenv("PROFIT_MARKET_CONVERGENCE_TIMEOUT_SEC", "10")
)
PROFIT_MARKET_CONVERGENCE_POLL_SEC = float(
    os.getenv("PROFIT_MARKET_CONVERGENCE_POLL_SEC", "2")
)


def wait_for_profit_market_convergence(
    adjustment: WeakMarketProfitAdjustmentModule,
    decision_round_ts: int,
    *,
    timeout_sec: float | None = None,
    poll_sec: float | None = None,
) -> tuple[bool, str]:
    """Wait briefly for market inputs without starving position protection.

    A missing ALLUSDT candle previously left the minute-level profit worker in
    an unbounded loop.  Consequently every strategy scheduled after partial
    take-profit (including trailing stop) appeared disabled even when its
    feature flag was on.  On timeout, callers use the latest available market
    inputs and continue through all position-protection strategies.
    """
    timeout = max(
        0.0,
        PROFIT_MARKET_CONVERGENCE_TIMEOUT_SEC
        if timeout_sec is None
        else float(timeout_sec),
    )
    poll = max(
        0.01,
        PROFIT_MARKET_CONVERGENCE_POLL_SEC if poll_sec is None else float(poll_sec),
    )
    deadline = time.monotonic() + timeout
    while True:
        converged, reason = adjustment.is_data_converged_for_round(decision_round_ts)
        if converged:
            return True, reason
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, reason
        print(f"⏳ weak-market profit adjustment round={decision_round_ts} waiting: {reason}")
        time.sleep(min(poll, remaining))


def _initialize_base_database() -> None:
    """Recreate the complete base schema after startup or file recovery.

    ``collector.init_db`` owns the candle tables, but the indicator tables are
    owned by ``data_processor``.  A recovered base database must initialize
    both sets before the recovery fence is removed; otherwise the processor and
    scoring child can observe a healthy SQLite file with no MA20 table.
    """
    collector.init_db()
    init_ma20_table(db_path=db_config.BASE_DB_PATH)
    init_ema_table(db_path=db_config.BASE_DB_PATH)
    init_macd_table(db_path=db_config.BASE_DB_PATH)
    feature_flags.init_feature_flags(db_config.BASE_DB_PATH)


def _database_initializers() -> dict[str, Callable[[], None]]:
    return {
        db_config.BASE_DB_PATH: _initialize_base_database,
        db_config.SCORING_DB_PATH: lambda: (
            PreSafetyModule(db_path=db_config.SCORING_DB_PATH).init_table(),
            CooldownModule(db_path=db_config.SCORING_DB_PATH).init_table(),
            ScoringSystem(db_path=db_config.SCORING_DB_PATH).init_table(),
            OpenableSymbolModule(db_path=db_config.SCORING_DB_PATH).init_table(),
            DynamicOpenThresholdModule(db_path=db_config.SCORING_DB_PATH).init_table(),
        ),
        db_config.MARKET_DB_PATH: lambda: (
            MarketFilterModule(db_path=db_config.MARKET_DB_PATH).init_table(),
            AddPositionPermissionModule(db_path=db_config.MARKET_DB_PATH).init_table(),
        ),
        db_config.TRADING_DB_PATH: lambda: (
            TradingExperiment(db_path=db_config.TRADING_DB_PATH).init_error_tables(),
            HoldingPositionScoringSystem(db_path=db_config.TRADING_DB_PATH).init_tables(),
            BreakEvenTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH).init_tables(),
            PartialTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH).init_tables(),
            DynamicProfitProtection(db_path=db_config.TRADING_DB_PATH).init_tables(),
            TrailingStopTracker(db_path=db_config.TRADING_DB_PATH).init_tables(),
            TrailingReductionTracker(db_path=db_config.TRADING_DB_PATH).init_tables(),
            DynamicAddPositionThresholdModule(db_path=db_config.TRADING_DB_PATH).init_table(),
        ),
        db_config.TRADING_CORE_DB_PATH: lambda: (
            TradingExperiment(db_path=db_config.TRADING_DB_PATH).init_core_tables(),
            # These tables were moved out of trading.db together.  Recreate
            # them when trading_core.db is recovered independently; otherwise
            # the next holding round fails on its first stop-loss write and the
            # sequential reduction and portfolio-risk stages never run.
            HoldingPositionScoringSystem(
                db_path=db_config.TRADING_DB_PATH
            ).init_tables(),
            ZombieForceLiquidationModule(db_path=db_config.TRADING_DB_PATH).init_tables(),
        ),
    }


def check_worker_databases() -> dict[str, list[str]]:
    """Fence, drain, replace, and initialize each malformed database."""
    recovered: dict[str, list[str]] = {}
    for db_path, initialize in _database_initializers().items():
        ok, detail = quick_check_sqlite_database(db_path)
        if ok:
            # Clear a stale fence left by a worker crash after a successful
            # rebuild, or a transient Web-side detection that now checks clean.
            Path(db_config.database_recovery_marker(db_path)).unlink(missing_ok=True)
            continue
        detail_lower = detail.lower()
        if "database is locked" in detail_lower or "database is busy" in detail_lower:
            # A health probe can overlap a legitimately long write transaction.
            # Contention is not corruption: quarantining here would discard a
            # healthy database merely because quick_check exhausted its timeout.
            print(f"⚠️ SQLite health check deferred db={db_path}; detail={detail}")
            continue
        marker = Path(db_config.database_recovery_marker(db_path))
        marker.write_text(f"pid={os.getpid()} detail={detail}\n", encoding="utf-8")
        # The marker rejects new connections. EX waits until every managed main
        # or ATTACH connection has closed, so no process retains the old inode.
        with db_config.sqlite_access_lock(db_path, exclusive=True):
            quarantined = quarantine_sqlite_database(db_path)
        try:
            with db_config.sqlite_recovery_bypass(db_path):
                initialize()
                verified, verify_detail = quick_check_sqlite_database(db_path)
            if not verified:
                raise sqlite3.DatabaseError(
                    f"SQLite recovery verification failed db={db_path}: {verify_detail}"
                )
        except Exception:
            # Keep the marker in place: business access must remain fenced if
            # creation or verification failed.
            raise
        else:
            marker.unlink(missing_ok=True)
            recovered[db_path] = quarantined
            print(f"✅ SQLite recovered db={db_path}; quarantined={quarantined}")
    return recovered


def recover_after_worker_error(exc: BaseException) -> bool:
    """Fence and replace a malformed worker database."""
    if not is_malformed_database_error(exc):
        return False
    # Recovery needs an exclusive access lock.  Do not wait on the connection
    # owned by the current round when a nested strategy reports corruption.
    for db_path in db_config.DB_LABELS.values():
        db_config.close_scoped_connection(db_path)
    check_worker_databases()
    return True


def start_database_health_check_task() -> None:
    """Run quick_check and recover every configured database every five minutes."""
    while True:
        try:
            check_worker_databases()
        except Exception as exc:
            print(f"⚠️ SQLite health check failed: {exc}")
        time.sleep(DATABASE_HEALTH_CHECK_INTERVAL_SEC)


def verify_db_writable(db_path: str) -> None:
    """Fail fast with actionable diagnostics when DB path is not writable."""
    data_dir = collector.DATA_DIR

    try:
        collector.init_db()
    except sqlite3.OperationalError as exc:
        if "readonly" not in str(exc).lower():
            raise
        dir_writable = os.access(data_dir, os.W_OK)
        file_exists = os.path.exists(db_path)
        file_writable = os.access(db_path, os.W_OK) if file_exists else False
        raise RuntimeError(
            "Database is readonly. "
            f"db_path={db_path}, data_dir={data_dir}, "
            f"data_dir_writable={dir_writable}, "
            f"db_exists={file_exists}, db_writable={file_writable}. "
            "Check host volume permissions (chown/chmod) for the mounted ./data directory."
        ) from exc


def ensure_universe() -> List[str]:
    """Return current universe snapshot and refresh it every 12 hours."""
    global _universe_last_refresh_ts
    with _universe_lock:
        now_ts = time.time()
        should_refresh = (
            collector.UNIVERSE is None
            or (now_ts - _universe_last_refresh_ts) >= _universe_refresh_interval_sec
        )
        if should_refresh:
            collector.UNIVERSE = collector.build_universe()
            _universe_last_refresh_ts = now_ts
        return list(collector.UNIVERSE)


def run_first_experiment_after_openable_round(
    openable_symbols: Iterable[OpenableSymbol], round_ts: int
) -> None:
    """Run the first experiment only after openable-symbol evaluation is complete."""
    openable_rows = list(openable_symbols)
    qualified_openable_count = sum(1 for row in openable_rows if row.qualified)

    try:
        market_result = None
        if feature_flags.is_feature_enabled(feature_flags.MARKET_FILTER):
            market_filter = MarketFilterModule(db_path=db_config.MARKET_DB_PATH)
            market_result = market_filter.run_round(decision_round_ts=round_ts)
            print(
                f"🌐 market filter round={round_ts} allow={market_result.allow_new_positions} "
                f"allusdt_delta={market_result.allusdt_delta} btc_delta={market_result.btc_delta} "
                f"reason={market_result.reason}"
            )
        else:
            print(f"⏸️ market filter disabled before trading round={round_ts}; skipping market block check")

        zombie_result = ZombieForceLiquidationModule(
            db_path=db_config.TRADING_DB_PATH
        ).run_round(checked_at=round_ts)
        print(
            f"🧟 zombie force liquidation before open round={round_ts} "
            f"checked={zombie_result.get('checked', 0)} "
            f"triggered={zombie_result.get('triggered', 0)} "
            f"records={zombie_result.get('records', 0)}"
        )
        if qualified_openable_count <= 0:
            print(
                f"🧪 first trading experiment round={round_ts} skipped after zombie force liquidation: no qualified symbols"
            )
            return
        if market_result is not None and not market_result.allow_new_positions:
            print(f"🧪 first trading experiment round={round_ts} skipped by market filter: {market_result.reason}")
            return
        if not feature_flags.is_feature_enabled(feature_flags.TRADING_SYSTEM):
            print(f"⏸️ trading system disabled round={round_ts}; skipping new positions")
            return
        experiment_result = TradingExperiment(db_path=db_config.TRADING_DB_PATH).run_round(
            openable_rows
        )
        print(
            f"🧪 first trading experiment after openable round={round_ts} "
            f"opened={experiment_result.get('opened', 0)} "
            f"skipped={experiment_result.get('skipped', 0)} "
            f"reason={experiment_result.get('reason', '')}"
        )
        # The regular holding round runs before the openable-symbol and trading
        # stages.  When an account starts a decision round with no positions
        # (notably just after a Binance account reset), that first pass quite
        # correctly persists zero portfolio positions; positions opened below
        # would otherwise remain absent from the holding checks and portfolio
        # risk UI until the next 15-minute round.  Re-run the idempotent holding
        # pipeline after a successful open so the current round immediately
        # reflects and protects the newly created positions.
        if int(experiment_result.get("opened", 0) or 0) > 0:
            try:
                holding_result = HoldingPositionScoringSystem(
                    db_path=db_config.TRADING_DB_PATH
                ).run_round(
                    decision_round_ts=round_ts,
                    enable_stop_loss=feature_flags.is_feature_enabled(feature_flags.STOP_LOSS_RULE),
                    enable_reduction=feature_flags.is_feature_enabled(feature_flags.REDUCTION_CONDITIONS),
                    enable_increase=feature_flags.is_feature_enabled(feature_flags.INCREASE_CONDITIONS),
                    enable_portfolio_risk=feature_flags.is_feature_enabled(feature_flags.PORTFOLIO_RISK),
                )
                print(
                    f"📊 holding scoring refreshed after open round={round_ts} "
                    f"checked={holding_result.get('checked', 0)} "
                    f"risk_positions={holding_result.get('risk_position_count', 0)}"
                )
            except Exception as exc:
                recover_after_worker_error(exc)
                print(
                    f"⚠️ holding scoring refresh failed after open "
                    f"round={round_ts}: {exc}"
                )
    except Exception as exc:
        recover_after_worker_error(exc)
        print(
            f"⚠️ first trading experiment failed after openable round={round_ts}: {exc}"
        )


SCORING_WORKER_DEADLINE_MS = 10 * 60_000
SCORING_WORKER_TERMINATE_GRACE_MS = 2 * 60_000


def _scoring_worker_should_stop(
    *,
    decision_round_ts: int,
    deadline_ts: int | None,
    stage: str,
    now_ms: Callable[[], int] | None = None,
) -> bool:
    """Return True when a scoring worker should stop at a safe checkpoint."""
    if deadline_ts is None:
        return False
    clock = now_ms or (lambda: int(time.time() * 1000))
    current_ts = clock()
    if current_ts < deadline_ts:
        return False
    print(
        f"⏱️ scoring worker stopping safely round={decision_round_ts} "
        f"stage={stage} now={current_ts} deadline={deadline_ts}"
    )
    return True


def _reap_or_timeout_scoring_worker(
    *,
    process: multiprocessing.Process | None,
    round_ts: int | None,
    deadline_ts: int | None,
    terminate_after_ts: int | None,
    now_ms: int,
) -> tuple[multiprocessing.Process | None, int | None, int | None, int | None]:
    """Reap finished scoring workers and force-stop workers stuck past grace.

    Workers normally exit at safe checkpoints after the scoring deadline.  If a
    worker gets stuck inside a long-running call and never reaches the next
    checkpoint, the scheduler must eventually clear it so subsequent 15m rounds
    can start.
    """
    if process is None:
        return None, None, None, None

    if not process.is_alive():
        process.join(timeout=0)
        status = "✅" if process.exitcode == 0 else "❌"
        print(
            f"{status} scoring worker finished round={round_ts} "
            f"exitcode={process.exitcode}"
        )
        return None, None, None, None

    if terminate_after_ts is not None and now_ms >= terminate_after_ts:
        print(
            f"🛑 scoring round={round_ts} stuck after grace; terminating worker "
            f"pid={process.pid} now={now_ms} terminate_after={terminate_after_ts}"
        )
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            print(
                f"🛑 scoring round={round_ts} still alive after terminate; killing worker "
                f"pid={process.pid}"
            )
            process.kill()
            process.join(timeout=5)
        print(
            f"✅ scoring worker cleared round={round_ts} "
            f"exitcode={process.exitcode}"
        )
        return None, None, None, None

    if deadline_ts is not None and now_ms >= deadline_ts:
        terminate_after_ts = now_ms + SCORING_WORKER_TERMINATE_GRACE_MS
        print(
            f"⏱️ scoring round={round_ts} exceeded deadline "
            f"deadline={deadline_ts}; waiting for safe worker exit until {terminate_after_ts}"
        )
        deadline_ts = None

    return process, round_ts, deadline_ts, terminate_after_ts


def run_scoring_round_worker(
    db_path: str,
    decision_round_ts: int,
    symbols: List[str],
    abnormal_symbols: List[str],
    evaluated_at: int,
    deadline_ts: int | None = None,
) -> None:
    """Run one scoring round in an isolated process.

    The parent process owns scheduling, but the child exits only at explicit safe
    checkpoints so SQLite writes are not interrupted by forced termination.
    """
    scoring = ScoringSystem(db_path=db_path)
    scoring.init_table()
    openable = OpenableSymbolModule(db_path=db_path)
    openable.init_table()
    dynamic_open_threshold = DynamicOpenThresholdModule(db_path=db_path)
    dynamic_open_threshold.init_table()
    if _scoring_worker_should_stop(
        decision_round_ts=decision_round_ts,
        deadline_ts=deadline_ts,
        stage="before_ma20_readiness",
    ):
        return

    readiness = scoring.wait_for_15m_ma20_readiness_for_round(
        decision_round_ts=decision_round_ts,
        symbols=symbols,
        retries=6,
        retry_delay_seconds=5.0,
    )
    if not readiness.ready:
        scoring.record_ma20_skip_for_round(
            decision_round_ts=decision_round_ts,
            readiness=readiness,
            universe_count=len(symbols),
            created_at=int(time.time() * 1000),
        )
        missing_preview = ",".join(readiness.missing_symbols[:10])
        if len(readiness.missing_symbols) > 10:
            missing_preview += ",..."
        print(
            f"⚠️ scoring round={decision_round_ts} skipping symbols missing converged 15m MA20/EMA20 "
            f"target_open_time={readiness.target_open_time} "
            f"ready={len(readiness.ready_symbols)} "
            f"missing={len(readiness.missing_symbols)} "
            f"missing_symbols={missing_preview}"
        )

    if _scoring_worker_should_stop(
        decision_round_ts=decision_round_ts,
        deadline_ts=deadline_ts,
        stage="before_score_round",
    ):
        return

    scored = scoring.score_round(
        decision_round_ts=decision_round_ts,
        all_symbols=readiness.ready_symbols,
        abnormal_symbols=abnormal_symbols,
    )
    print(
        f"🧮 scoring round={decision_round_ts} universe={len(symbols)} "
        f"ready={len(readiness.ready_symbols)} "
        f"abnormal={len(set(abnormal_symbols))} scored={len(scored)}"
    )

    try:
        # A completed score round must always publish the holding stop-loss
        # judgement that consumes it.  Previously the generic scoring deadline
        # could return at either of the two checkpoints above this block.  When
        # scoring used most of its time budget (for example after schema work
        # during a database split), the holding table then appeared to have
        # stopped even though scoring itself completed successfully.  Keep the
        # deadline for optional downstream work, but never strand this
        # safety-critical result between two decision rounds.
        # Trading-owned modules are optional consumers of a completed scoring
        # round.  Initialize them here, rather than before score_round, so a
        # damaged/recovering trading DB cannot stop the independent scoring DB.
        holding_scoring = HoldingPositionScoringSystem(
            db_path=db_config.TRADING_DB_PATH
        )
        holding_scoring.init_tables()
        trailing_reduction = TrailingReductionTracker(
            db_path=db_config.TRADING_DB_PATH
        )
        trailing_reduction.init_tables()
        with db_config.sqlite_connection_scopes(
            db_config.TRADING_DB_PATH,
            db_config.trading_core_path(db_config.TRADING_DB_PATH),
            row_factory=sqlite3.Row,
        ):
            holding_result = holding_scoring.run_round(
                decision_round_ts=decision_round_ts,
                enable_stop_loss=feature_flags.is_feature_enabled(feature_flags.STOP_LOSS_RULE),
                enable_reduction=feature_flags.is_feature_enabled(feature_flags.REDUCTION_CONDITIONS),
                enable_increase=feature_flags.is_feature_enabled(feature_flags.INCREASE_CONDITIONS),
                enable_portfolio_risk=feature_flags.is_feature_enabled(feature_flags.PORTFOLIO_RISK),
            )
            print(
                f"📊 holding scoring round={decision_round_ts} "
                f"checked={holding_result.get('checked', 0)} "
                f"triggered={holding_result.get('triggered', 0)} "
                f"records={holding_result.get('records', 0)} "
                f"reduction_checked={holding_result.get('reduction_checked', 0)} "
                f"reduction_triggered={holding_result.get('reduction_triggered', 0)}"
            )
            if _scoring_worker_should_stop(
                decision_round_ts=decision_round_ts,
                deadline_ts=deadline_ts,
                stage="after_holding_scoring",
            ):
                return
            if not feature_flags.is_feature_enabled(feature_flags.TRAILING_REDUCTION):
                print("⏸️ trailing reduction skipped: feature flag disabled")
            else:
                trailing_reduction_result = trailing_reduction.run_round(decision_round_ts=decision_round_ts)
                print(
                    f"🧭 trailing reduction round={decision_round_ts} "
                    f"checked={trailing_reduction_result.get('checked', 0)} "
                    f"eligible={trailing_reduction_result.get('eligible', 0)} "
                    f"pretriggered={trailing_reduction_result.get('pretriggered', 0)} "
                    f"2R={trailing_reduction_result.get('trigger_r_usdt', '')}"
                )
    except Exception as exc:
        recover_after_worker_error(exc)
        print(f"⚠️ holding scoring failed round={decision_round_ts}: {exc}")

    if _scoring_worker_should_stop(
        decision_round_ts=decision_round_ts,
        deadline_ts=deadline_ts,
        stage="before_dynamic_threshold",
    ):
        return

    dynamic_threshold_result = dynamic_open_threshold.run_round(
        decision_round_ts=decision_round_ts, evaluated_at=evaluated_at
    )
    print(
        f"🚦 dynamic open threshold round={decision_round_ts} "
        f"highest={dynamic_threshold_result.highest_total_score} "
        f"min_open={dynamic_threshold_result.min_open_total_score} "
        f"allow={dynamic_threshold_result.allow_new_positions} "
        f"policy={dynamic_threshold_result.policy}"
    )

    market_filter_result = None
    if feature_flags.is_feature_enabled(feature_flags.MARKET_FILTER):
        try:
            market_filter_result = MarketFilterModule(
                db_path=db_config.MARKET_DB_PATH
            ).get_result_for_round(decision_round_ts)
        except Exception as exc:
            recover_after_worker_error(exc)
            print(f"⚠️ market filter lookup failed round={decision_round_ts}: {exc}")

    allow_new_positions = dynamic_threshold_result.allow_new_positions
    openable_reason = dynamic_threshold_result.reason
    if market_filter_result is not None and not market_filter_result.allow_new_positions:
        allow_new_positions = False
        openable_reason = f"market_filter_blocked:{market_filter_result.reason}"
        print(
            f"🚫 openable round={decision_round_ts} blocked by independent market filter "
            f"despite dynamic_threshold_allow={dynamic_threshold_result.allow_new_positions}: "
            f"{market_filter_result.reason}"
        )

    if _scoring_worker_should_stop(
        decision_round_ts=decision_round_ts,
        deadline_ts=deadline_ts,
        stage="before_openable",
    ):
        return

    openable_symbols = openable.run_round(
        decision_round_ts=decision_round_ts,
        evaluated_at=evaluated_at,
        min_total_score=dynamic_threshold_result.min_open_total_score,
        allow_new_positions=allow_new_positions,
        threshold_reason=openable_reason,
    )
    qualified_openable_count = sum(1 for row in openable_symbols if row.qualified)
    print(
        f"🚪 openable round={decision_round_ts} candidates={len(openable_symbols)} "
        f"qualified={qualified_openable_count}"
    )

    if _scoring_worker_should_stop(
        decision_round_ts=decision_round_ts,
        deadline_ts=deadline_ts,
        stage="before_first_experiment",
    ):
        return

    run_first_experiment_after_openable_round(openable_symbols, decision_round_ts)


def start_break_even_take_profit_task() -> None:
    """Run break-even protection first, then partial take-profit, every minute."""
    strategy = BreakEvenTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH)
    partial_strategy = PartialTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH)
    dynamic_profit_protection = DynamicProfitProtection(db_path=db_config.TRADING_DB_PATH)
    trailing_stop_tracker = TrailingStopTracker(db_path=db_config.TRADING_DB_PATH)
    strategy.init_tables()
    partial_strategy.init_tables()
    dynamic_profit_protection.init_tables()
    trailing_stop_tracker.init_tables()
    weak_market_adjustment = WeakMarketProfitAdjustmentModule(db_path=db_config.MARKET_DB_PATH)
    weak_market_adjustment.init_table()
    print("🟢 Break-even, partial take-profit, dynamic profit protection and trailing stop tracker task started")
    while True:
        with db_config.sqlite_connection_scope(
            db_config.TRADING_DB_PATH, row_factory=sqlite3.Row
        ):
            try:
                reconcile_result = TradingExperiment(
                    db_path=db_config.TRADING_DB_PATH
                ).reconcile_missing_exit_orders()
                print(
                    f"🧩 exit-order reconcile checked={reconcile_result.get('checked', 0)} "
                    f"created={reconcile_result.get('created', 0)} "
                    f"errors={reconcile_result.get('errors', 0)}"
                )
            except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ exit-order reconcile failed: {exc}")

            if feature_flags.is_feature_enabled(feature_flags.BREAK_EVEN_TAKE_PROFIT):
              try:
                result = strategy.run_round()
                print(
                    f"🟢 break-even take-profit checked={result.get('checked', 0)} "
                    f"triggered={result.get('triggered', 0)} "
                    f"records={result.get('records', 0)} R={result.get('r_usdt', '')}"
                )
              except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ break-even take-profit failed: {exc}")

            try:
                scan_ms = int(time.time() * 1000)
                market_round_ts = WeakMarketProfitAdjustmentModule.decision_round_ts(scan_ms)
                # The first minute-level scan observed in every quarter-hour must
                # not overtake the adjustment, even when this worker started late.
                if weak_market_adjustment.latest_result_for_round(market_round_ts) is None:
                    converged, convergence_reason = wait_for_profit_market_convergence(
                        weak_market_adjustment, market_round_ts
                    )
                    if not converged:
                        print(
                            f"⚠️ weak-market profit adjustment round={market_round_ts} "
                            f"convergence timeout ({convergence_reason}); using latest data "
                            "so position-protection strategies can continue"
                        )
                    adjustment = weak_market_adjustment.run_round(market_round_ts)
                    print(f"📉 weak-market profit adjustment round={market_round_ts} weak={adjustment.weak_market} trigger={adjustment.trigger_r_multiple}R fraction={adjustment.take_profit_fraction}")
                partial_result = partial_strategy.run_round(decision_round_ts=scan_ms) if feature_flags.is_feature_enabled(feature_flags.PARTIAL_TAKE_PROFIT) else {"checked": 0, "triggered": 0, "records": 0}
                print(
                    f"🟢 partial take-profit checked={partial_result.get('checked', 0)} "
                    f"triggered={partial_result.get('triggered', 0)} "
                    f"records={partial_result.get('records', 0)} 2R={partial_result.get('trigger_r_usdt', '')}"
                )
            except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ partial take-profit failed: {exc}")

            if feature_flags.is_feature_enabled(feature_flags.DYNAMIC_PROFIT_PROTECTION):
              try:
                dynamic_result = dynamic_profit_protection.run_round()
                print(
                    f"🟢 dynamic profit protection checked={dynamic_result.get('checked', 0)} "
                    f"eligible={dynamic_result.get('eligible', 0)} "
                    f"triggered={dynamic_result.get('triggered', 0)} R={dynamic_result.get('r_usdt', '')}"
                )
              except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ dynamic profit protection failed: {exc}")

            if feature_flags.is_feature_enabled(feature_flags.TRAILING_STOP):
                try:
                    trailing_result = trailing_stop_tracker.run_round()
                    print(
                        f"🟢 trailing stop tracker checked={trailing_result.get('checked', 0)} "
                        f"eligible={trailing_result.get('eligible', 0)} "
                        f"updated={trailing_result.get('updated', 0)}"
                    )
                except Exception as exc:
                    recover_after_worker_error(exc)
                    print(f"⚠️ trailing stop tracker failed: {exc}")
            else:
                print("⏸️ trailing stop tracker skipped: feature flag disabled")

        time.sleep(60)


def start_pre_safety_task() -> None:
    """Run pre-safety abnormal wick detection in an isolated daemon thread.

    This task only reads existing 5m candle data and writes its own event table,
    so it will not interfere with collector/MA20 pipelines.
    """
    module = PreSafetyModule(db_path=db_config.SCORING_DB_PATH)
    module.init_table()
    cooldown = CooldownModule(db_path=db_config.SCORING_DB_PATH)
    cooldown.init_table()
    ScoringSystem(db_path=db_config.SCORING_DB_PATH).init_table()
    OpenableSymbolModule(db_path=db_config.SCORING_DB_PATH).init_table()
    HoldingPositionScoringSystem(db_path=db_config.TRADING_DB_PATH).init_tables()
    market_filter = MarketFilterModule(db_path=db_config.MARKET_DB_PATH)
    market_filter.init_table()
    add_permission = AddPositionPermissionModule(db_path=db_config.MARKET_DB_PATH)
    add_permission.init_table()
    DynamicOpenThresholdModule(db_path=db_config.SCORING_DB_PATH).init_table()
    dynamic_add_threshold = DynamicAddPositionThresholdModule(db_path=db_config.TRADING_DB_PATH)
    dynamic_add_threshold.init_table()

    last_pre_safety_round_ts = None
    last_scoring_started_round_ts = None
    last_add_permission_round_ts = None
    active_scoring_process: multiprocessing.Process | None = None
    active_scoring_round_ts: int | None = None
    active_scoring_deadline_ts: int | None = None
    active_scoring_terminate_after_ts: int | None = None
    round_ms = 15 * 60_000

    print("🛡️ Pre-safety task started")
    while True:
        symbols = ensure_universe()
        now_ms = int(time.time() * 1000)
        round_ts = (now_ms // round_ms) * round_ms

        scoring_execute_ts = round_ts + 30_000
        scoring_enabled = feature_flags.is_feature_enabled(feature_flags.SCORING_SYSTEM)
        market_filter_enabled = feature_flags.is_feature_enabled(feature_flags.MARKET_FILTER)

        if round_ts != last_pre_safety_round_ts:
            if scoring_enabled:
                for symbol in symbols:
                    try:
                        events = module.detect_for_symbol(symbol, now_ms=now_ms)
                        for event in events:
                            print(
                                f"🚨 abnormal wick {event.symbol} "
                                f"round={event.decision_round_ts} "
                                f"candle_index={event.candle_index} "
                                f"first_open={event.first_candle_open_time} "
                                f"cond1={event.cond1_ratio:.6f} cond2={event.cond2_ratio:.6f}"
                            )
                    except Exception as exc:  # keep this side-task isolated
                        recover_after_worker_error(exc)
                        print(f"⚠️ pre-safety detect failed symbol={symbol}: {exc}")
            else:
                print(f"⏸️ scoring system disabled round={round_ts}; skipping pre-safety, cooldown and scoring")

            if market_filter_enabled:
                try:
                    market_result = market_filter.run_round(decision_round_ts=round_ts, evaluated_at=now_ms)
                    print(
                        f"🌐 market filter round={round_ts} allow={market_result.allow_new_positions} "
                        f"allusdt_delta={market_result.allusdt_delta} btc_delta={market_result.btc_delta} "
                        f"reason={market_result.reason}"
                    )
                    dynamic_add_result = dynamic_add_threshold.run_round(decision_round_ts=round_ts, evaluated_at=now_ms)
                    print(
                        f"📈 dynamic add-position threshold round={round_ts} "
                        f"success_rate={dynamic_add_result.success_rate} "
                        f"success={dynamic_add_result.success_count}/{dynamic_add_result.sample_size} "
                        f"threshold={dynamic_add_result.threshold_r_multiple}R"
                    )
                except Exception as exc:
                    recover_after_worker_error(exc)
                    print(f"⚠️ market filter failed round={round_ts}: {exc}")
            else:
                print(f"⏸️ market filter disabled round={round_ts}; skipping market filter and add-position permission")

            if scoring_enabled:
                try:
                    cooldown_symbols = cooldown.run_round(
                        symbols=symbols, decision_round_ts=round_ts, now_ms=now_ms
                    )
                    print(
                        f"🧊 cooldown round={round_ts} universe={len(symbols)} "
                        f"cooldown={len(cooldown_symbols)}"
                    )
                except Exception as exc:
                    recover_after_worker_error(exc)
                    print(f"⚠️ cooldown failed round={round_ts}: {exc}")

            last_pre_safety_round_ts = round_ts

        if market_filter_enabled and last_add_permission_round_ts != round_ts:
            try:
                data_converged, convergence_reason = add_permission.is_data_converged_for_round(round_ts)
                if data_converged:
                    add_permission_result = add_permission.run_round(decision_round_ts=round_ts, evaluated_at=now_ms)
                    last_add_permission_round_ts = round_ts
                    print(
                        f"➕ add-position permission round={round_ts} allow={add_permission_result.allow_add_positions} "
                        f"allusdt_delta={add_permission_result.allusdt_delta} btc_delta={add_permission_result.btc_delta} "
                        f"reason={add_permission_result.reason}"
                    )
                else:
                    print(
                        f"⏳ add-position permission round={round_ts} waiting for data convergence: "
                        f"{convergence_reason}"
                    )
            except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ add-position permission failed round={round_ts}: {exc}")

        (
            active_scoring_process,
            active_scoring_round_ts,
            active_scoring_deadline_ts,
            active_scoring_terminate_after_ts,
        ) = _reap_or_timeout_scoring_worker(
            process=active_scoring_process,
            round_ts=active_scoring_round_ts,
            deadline_ts=active_scoring_deadline_ts,
            terminate_after_ts=active_scoring_terminate_after_ts,
            now_ms=now_ms,
        )

        if (
            active_scoring_process is None
            and round_ts != last_scoring_started_round_ts
            and now_ms >= scoring_execute_ts
        ):
            if not scoring_enabled:
                last_scoring_started_round_ts = round_ts
                time.sleep(5)
                continue
            try:
                _, abnormal_symbols = module.get_latest_round_abnormal_symbols(
                    decision_round_ts=round_ts
                )
                active_scoring_deadline_ts = round_ts + SCORING_WORKER_DEADLINE_MS
                active_scoring_terminate_after_ts = None
                active_scoring_round_ts = round_ts
                active_scoring_process = multiprocessing.Process(
                    target=run_scoring_round_worker,
                    args=(
                        db_config.SCORING_DB_PATH,
                        round_ts,
                        list(symbols),
                        list(abnormal_symbols),
                        now_ms,
                        active_scoring_deadline_ts,
                    ),
                    name=f"scoring-round-{round_ts}",
                )
                active_scoring_process.start()
                last_scoring_started_round_ts = round_ts
                print(
                    f"🚀 scoring worker started round={round_ts} "
                    f"pid={active_scoring_process.pid} deadline={active_scoring_deadline_ts}"
                )
            except Exception as exc:
                recover_after_worker_error(exc)
                print(f"⚠️ scoring worker start failed round={round_ts}: {exc}")
                active_scoring_process = None
                active_scoring_round_ts = None
                active_scoring_deadline_ts = None
                active_scoring_terminate_after_ts = None

        time.sleep(5)


def start_increase_pretrigger_refresh_task() -> None:
    """Refresh pre-triggered first-add symbols once per minute."""
    holding_scoring = HoldingPositionScoringSystem(db_path=db_config.TRADING_DB_PATH)
    holding_scoring.init_tables()
    print("🟣 Increase pre-trigger refresh task started")
    while True:
        if not feature_flags.is_feature_enabled(feature_flags.INCREASE_CONDITIONS):
            print("⏸️ increase pretrigger refresh skipped: feature flag disabled")
            time.sleep(60)
            continue
        try:
            with db_config.sqlite_connection_scope(
                db_config.TRADING_DB_PATH, row_factory=sqlite3.Row
            ):
                result = holding_scoring.refresh_pretrigger_increase_checks()
            if result.get("refreshed", 0):
                print(
                    f"🟣 increase pretrigger refresh round={result.get('round_ts')} "
                    f"refreshed={result.get('refreshed', 0)} "
                    f"triggered={result.get('triggered', 0)} "
                    f"records={result.get('records', 0)}"
                )
        except Exception as exc:
            recover_after_worker_error(exc)
            print(f"⚠️ increase pretrigger refresh failed: {exc}")
        time.sleep(60)


def on_ma20_result(result: MACalcResult) -> None:
    """Log a calculated result; interval completion persists the whole batch."""
    print(
        f"📈 MA20 {result.symbol} {result.interval} "
        f"open_time={result.open_time} close={result.close:.6f} ma20={result.ma20:.6f}"
    )
    if (
        result.interval == "15m"
        and result.ema12 is not None
        and result.ema16 is not None
        and result.ema21 is not None
        and result.ema26 is not None
    ):
        print(
            f"📈 EMA {result.symbol} {result.interval} "
            f"open_time={result.open_time} close={result.close:.6f} "
            f"ema12={result.ema12:.6f} ema16={result.ema16:.6f} "
            f"ema21={result.ema21:.6f} ema26={result.ema26:.6f}"
        )


def on_indicator_interval_complete(interval: str, results: List[MACalcResult]) -> None:
    """Persist all indicators for the completed interval in one transaction."""
    saved = save_indicator_results(db_config.BASE_DB_PATH, results)

    for result in results:
        if (
            result.macd_dif is None
            or result.macd_dea is None
            or result.macd_histogram is None
        ):
            continue
        print(
            f"📈 MACD {result.symbol} {result.interval} "
            f"open_time={result.open_time} close={result.close:.6f} "
            f"dif={result.macd_dif:.6f} dea={result.macd_dea:.6f} "
            f"macd={result.macd_histogram:.6f}"
        )

    print(
        f"📈 indicator {interval} round committed in one batch, "
        f"ma20={saved['ma20']} ema={saved['ema']} macd={saved['macd']}"
    )


def start_collector_task(symbols: List[str]) -> None:
    collector.init_db()
    collector.UNIVERSE = list(symbols)

    def _run_with_fresh_universe(job_func):
        if not feature_flags.is_feature_enabled(feature_flags.BASE_DATA_COLLECTION):
            print(f"⏸️ base data collection disabled; skipping {job_func.__name__}")
            return
        ensure_universe()
        job_func()

    scheduler = collector.BlockingScheduler()
    scheduler.add_job(ensure_universe, "interval", hours=12)
    scheduler.add_job(
        lambda: _run_with_fresh_universe(collector.kline_job), "cron", second=0
    )
    scheduler.add_job(
        lambda: _run_with_fresh_universe(collector.oi_job), "cron", second=20
    )
    scheduler.add_job(
        lambda: _run_with_fresh_universe(collector.funding_job),
        "cron",
        minute=1,
        second=40,
    )
    scheduler.add_job(
        lambda: _run_with_fresh_universe(collector.btc_5m_job),
        "cron",
        minute="*/5",
        second=10,
    )

    print("🚀 Collector task started")
    scheduler.start()


def start_atr_15m_task(symbols: List[str]) -> None:
    """Run 15m ATR collection in its own scheduler thread.

    ATR is deliberately isolated from the scoring scheduler/process so a failed
    ATR round is logged by ``collector.atr_15m_job`` and cannot block scoring.
    """
    collector.init_db()
    collector.UNIVERSE = list(symbols)

    def _run_with_fresh_universe():
        if not feature_flags.is_feature_enabled(feature_flags.BASE_DATA_COLLECTION):
            print("⏸️ base data collection disabled; skipping atr_15m_job")
            return
        ensure_universe()
        collector.atr_15m_job()
        if not feature_flags.is_feature_enabled(feature_flags.TRAILING_REDUCTION):
            print("⏸️ trailing reduction after ATR skipped: feature flag disabled")
            return
        try:
            with db_config.sqlite_connection_scope(
                db_config.TRADING_DB_PATH, row_factory=sqlite3.Row
            ):
                result = TrailingReductionTracker(
                    db_path=db_config.TRADING_DB_PATH
                ).run_round(decision_round_ts=int(time.time() * 1000))
            print(
                f"🧭 trailing reduction after ATR checked={result.get('checked', 0)} "
                f"eligible={result.get('eligible', 0)} pretriggered={result.get('pretriggered', 0)}"
            )
        except Exception as exc:
            recover_after_worker_error(exc)
            print(f"⚠️ trailing reduction after ATR failed: {exc}")

    scheduler = collector.BlockingScheduler()
    scheduler.add_job(ensure_universe, "interval", hours=12)
    scheduler.add_job(_run_with_fresh_universe, "cron", minute="*/15", second=30)

    print("🚀 ATR 15m task started")
    scheduler.start()



def start_trailing_reduction_refresh_task() -> None:
    tracker = TrailingReductionTracker(db_path=db_config.TRADING_DB_PATH)
    tracker.init_tables()
    scheduler = collector.BlockingScheduler()

    def _job():
        if not feature_flags.is_feature_enabled(feature_flags.TRAILING_REDUCTION):
            print("⏸️ trailing reduction refresh skipped: feature flag disabled")
            return
        try:
            with db_config.sqlite_connection_scope(
                db_config.TRADING_DB_PATH, row_factory=sqlite3.Row
            ):
                result = tracker.refresh_pretriggered_symbols()
            print(
                f"🧭 trailing reduction refresh refreshed={result.get('refreshed', 0)} "
                f"triggered={result.get('triggered', 0)} records={result.get('records', 0)}"
            )
        except Exception as exc:
            recover_after_worker_error(exc)
            print(f"⚠️ trailing reduction refresh failed: {exc}")

    scheduler.add_job(_job, "cron", second=45)
    print("🚀 Trailing reduction pretrigger refresh task started")
    scheduler.start()

def start_processor_task(symbols: List[str]) -> None:
    init_ma20_table(db_path=db_config.BASE_DB_PATH)
    init_ema_table(db_path=db_config.BASE_DB_PATH)
    init_macd_table(db_path=db_config.BASE_DB_PATH)
    processor = MA20Processor(db_path=db_config.BASE_DB_PATH)
    scheduler = MA20Scheduler(grace_seconds=5)

    print(f"🚀 MA20/MACD processor task started, symbols={len(symbols)}")
    run_loop(
        symbols=symbols,
        processor=processor,
        scheduler=scheduler,
        on_result=on_ma20_result,
        symbol_provider=lambda: ensure_universe()
        if feature_flags.is_feature_enabled(feature_flags.BASE_DATA_COLLECTION)
        else [],
        poll_seconds=20,
        on_interval_complete=on_indicator_interval_complete,
    )


if __name__ == "__main__":
    collector.database_error_handler = recover_after_worker_error
    verify_db_writable(db_config.BASE_DB_PATH)
    feature_flags.init_feature_flags(db_config.BASE_DB_PATH)
    # 预先构建一次 universe，并按12小时周期刷新
    symbols = ensure_universe()

    # 七个独立 task：collector / ATR 15m / pre_safety / break_even_take_profit / 加仓预触发刷新 / 移动追踪减仓刷新 / data_processor
    collector_thread = threading.Thread(
        target=start_collector_task, args=(symbols,), daemon=True
    )
    collector_thread.start()

    atr_15m_thread = threading.Thread(
        target=start_atr_15m_task, args=(symbols,), daemon=True
    )
    atr_15m_thread.start()

    pre_safety_thread = threading.Thread(target=start_pre_safety_task, daemon=True)
    pre_safety_thread.start()

    break_even_thread = threading.Thread(
        target=start_break_even_take_profit_task, daemon=True
    )
    break_even_thread.start()

    increase_pretrigger_thread = threading.Thread(
        target=start_increase_pretrigger_refresh_task, daemon=True
    )
    increase_pretrigger_thread.start()

    trailing_reduction_refresh_thread = threading.Thread(
        target=start_trailing_reduction_refresh_task, daemon=True
    )
    trailing_reduction_refresh_thread.start()

    database_health_thread = threading.Thread(
        target=start_database_health_check_task, daemon=True
    )
    database_health_thread.start()

    # 主线程跑 processor task
    start_processor_task(symbols)
