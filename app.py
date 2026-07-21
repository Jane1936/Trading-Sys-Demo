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
from typing import Iterable, List

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
    save_ema_result,
    save_macd_result,
    save_ma20_result,
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
from add_position_permission_module import AddPositionPermissionModule
from dynamic_open_threshold import DynamicOpenThresholdModule
from zombie_force_liquidation import ZombieForceLiquidationModule

_universe_lock = threading.Lock()
_universe_refresh_interval_sec = 12 * 60 * 60
_universe_last_refresh_ts = 0.0


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
    except Exception as exc:
        print(
            f"⚠️ first trading experiment failed after openable round={round_ts}: {exc}"
        )


def run_scoring_round_worker(
    db_path: str,
    decision_round_ts: int,
    symbols: List[str],
    abnormal_symbols: List[str],
    evaluated_at: int,
) -> None:
    """Run one scoring round in an isolated process.

    The parent process owns scheduling and may terminate this worker when the
    round deadline is reached, so stale rounds do not block later decisions.
    """
    scoring = ScoringSystem(db_path=db_path)
    scoring.init_table()
    openable = OpenableSymbolModule(db_path=db_path)
    openable.init_table()
    dynamic_open_threshold = DynamicOpenThresholdModule(db_path=db_path)
    dynamic_open_threshold.init_table()
    holding_scoring = HoldingPositionScoringSystem(db_path=db_config.TRADING_DB_PATH)
    holding_scoring.init_tables()
    trailing_reduction = TrailingReductionTracker(db_path=db_config.TRADING_DB_PATH)
    trailing_reduction.init_tables()

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
            f"⚠️ scoring round={decision_round_ts} skipping symbols missing 15m MA20 "
            f"target_open_time={readiness.target_open_time} "
            f"ready={len(readiness.ready_symbols)} "
            f"missing={len(readiness.missing_symbols)} "
            f"missing_symbols={missing_preview}"
        )

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
        holding_result = holding_scoring.run_round(decision_round_ts=decision_round_ts)
        print(
            f"📊 holding scoring round={decision_round_ts} "
            f"checked={holding_result.get('checked', 0)} "
            f"triggered={holding_result.get('triggered', 0)} "
            f"records={holding_result.get('records', 0)} "
            f"reduction_checked={holding_result.get('reduction_checked', 0)} "
            f"reduction_triggered={holding_result.get('reduction_triggered', 0)}"
        )
        trailing_reduction_result = trailing_reduction.run_round(decision_round_ts=decision_round_ts)
        print(
            f"🧭 trailing reduction round={decision_round_ts} "
            f"checked={trailing_reduction_result.get('checked', 0)} "
            f"eligible={trailing_reduction_result.get('eligible', 0)} "
            f"pretriggered={trailing_reduction_result.get('pretriggered', 0)} "
            f"2R={trailing_reduction_result.get('trigger_r_usdt', '')}"
        )
    except Exception as exc:
        print(f"⚠️ holding scoring failed round={decision_round_ts}: {exc}")

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

    run_first_experiment_after_openable_round(openable_symbols, decision_round_ts)


def start_break_even_take_profit_task() -> None:
    """Run break-even protection and partial take-profit every 5 minutes."""
    strategy = BreakEvenTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH)
    partial_strategy = PartialTakeProfitStrategy(db_path=db_config.TRADING_DB_PATH)
    dynamic_profit_protection = DynamicProfitProtection(db_path=db_config.TRADING_DB_PATH)
    trailing_stop_tracker = TrailingStopTracker(db_path=db_config.TRADING_DB_PATH)
    strategy.init_tables()
    partial_strategy.init_tables()
    dynamic_profit_protection.init_tables()
    trailing_stop_tracker.init_tables()
    print("🟢 Break-even, partial take-profit, dynamic profit protection and trailing stop tracker task started")
    while True:
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
            print(f"⚠️ exit-order reconcile failed: {exc}")

        try:
            partial_result = partial_strategy.run_round()
            print(
                f"🟢 partial take-profit checked={partial_result.get('checked', 0)} "
                f"triggered={partial_result.get('triggered', 0)} "
                f"records={partial_result.get('records', 0)} 2R={partial_result.get('trigger_r_usdt', '')}"
            )
        except Exception as exc:
            print(f"⚠️ partial take-profit failed: {exc}")

        try:
            result = strategy.run_round()
            print(
                f"🟢 break-even take-profit checked={result.get('checked', 0)} "
                f"triggered={result.get('triggered', 0)} "
                f"records={result.get('records', 0)} R={result.get('r_usdt', '')}"
            )
        except Exception as exc:
            print(f"⚠️ break-even take-profit failed: {exc}")

        for _ in range(5):
            try:
                dynamic_result = dynamic_profit_protection.run_round()
                print(
                    f"🟢 dynamic profit protection checked={dynamic_result.get('checked', 0)} "
                    f"eligible={dynamic_result.get('eligible', 0)} "
                    f"triggered={dynamic_result.get('triggered', 0)} R={dynamic_result.get('r_usdt', '')}"
                )
            except Exception as exc:
                print(f"⚠️ dynamic profit protection failed: {exc}")

            try:
                trailing_result = trailing_stop_tracker.run_round()
                print(
                    f"🟢 trailing stop tracker checked={trailing_result.get('checked', 0)} "
                    f"eligible={trailing_result.get('eligible', 0)} "
                    f"updated={trailing_result.get('updated', 0)}"
                )
            except Exception as exc:
                print(f"⚠️ trailing stop tracker failed: {exc}")

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

    last_pre_safety_round_ts = None
    last_scoring_started_round_ts = None
    active_scoring_process: multiprocessing.Process | None = None
    active_scoring_round_ts: int | None = None
    active_scoring_deadline_ts: int | None = None
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
                except Exception as exc:
                    print(f"⚠️ market filter failed round={round_ts}: {exc}")
                try:
                    add_permission_result = add_permission.run_round(decision_round_ts=round_ts, evaluated_at=now_ms)
                    print(
                        f"➕ add-position permission round={round_ts} allow={add_permission_result.allow_add_positions} "
                        f"allusdt_delta={add_permission_result.allusdt_delta} btc_delta={add_permission_result.btc_delta} "
                        f"reason={add_permission_result.reason}"
                    )
                except Exception as exc:
                    print(f"⚠️ add-position permission failed round={round_ts}: {exc}")
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
                    print(f"⚠️ cooldown failed round={round_ts}: {exc}")

            last_pre_safety_round_ts = round_ts

        if active_scoring_process is not None:
            if active_scoring_process.is_alive():
                if (
                    active_scoring_deadline_ts is not None
                    and now_ms >= active_scoring_deadline_ts
                ):
                    print(
                        f"⏱️ scoring round={active_scoring_round_ts} exceeded deadline "
                        f"deadline={active_scoring_deadline_ts}; terminating stale worker"
                    )
                    active_scoring_process.terminate()
                    active_scoring_process.join(timeout=5)
                    if active_scoring_process.is_alive():
                        print(
                            f"⏱️ scoring round={active_scoring_round_ts} did not terminate gracefully; killing worker"
                        )
                        active_scoring_process.kill()
                        active_scoring_process.join(timeout=5)
                    active_scoring_process = None
                    active_scoring_round_ts = None
                    active_scoring_deadline_ts = None
            else:
                active_scoring_process.join(timeout=0)
                print(
                    f"✅ scoring worker finished round={active_scoring_round_ts} "
                    f"exitcode={active_scoring_process.exitcode}"
                )
                active_scoring_process = None
                active_scoring_round_ts = None
                active_scoring_deadline_ts = None

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
                active_scoring_deadline_ts = round_ts + round_ms
                active_scoring_round_ts = round_ts
                active_scoring_process = multiprocessing.Process(
                    target=run_scoring_round_worker,
                    args=(
                        db_config.SCORING_DB_PATH,
                        round_ts,
                        list(symbols),
                        list(abnormal_symbols),
                        now_ms,
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
                print(f"⚠️ scoring worker start failed round={round_ts}: {exc}")
                active_scoring_process = None
                active_scoring_round_ts = None
                active_scoring_deadline_ts = None

        time.sleep(5)


def start_increase_pretrigger_refresh_task() -> None:
    """Refresh pre-triggered first-add symbols once per minute."""
    holding_scoring = HoldingPositionScoringSystem(db_path=db_config.TRADING_DB_PATH)
    holding_scoring.init_tables()
    print("🟣 Increase pre-trigger refresh task started")
    while True:
        try:
            result = holding_scoring.refresh_pretrigger_increase_checks()
            if result.get("refreshed", 0):
                print(
                    f"🟣 increase pretrigger refresh round={result.get('round_ts')} "
                    f"refreshed={result.get('refreshed', 0)} "
                    f"triggered={result.get('triggered', 0)} "
                    f"records={result.get('records', 0)}"
                )
        except Exception as exc:
            print(f"⚠️ increase pretrigger refresh failed: {exc}")
        time.sleep(60)


def on_ma20_result(result: MACalcResult) -> None:
    save_ma20_result(db_config.BASE_DB_PATH, result)
    save_ema_result(db_config.BASE_DB_PATH, result)
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
    """Persist MACD only after the interval's EMA/MA20 collection has completed."""
    if interval != "15m":
        return

    macd_saved = 0
    for result in results:
        if (
            result.macd_dif is None
            or result.macd_dea is None
            or result.macd_histogram is None
        ):
            continue
        save_macd_result(db_config.BASE_DB_PATH, result)
        macd_saved += 1
        print(
            f"📈 MACD {result.symbol} {result.interval} "
            f"open_time={result.open_time} close={result.close:.6f} "
            f"dif={result.macd_dif:.6f} dea={result.macd_dea:.6f} "
            f"macd={result.macd_histogram:.6f}"
        )

    print(f"📈 MACD 15m round complete, saved={macd_saved}")


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
        try:
            result = TrailingReductionTracker(db_path=db_config.TRADING_DB_PATH).run_round(decision_round_ts=int(time.time() * 1000))
            print(
                f"🧭 trailing reduction after ATR checked={result.get('checked', 0)} "
                f"eligible={result.get('eligible', 0)} pretriggered={result.get('pretriggered', 0)}"
            )
        except Exception as exc:
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
        try:
            result = tracker.refresh_pretriggered_symbols()
            print(
                f"🧭 trailing reduction refresh refreshed={result.get('refreshed', 0)} "
                f"triggered={result.get('triggered', 0)} records={result.get('records', 0)}"
            )
        except Exception as exc:
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

    # 主线程跑 processor task
    start_processor_task(symbols)
