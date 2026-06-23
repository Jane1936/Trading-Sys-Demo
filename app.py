"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
- pre-safety task: detect abnormal wick events and cooldown symbols every 15m decision round
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Iterable, List

import collector
from data_processor import (
    MA20Processor,
    MA20Scheduler,
    MACalcResult,
    init_ma20_table,
    run_loop,
    save_ma20_result,
)
from break_even_take_profit import BreakEvenTakeProfitStrategy
from cooldown_module import CooldownModule
from openable_symbol_module import OpenableSymbol, OpenableSymbolModule
from pre_safety_module import PreSafetyModule
from partial_take_profit import PartialTakeProfitStrategy
from trailing_stop_tracker import TrailingStopTracker
from holding_position_scoring import HoldingPositionScoringSystem
from scoring_system import ScoringSystem
from trading_experiment import TradingExperiment

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
        should_refresh = collector.UNIVERSE is None or (now_ts - _universe_last_refresh_ts) >= _universe_refresh_interval_sec
        if should_refresh:
            collector.UNIVERSE = collector.build_universe()
            _universe_last_refresh_ts = now_ts
        return list(collector.UNIVERSE)


def run_first_experiment_after_openable_round(openable_symbols: Iterable[OpenableSymbol], round_ts: int) -> None:
    """Run the first experiment only after openable-symbol evaluation is complete."""
    openable_rows = list(openable_symbols)
    qualified_openable_count = sum(1 for row in openable_rows if row.qualified)
    if qualified_openable_count <= 0:
        print(f"🧪 first trading experiment round={round_ts} skipped after openable round: no qualified symbols")
        return

    try:
        experiment_result = TradingExperiment(db_path=collector.DB_PATH).run_round(openable_rows)
        print(
            f"🧪 first trading experiment after openable round={round_ts} "
            f"opened={experiment_result.get('opened', 0)} "
            f"skipped={experiment_result.get('skipped', 0)} "
            f"reason={experiment_result.get('reason', '')}"
        )
    except Exception as exc:
        print(f"⚠️ first trading experiment failed after openable round={round_ts}: {exc}")


def start_break_even_take_profit_task() -> None:
    """Run break-even and partial take-profit protection every 5 minutes."""
    strategy = BreakEvenTakeProfitStrategy(db_path=collector.DB_PATH)
    partial_strategy = PartialTakeProfitStrategy(db_path=collector.DB_PATH)
    trailing_stop_tracker = TrailingStopTracker(db_path=collector.DB_PATH)
    strategy.init_tables()
    partial_strategy.init_tables()
    trailing_stop_tracker.init_tables()
    print("🟢 Break-even, partial take-profit and trailing stop tracker task started")
    while True:
        try:
            result = strategy.run_round()
            print(
                f"🟢 break-even take-profit checked={result.get('checked', 0)} "
                f"triggered={result.get('triggered', 0)} "
                f"records={result.get('records', 0)} R={result.get('r_usdt', '')}"
            )
            partial_result = partial_strategy.run_round()
            print(
                f"🟢 partial take-profit checked={partial_result.get('checked', 0)} "
                f"triggered={partial_result.get('triggered', 0)} "
                f"records={partial_result.get('records', 0)} 2R={partial_result.get('trigger_r_usdt', '')}"
            )
            for _ in range(5):
                trailing_result = trailing_stop_tracker.run_round()
                print(
                    f"🟢 trailing stop tracker checked={trailing_result.get('checked', 0)} "
                    f"eligible={trailing_result.get('eligible', 0)} "
                    f"updated={trailing_result.get('updated', 0)}"
                )
                time.sleep(60)
            continue
        except Exception as exc:
            print(f"⚠️ break-even take-profit failed: {exc}")
        time.sleep(5 * 60)


def start_pre_safety_task() -> None:
    """Run pre-safety abnormal wick detection in an isolated daemon thread.

    This task only reads existing 5m candle data and writes its own event table,
    so it will not interfere with collector/MA20 pipelines.
    """
    module = PreSafetyModule(db_path=collector.DB_PATH)
    module.init_table()
    cooldown = CooldownModule(db_path=collector.DB_PATH)
    cooldown.init_table()
    scoring = ScoringSystem(db_path=collector.DB_PATH)
    scoring.init_table()
    openable = OpenableSymbolModule(db_path=collector.DB_PATH)
    openable.init_table()
    holding_scoring = HoldingPositionScoringSystem(db_path=collector.DB_PATH)
    holding_scoring.init_tables()

    last_pre_safety_round_ts = None
    last_scoring_round_ts = None
    round_ms = 15 * 60_000

    print("🛡️ Pre-safety task started")
    while True:
        symbols = ensure_universe()
        now_ms = int(time.time() * 1000)
        round_ts = (now_ms // round_ms) * round_ms

        scoring_execute_ts = round_ts + 30_000

        if round_ts != last_pre_safety_round_ts:
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

            try:
                cooldown_symbols = cooldown.run_round(symbols=symbols, decision_round_ts=round_ts, now_ms=now_ms)
                print(
                    f"🧊 cooldown round={round_ts} universe={len(symbols)} "
                    f"cooldown={len(cooldown_symbols)}"
                )
            except Exception as exc:
                print(f"⚠️ cooldown failed round={round_ts}: {exc}")

            last_pre_safety_round_ts = round_ts

        if round_ts != last_scoring_round_ts and now_ms >= scoring_execute_ts:
            try:
                _, abnormal_symbols = module.get_latest_round_abnormal_symbols(decision_round_ts=round_ts)
                # 评分系统固定在整15分钟后第30秒执行（如 15:30:30）。
                # 当前轮次 round_ts 对应的最新已收盘15m K线 open_time=round_ts-15m；
                # 若 MA20 写入与评分任务存在轻微竞态，最多等到整15分钟后约第60秒；
                # 仍缺少15m MA20的 symbol 才跳过，避免在 :30.5 附近误跳过已即将入库的数据。
                readiness = scoring.wait_for_15m_ma20_readiness_for_round(
                    decision_round_ts=round_ts,
                    symbols=symbols,
                    retries=6,
                    retry_delay_seconds=5.0,
                )
                if not readiness.ready:
                    scoring.record_ma20_skip_for_round(
                        decision_round_ts=round_ts,
                        readiness=readiness,
                        universe_count=len(symbols),
                        created_at=int(time.time() * 1000),
                    )
                    missing_preview = ",".join(readiness.missing_symbols[:10])
                    if len(readiness.missing_symbols) > 10:
                        missing_preview += ",..."
                    print(
                        f"⚠️ scoring round={round_ts} skipping symbols missing 15m MA20 "
                        f"target_open_time={readiness.target_open_time} "
                        f"ready={len(readiness.ready_symbols)} "
                        f"missing={len(readiness.missing_symbols)} "
                        f"missing_symbols={missing_preview}"
                    )
                scored = scoring.score_round(
                    decision_round_ts=round_ts,
                    all_symbols=readiness.ready_symbols,
                    abnormal_symbols=abnormal_symbols,
                )
                print(
                    f"🧮 scoring round={round_ts} universe={len(symbols)} "
                    f"ready={len(readiness.ready_symbols)} "
                    f"abnormal={len(set(abnormal_symbols))} scored={len(scored)}"
                )
                # 持仓评分系统只依赖本轮总分评分完成后生成的结构止损位，
                # 不依赖“本轮可开仓symbol情况”的评估结果。
                try:
                    holding_result = holding_scoring.run_round(decision_round_ts=round_ts)
                    print(
                        f"📊 holding scoring round={round_ts} "
                        f"checked={holding_result.get('checked', 0)} "
                        f"triggered={holding_result.get('triggered', 0)} "
                        f"records={holding_result.get('records', 0)}"
                    )
                except Exception as exc:
                    print(f"⚠️ holding scoring failed round={round_ts}: {exc}")

                openable_symbols = openable.run_round(decision_round_ts=round_ts, evaluated_at=now_ms)
                qualified_openable_count = sum(1 for row in openable_symbols if row.qualified)
                print(
                    f"🚪 openable round={round_ts} candidates={len(openable_symbols)} "
                    f"qualified={qualified_openable_count}"
                )

                # 第一组实验必须在“本轮可开仓symbol情况”完成计算之后执行，
                # 这样才能使用该模块计算出的最终可开仓结果和杠杆大小。
                run_first_experiment_after_openable_round(openable_symbols, round_ts)
            except Exception as exc:
                print(f"⚠️ scoring failed round={round_ts}: {exc}")

            last_scoring_round_ts = round_ts

        time.sleep(5)


def on_ma20_result(result: MACalcResult) -> None:
    save_ma20_result(collector.DB_PATH, result)
    print(
        f"📈 MA20 {result.symbol} {result.interval} "
        f"open_time={result.open_time} close={result.close:.6f} ma20={result.ma20:.6f}"
    )


def start_collector_task(symbols: List[str]) -> None:
    collector.init_db()
    collector.UNIVERSE = list(symbols)

    def _run_with_fresh_universe(job_func):
        ensure_universe()
        job_func()

    scheduler = collector.BlockingScheduler()
    scheduler.add_job(ensure_universe, "interval", hours=12)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.kline_job), "cron", second=0)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.oi_job), "cron", second=20)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.funding_job), "cron", minute=1, second=40)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.btc_5m_job), "cron", minute="*/5", second=10)

    print("🚀 Collector task started")
    scheduler.start()


def start_processor_task(symbols: List[str]) -> None:
    init_ma20_table(db_path=collector.DB_PATH)
    processor = MA20Processor(db_path=collector.DB_PATH)
    scheduler = MA20Scheduler(grace_seconds=5)

    print(f"🚀 MA20 processor task started, symbols={len(symbols)}")
    run_loop(
        symbols=symbols,
        processor=processor,
        scheduler=scheduler,
        on_result=on_ma20_result,
        symbol_provider=ensure_universe,
        poll_seconds=20,
    )


if __name__ == "__main__":
    verify_db_writable(collector.DB_PATH)
    # 预先构建一次 universe，并按12小时周期刷新
    symbols = ensure_universe()

    # 四个独立 task：collector / pre_safety（异常插针后立即执行冷却期symbol） / break_even_take_profit / data_processor
    collector_thread = threading.Thread(target=start_collector_task, args=(symbols,), daemon=True)
    collector_thread.start()

    pre_safety_thread = threading.Thread(target=start_pre_safety_task, daemon=True)
    pre_safety_thread.start()

    break_even_thread = threading.Thread(target=start_break_even_take_profit_task, daemon=True)
    break_even_thread.start()

    # 主线程跑 processor task
    start_processor_task(symbols)
