"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
- pre-safety task: detect abnormal wick events every 15m decision round
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import List

import collector
from data_processor import (
    MA20Processor,
    MA20Scheduler,
    MACalcResult,
    init_ma20_table,
    run_loop,
    save_ma20_result,
)
from pre_safety_module import PreSafetyModule
from scoring_system import ScoringSystem

_universe_lock = threading.Lock()
_universe_refresh_interval_sec = 12 * 60 * 60
_universe_empty_retry_interval_sec = 5 * 60
_universe_last_refresh_ts = 0.0
_scoring_wait_log_interval_sec = int(os.getenv("SCORING_WAIT_LOG_INTERVAL_SEC", "60"))
_scoring_wait_timeout_sec = int(os.getenv("SCORING_WAIT_TIMEOUT_SEC", "600"))


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
            refreshed = collector.build_universe()
            collector.UNIVERSE = refreshed
            if refreshed:
                _universe_last_refresh_ts = now_ts
            else:
                _universe_last_refresh_ts = (
                    now_ts
                    - _universe_refresh_interval_sec
                    + _universe_empty_retry_interval_sec
                )
        return list(collector.UNIVERSE)


def start_pre_safety_task() -> None:
    """Run pre-safety abnormal wick detection in an isolated daemon thread.

    This task only reads existing 5m candle data and writes its own event table,
    so it will not interfere with collector/MA20 pipelines.
    """
    module = PreSafetyModule(db_path=collector.DB_PATH)
    module.init_table()
    scoring = ScoringSystem(db_path=collector.DB_PATH)
    scoring.init_table()

    last_round_ts = None
    round_ms = 15 * 60_000
    scoring_wait_started_by_round: dict[int, float] = {}
    scoring_wait_last_log_by_round: dict[int, float] = {}

    print("🛡️ Pre-safety task started")
    while True:
        symbols = ensure_universe()
        now_ms = int(time.time() * 1000)
        round_ts = (now_ms // round_ms) * round_ms

        if round_ts != last_round_ts:
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
                _, abnormal_symbols = module.get_latest_round_abnormal_symbols(decision_round_ts=round_ts)
                # MA20 由 klines_1m 聚合出的15m K线计算得到：如果1m基准数据本身不新，
                # 该轮15m MA20不可能就绪，先直接报告1m数据新鲜度，避免误导为MA20问题。
                freshness = scoring.get_1m_kline_freshness_for_round(
                    decision_round_ts=round_ts,
                    symbols=symbols,
                )
                if not freshness["fresh"]:
                    now_ts = time.time()
                    wait_started_ts = scoring_wait_started_by_round.setdefault(round_ts, now_ts)
                    last_log_ts = scoring_wait_last_log_by_round.get(round_ts, 0.0)
                    waited_sec = int(now_ts - wait_started_ts)
                    should_log_wait = (now_ts - last_log_ts) >= _scoring_wait_log_interval_sec
                    if should_log_wait:
                        scoring_wait_last_log_by_round[round_ts] = now_ts
                        print(
                            f"⏳ scoring round={round_ts} waiting klines_1m freshness "
                            f"target_close_time={freshness['target_close_time']} "
                            f"fresh={freshness['fresh_count']}/{freshness['expected_count']} "
                            f"stale={freshness['stale_count']} "
                            f"sample={freshness['stale_sample']} waited={waited_sec}s"
                        )

                    if waited_sec >= _scoring_wait_timeout_sec:
                        print(
                            f"⚠️ scoring round={round_ts} skipped after waiting {waited_sec}s for klines_1m "
                            f"target_close_time={freshness['target_close_time']} "
                            f"fresh={freshness['fresh_count']}/{freshness['expected_count']} "
                            f"stale={freshness['stale_count']} sample={freshness['stale_sample']}"
                        )
                        last_round_ts = round_ts
                        scoring_wait_started_by_round.pop(round_ts, None)
                        scoring_wait_last_log_by_round.pop(round_ts, None)
                    time.sleep(5)
                    continue

                # 评分严格依赖15m MA20：先等待该轮对应已收盘15m K线的MA20写入完成，再打分。
                # 当前轮次 round_ts 对应的最新已收盘15m K线 open_time=round_ts-15m。
                readiness = scoring.get_15m_ma20_readiness_for_round(
                    decision_round_ts=round_ts,
                    symbols=symbols,
                )
                if not readiness["ready"]:
                    now_ts = time.time()
                    wait_started_ts = scoring_wait_started_by_round.setdefault(round_ts, now_ts)
                    last_log_ts = scoring_wait_last_log_by_round.get(round_ts, 0.0)
                    waited_sec = int(now_ts - wait_started_ts)
                    should_log_wait = (now_ts - last_log_ts) >= _scoring_wait_log_interval_sec
                    if should_log_wait:
                        scoring_wait_last_log_by_round[round_ts] = now_ts
                        print(
                            f"⏳ scoring round={round_ts} waiting MA20 readiness "
                            f"target_open_time={readiness['target_open_time']} "
                            f"ready={readiness['ready_count']}/{readiness['expected_count']} "
                            f"missing={readiness['missing_count']} "
                            f"sample={readiness['missing_sample']} waited={waited_sec}s"
                        )

                    if waited_sec >= _scoring_wait_timeout_sec:
                        print(
                            f"⚠️ scoring round={round_ts} skipped after waiting {waited_sec}s for MA20 "
                            f"target_open_time={readiness['target_open_time']} "
                            f"ready={readiness['ready_count']}/{readiness['expected_count']} "
                            f"missing={readiness['missing_count']} sample={readiness['missing_sample']}"
                        )
                        last_round_ts = round_ts
                        scoring_wait_started_by_round.pop(round_ts, None)
                        scoring_wait_last_log_by_round.pop(round_ts, None)
                    time.sleep(5)
                    continue
                scored = scoring.score_round(
                    decision_round_ts=round_ts,
                    all_symbols=symbols,
                    abnormal_symbols=abnormal_symbols,
                )
                print(
                    f"🧮 scoring round={round_ts} universe={len(symbols)} "
                    f"abnormal={len(set(abnormal_symbols))} scored={len(scored)}"
                )
                scoring_wait_started_by_round.pop(round_ts, None)
                scoring_wait_last_log_by_round.pop(round_ts, None)
            except Exception as exc:
                print(f"⚠️ scoring failed round={round_ts}: {exc}")

            last_round_ts = round_ts

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
    job_options = {"coalesce": True, "misfire_grace_time": 30, "max_instances": 2}
    scheduler.add_job(ensure_universe, "interval", hours=12, coalesce=True, max_instances=1)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.kline_job), "cron", second=0, **job_options)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.oi_job), "cron", second=20, **job_options)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.funding_job), "cron", minute=1, second=40, **job_options)
    scheduler.add_job(lambda: _run_with_fresh_universe(collector.btc_5m_job), "cron", minute="*/5", second=10, **job_options)

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

    # 三个独立 task：collector / pre_safety / data_processor
    collector_thread = threading.Thread(target=start_collector_task, args=(symbols,), daemon=True)
    collector_thread.start()

    pre_safety_thread = threading.Thread(target=start_pre_safety_task, daemon=True)
    pre_safety_thread.start()

    # 主线程跑 processor task
    start_processor_task(symbols)
