"""Symbol scoring system executed every 15-minute decision round."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Iterable, List


@dataclass
class SymbolScore:
    symbol: str
    decision_round_ts: int
    score: int
    reason: str
    ma20_latest: float
    ma20_prev1: float
    ma20_prev2: float
    updated_at: int


@dataclass
class SymbolTotalScore:
    symbol: str
    decision_round_ts: int
    rule1_score: int
    rule2_score: int
    rule3_score: int
    rule4_score: int
    rule5_score: int
    rule6_score: int
    rule7_score: int
    rule8_score: int
    rule9_score: int
    rule10_score: int
    rule11_score: int
    rule12_score: int
    rule13_score: int
    total_score: int


class ScoringSystem:
    """Score symbols after pre-safety using latest 3 rows of 15m MA20."""

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    ma20_latest REAL NOT NULL,
                    ma20_prev1 REAL NOT NULL,
                    ma20_prev2 REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_round ON symbol_scores(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_close_gt_ma20 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_1m_close REAL NOT NULL,
                    latest_15m_ma20 REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_close_gt_ma20_round ON symbol_scores_close_gt_ma20(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_1m_close_gt_5m_ma20 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_1m_close REAL NOT NULL,
                    latest_5m_ma20 REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_1m_close_gt_5m_ma20_round ON symbol_scores_1m_close_gt_5m_ma20(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_1h_close_gt_prev (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_1h_close REAL NOT NULL,
                    prev_1h_close REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_1h_close_gt_prev_round ON symbol_scores_1h_close_gt_prev(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_close_increasing_3of4 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    increasing_pairs_count INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_close_increasing_3of4_round ON symbol_scores_15m_close_increasing_3of4(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_bullish_3of4 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    bullish_count INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_bullish_3of4_round ON symbol_scores_15m_bullish_3of4(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_close_near_high_2of4 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    qualified_count INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_close_near_high_2of4_round ON symbol_scores_15m_close_near_high_2of4(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_1h_latest_highest_24 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_high REAL NOT NULL,
                    prev_23_max_high REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_1h_latest_highest_24_round ON symbol_scores_1h_latest_highest_24(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_close_desc_3_with_oi_45m (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    close_latest REAL NOT NULL,
                    close_prev1 REAL NOT NULL,
                    close_prev2 REAL NOT NULL,
                    latest_open_interest REAL NOT NULL,
                    open_interest_45m_ago REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_close_desc_3_with_oi_45m_round ON symbol_scores_15m_close_desc_3_with_oi_45m(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_1m_close_gt_60m_open_with_oi_60m (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_1m_close REAL NOT NULL,
                    open_60m_ago REAL NOT NULL,
                    latest_open_interest REAL NOT NULL,
                    open_interest_60m_ago REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_1m_close_gt_60m_open_with_oi_60m_round ON symbol_scores_1m_close_gt_60m_open_with_oi_60m(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_oi_loss_rate_240m (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_open_interest REAL NOT NULL,
                    open_interest_240m_ago REAL NOT NULL,
                    oi_loss_rate REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_oi_loss_rate_240m_round ON symbol_scores_oi_loss_rate_240m(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_funding_rate_4bars (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    funding_rate_latest REAL,
                    funding_rate_prev1 REAL,
                    funding_rate_prev2 REAL,
                    funding_rate_prev3 REAL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_funding_rate_4bars_round ON symbol_scores_15m_funding_rate_4bars(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_bullish_volume_breakout (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_open REAL NOT NULL,
                    latest_close REAL NOT NULL,
                    prev_close REAL NOT NULL,
                    latest_volume REAL NOT NULL,
                    volume_avg REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_bullish_volume_breakout_round ON symbol_scores_15m_bullish_volume_breakout(decision_round_ts DESC)"
            )

    def _latest_three_ma20_15m(self, symbol: str) -> tuple[float, float, float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ma20
                FROM ma20_indicators
                WHERE symbol = ? AND interval = '15m'
                ORDER BY open_time DESC
                LIMIT 3
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 3:
            return None
        return float(rows[0]["ma20"]), float(rows[1]["ma20"]), float(rows[2]["ma20"])

    def score_round(
        self,
        decision_round_ts: int,
        all_symbols: Iterable[str],
        abnormal_symbols: Iterable[str],
    ) -> List[SymbolScore]:
        abnormal_set = set(abnormal_symbols)
        candidates = sorted(set(all_symbols) - abnormal_set)
        now_ms = int(time.time() * 1000)
        results: List[SymbolScore] = []
        for symbol in candidates:
            self._save_close_gt_ma20_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_1h_close_gt_prev_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_bullish_3of4_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_close_increasing_3of4_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_1m_close_gt_5m_ma20_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_close_near_high_2of4_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_1h_latest_highest_24_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_close_desc_3_with_oi_45m_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_1m_close_gt_60m_open_with_oi_60m_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_oi_loss_rate_240m_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_funding_rate_4bars_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            self._save_15m_bullish_volume_breakout_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
            ma20s = self._latest_three_ma20_15m(symbol)
            if ma20s is None:
                continue
            m1, m2, m3 = ma20s
            hit = m1 > m2 > m3
            score = 4 if hit else 0
            reason = "ma20_15m_desc_3bars" if hit else "ma20_15m_rule_not_met"
            rec = SymbolScore(symbol, decision_round_ts, score, reason, m1, m2, m3, now_ms)
            results.append(rec)
            self._save_score(rec)
        return results

    def is_15m_ma20_ready_for_round(self, decision_round_ts: int, symbols: Iterable[str]) -> bool:
        target_open_time = decision_round_ts - 15 * 60_000
        symbol_list = list(set(symbols))
        if not symbol_list:
            return True
        placeholders = ",".join(["?"] * len(symbol_list))
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(DISTINCT symbol) AS cnt
                FROM ma20_indicators
                WHERE interval = '15m' AND open_time = ? AND symbol IN ({placeholders})
                """,
                [target_open_time, *symbol_list],
            ).fetchone()
        return int(row["cnt"]) == len(symbol_list)

    def _latest_1m_close_and_15m_ma20(self, symbol: str) -> tuple[float, float] | None:
        with self._connect() as conn:
            close_row = conn.execute(
                """
                SELECT close
                FROM klines_1m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            ma20_row = conn.execute(
                """
                SELECT ma20
                FROM ma20_indicators
                WHERE symbol = ? AND interval = '15m'
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
        if not close_row or not ma20_row:
            return None
        return float(close_row["close"]), float(ma20_row["ma20"])

    def _save_close_gt_ma20_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        values = self._latest_1m_close_and_15m_ma20(symbol)
        if values is None:
            return
        close_1m, ma20_15m = values
        hit = close_1m > ma20_15m
        score = 6 if hit else 0
        reason = "close_1m_gt_15m_ma20" if hit else "close_1m_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_close_gt_ma20
                (symbol, decision_round_ts, score, reason, latest_1m_close, latest_15m_ma20, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_1m_close=excluded.latest_1m_close,
                    latest_15m_ma20=excluded.latest_15m_ma20,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, close_1m, ma20_15m, updated_at),
            )


    def _latest_1m_close_and_5m_ma20(self, symbol: str) -> tuple[float, float] | None:
        with self._connect() as conn:
            close_row = conn.execute(
                """
                SELECT close
                FROM klines_1m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            ma20_row = conn.execute(
                """
                SELECT ma20
                FROM ma20_indicators
                WHERE symbol = ? AND interval = '5m'
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
        if not close_row or not ma20_row:
            return None
        return float(close_row["close"]), float(ma20_row["ma20"])

    def _save_1m_close_gt_5m_ma20_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        values = self._latest_1m_close_and_5m_ma20(symbol)
        if values is None:
            return
        close_1m, ma20_5m = values
        hit = close_1m > ma20_5m
        score = 6 if hit else 0
        reason = "close_1m_gt_5m_ma20" if hit else "close_1m_gt_5m_ma20_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_1m_close_gt_5m_ma20
                (symbol, decision_round_ts, score, reason, latest_1m_close, latest_5m_ma20, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_1m_close=excluded.latest_1m_close,
                    latest_5m_ma20=excluded.latest_5m_ma20,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, close_1m, ma20_5m, updated_at),
            )

    def _latest_two_1h_close(self, symbol: str) -> tuple[float, float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT close
                FROM klines_1h
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 2
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 2:
            return None
        return float(rows[0]["close"]), float(rows[1]["close"])

    def _save_1h_close_gt_prev_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        values = self._latest_two_1h_close(symbol)
        if values is None:
            return
        latest_close_1h, prev_close_1h = values
        hit = latest_close_1h > prev_close_1h
        score = 6 if hit else 0
        reason = "close_1h_gt_prev_1h" if hit else "close_1h_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_1h_close_gt_prev
                (symbol, decision_round_ts, score, reason, latest_1h_close, prev_1h_close, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_1h_close=excluded.latest_1h_close,
                    prev_1h_close=excluded.prev_1h_close,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, latest_close_1h, prev_close_1h, updated_at),
            )

    def _latest_four_15m_open_close(self, symbol: str) -> list[tuple[float, float]] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open, close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 4
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 4:
            return None
        return [(float(r["open"]), float(r["close"])) for r in rows]

    def _latest_four_15m_close(self, symbol: str) -> list[float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 4
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 4:
            return None
        # oldest -> latest: c4, c3, c2, c1
        return [float(r["close"]) for r in rows[::-1]]

    def _save_15m_bullish_3of4_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_four_15m_open_close(symbol)
        if rows is None:
            return
        bullish_count = sum(1 for open_price, close_price in rows if close_price > open_price)
        hit = bullish_count >= 3
        score = 6 if hit else 0
        reason = "kline_15m_bullish_3of4" if hit else "kline_15m_bullish_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_bullish_3of4
                (symbol, decision_round_ts, score, reason, bullish_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    bullish_count=excluded.bullish_count,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, bullish_count, updated_at),
            )

    def _save_15m_close_increasing_3of4_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        closes = self._latest_four_15m_close(symbol)
        if closes is None:
            return
        # 历史字段 increasing_pairs_count 仅为兼容旧表结构保留，不再参与规则判定
        increasing_pairs_count = 0

        increasing_triplet_exists = False
        for i in range(len(closes)):
            for j in range(i + 1, len(closes)):
                for k in range(j + 1, len(closes)):
                    if closes[i] < closes[j] < closes[k]:
                        increasing_triplet_exists = True
                        break
                if increasing_triplet_exists:
                    break
            if increasing_triplet_exists:
                break

        # 4选3：至少存在一个三元组满足按时间顺序递增（不要求连续）
        hit = increasing_triplet_exists
        score = 4 if hit else 0
        reason = "close_15m_increasing_3of4" if hit else "close_15m_increasing_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_close_increasing_3of4
                (symbol, decision_round_ts, score, reason, increasing_pairs_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    increasing_pairs_count=excluded.increasing_pairs_count,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, increasing_pairs_count, updated_at),
            )

    def _latest_four_15m_ohlc(self, symbol: str) -> list[tuple[float, float, float, float]] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open, high, low, close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 4
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 4:
            return None
        return [(float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])) for r in rows]

    def _save_15m_close_near_high_2of4_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_four_15m_ohlc(symbol)
        if rows is None:
            return
        qualified_count = 0
        for _, high, low, close in rows:
            if high == low:
                continue
            if (close - low) / (high - low) >= 0.55:
                qualified_count += 1
        hit = qualified_count >= 2
        score = 3 if hit else 0
        reason = "close_pos_15m_ge_0.55_2of4" if hit else "close_pos_15m_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_close_near_high_2of4
                (symbol, decision_round_ts, score, reason, qualified_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    qualified_count=excluded.qualified_count,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, qualified_count, updated_at),
            )

    def _latest_24_1h_high(self, symbol: str) -> list[float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT high
                FROM klines_1h
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 24
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 24:
            return None
        return [float(r["high"]) for r in rows]

    def _save_1h_latest_highest_24_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        highs = self._latest_24_1h_high(symbol)
        if highs is None:
            return
        latest_high = highs[0]
        prev_23_max_high = max(highs[1:])
        hit = latest_high > prev_23_max_high
        score = 5 if hit else 0
        reason = "latest_1h_high_gt_prev_23_high" if hit else "latest_1h_high_rule_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_1h_latest_highest_24
                (symbol, decision_round_ts, score, reason, latest_high, prev_23_max_high, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_high=excluded.latest_high,
                    prev_23_max_high=excluded.prev_23_max_high,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, latest_high, prev_23_max_high, updated_at),
            )

    def get_latest_round_scores_15m_close_near_high_2of4(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_close_near_high_2of4").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, qualified_count, updated_at FROM symbol_scores_15m_close_near_high_2of4 WHERE decision_round_ts = ? ORDER BY score DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_close_near_high_2of4(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, qualified_count, updated_at FROM symbol_scores_15m_close_near_high_2of4 WHERE decision_round_ts = ? ORDER BY symbol ASC",
                (round_ts,),
            ).fetchall()

    def get_latest_round_scores_1h_latest_highest_24(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_1h_latest_highest_24").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, latest_high, prev_23_max_high AS prev_24_max_high, updated_at FROM symbol_scores_1h_latest_highest_24 WHERE decision_round_ts = ? ORDER BY score DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _latest_three_15m_close_and_oi_45m(self, symbol: str) -> tuple[float, float, float, float, float] | None:
        with self._connect() as conn:
            close_rows = conn.execute(
                """
                SELECT close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 3
                """,
                (symbol,),
            ).fetchall()
            oi_rows = conn.execute(
                """
                SELECT open_interest
                FROM open_interest_1m
                WHERE symbol = ?
                ORDER BY snapshot_time DESC
                LIMIT 46
                """,
                (symbol,),
            ).fetchall()
        if len(close_rows) < 3 or len(oi_rows) < 46:
            return None
        close_latest = float(close_rows[0]["close"])
        close_prev1 = float(close_rows[1]["close"])
        close_prev2 = float(close_rows[2]["close"])
        latest_oi = float(oi_rows[0]["open_interest"])
        oi_45m_ago = float(oi_rows[45]["open_interest"])
        return close_latest, close_prev1, close_prev2, latest_oi, oi_45m_ago

    def _save_15m_close_desc_3_with_oi_45m_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        values = self._latest_three_15m_close_and_oi_45m(symbol)
        if values is None:
            return
        close_latest, close_prev1, close_prev2, latest_oi, oi_45m_ago = values
        hit = (close_latest > close_prev1 > close_prev2) and (latest_oi > oi_45m_ago)
        score = 10 if hit else 0
        reason = "close_15m_desc_3_and_oi_1m_gt_45m" if hit else "rule9_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_close_desc_3_with_oi_45m
                (symbol, decision_round_ts, score, reason, close_latest, close_prev1, close_prev2, latest_open_interest, open_interest_45m_ago, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    close_latest=excluded.close_latest,
                    close_prev1=excluded.close_prev1,
                    close_prev2=excluded.close_prev2,
                    latest_open_interest=excluded.latest_open_interest,
                    open_interest_45m_ago=excluded.open_interest_45m_ago,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, close_latest, close_prev1, close_prev2, latest_oi, oi_45m_ago, updated_at),
            )

    def get_latest_round_scores_15m_close_desc_3_with_oi_45m(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_close_desc_3_with_oi_45m").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, close_latest, close_prev1, close_prev2, latest_open_interest, open_interest_45m_ago, updated_at
                FROM symbol_scores_15m_close_desc_3_with_oi_45m
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_close_desc_3_with_oi_45m(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, close_latest, close_prev1, close_prev2, latest_open_interest, open_interest_45m_ago, updated_at
                FROM symbol_scores_15m_close_desc_3_with_oi_45m
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()



    def _save_1m_close_gt_60m_open_with_oi_60m_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        with self._connect() as conn:
            k_rows = conn.execute("""
                SELECT open, close FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT 60
            """, (symbol,)).fetchall()
            oi_rows = conn.execute("""
                SELECT open_interest FROM open_interest_1m WHERE symbol = ? ORDER BY snapshot_time DESC LIMIT 60
            """, (symbol,)).fetchall()
        if len(k_rows) < 60 or len(oi_rows) < 60:
            return
        latest_close = float(k_rows[0]["close"])
        open_60m_ago = float(k_rows[59]["open"])
        latest_oi = float(oi_rows[0]["open_interest"])
        oi_60m_ago = float(oi_rows[59]["open_interest"])
        hit = (latest_close > open_60m_ago) and (latest_oi > oi_60m_ago)
        score = 10 if hit else 0
        reason = "close_1m_gt_60m_open_and_oi_gt_60m" if hit else "rule10_not_met"
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO symbol_scores_1m_close_gt_60m_open_with_oi_60m
                (symbol, decision_round_ts, score, reason, latest_1m_close, open_60m_ago, latest_open_interest, open_interest_60m_ago, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score, reason=excluded.reason, latest_1m_close=excluded.latest_1m_close,
                    open_60m_ago=excluded.open_60m_ago, latest_open_interest=excluded.latest_open_interest,
                    open_interest_60m_ago=excluded.open_interest_60m_ago, updated_at=excluded.updated_at
            """, (symbol, decision_round_ts, score, reason, latest_close, open_60m_ago, latest_oi, oi_60m_ago, updated_at))

    def _save_oi_loss_rate_240m_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        with self._connect() as conn:
            oi_rows = conn.execute("""
                SELECT open_interest FROM open_interest_1m WHERE symbol = ? ORDER BY snapshot_time DESC LIMIT 240
            """, (symbol,)).fetchall()
        if len(oi_rows) < 240:
            return
        latest_oi = float(oi_rows[0]["open_interest"])
        oi_240m_ago = float(oi_rows[239]["open_interest"])
        if oi_240m_ago <= 0:
            loss_rate = 1.0
            hit = False
        elif latest_oi >= oi_240m_ago:
            loss_rate = 0.0
            hit = True
        else:
            loss_rate = (oi_240m_ago - latest_oi) / oi_240m_ago
            hit = loss_rate <= 0.03
        score = 5 if hit else 0
        reason = "oi_1m_gte_240m_or_loss_le_3pct" if hit else "rule11_not_met"
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO symbol_scores_oi_loss_rate_240m
                (symbol, decision_round_ts, score, reason, latest_open_interest, open_interest_240m_ago, oi_loss_rate, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score, reason=excluded.reason, latest_open_interest=excluded.latest_open_interest,
                    open_interest_240m_ago=excluded.open_interest_240m_ago, oi_loss_rate=excluded.oi_loss_rate, updated_at=excluded.updated_at
            """, (symbol, decision_round_ts, score, reason, latest_oi, oi_240m_ago, loss_rate, updated_at))


    def _latest_four_15m_funding_rates(self, symbol: str) -> tuple[float | None, float | None, float | None, float | None] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT funding_rate
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 4
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 4:
            return None
        return tuple(
            None if row["funding_rate"] is None else float(row["funding_rate"])
            for row in rows
        )

    def _save_15m_funding_rate_4bars_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        funding_rates = self._latest_four_15m_funding_rates(symbol)
        if funding_rates is None:
            return
        hit = all(rate is not None and 0.0001 < rate < 0.001 for rate in funding_rates)
        score = 5 if hit else 0
        reason = "funding_rate_15m_4bars_between_0.01_and_0.1" if hit else "rule12_not_met"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_funding_rate_4bars
                (symbol, decision_round_ts, score, reason, funding_rate_latest, funding_rate_prev1, funding_rate_prev2, funding_rate_prev3, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    funding_rate_latest=excluded.funding_rate_latest,
                    funding_rate_prev1=excluded.funding_rate_prev1,
                    funding_rate_prev2=excluded.funding_rate_prev2,
                    funding_rate_prev3=excluded.funding_rate_prev3,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, *funding_rates, updated_at),
            )


    def _latest_17_15m_ohlcv(self, symbol: str) -> list[sqlite3.Row] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open, close, volume
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 17
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 17:
            return None
        return rows

    def _save_15m_bullish_volume_breakout_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_17_15m_ohlcv(symbol)
        if rows is None:
            return
        latest_open = float(rows[0]["open"])
        latest_close = float(rows[0]["close"])
        prev_close = float(rows[1]["close"])
        latest_volume = float(rows[0]["volume"])
        volume_avg = sum(float(row["volume"]) for row in rows[1:17]) / 16

        price_hit = latest_close > latest_open and latest_close > prev_close
        volume_hit = price_hit and latest_volume > 1.8 * volume_avg
        score = 10 if volume_hit else 0
        if volume_hit:
            reason = "latest_15m_bullish_close_up_and_volume_gt_1_8_avg"
        elif price_hit:
            reason = "rule13_volume_not_met"
        else:
            reason = "rule13_price_not_met"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_bullish_volume_breakout
                (symbol, decision_round_ts, score, reason, latest_open, latest_close, prev_close, latest_volume, volume_avg, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_open=excluded.latest_open,
                    latest_close=excluded.latest_close,
                    prev_close=excluded.prev_close,
                    latest_volume=excluded.latest_volume,
                    volume_avg=excluded.volume_avg,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, latest_open, latest_close, prev_close, latest_volume, volume_avg, updated_at),
            )

    def get_latest_round_scores_15m_bullish_volume_breakout(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_bullish_volume_breakout").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_open, latest_close, prev_close, latest_volume, volume_avg, updated_at
                FROM symbol_scores_15m_bullish_volume_breakout
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_bullish_volume_breakout(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_open, latest_close, prev_close, latest_volume, volume_avg, updated_at
                FROM symbol_scores_15m_bullish_volume_breakout
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def get_latest_round_scores_15m_funding_rate_4bars(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_funding_rate_4bars").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, funding_rate_latest, funding_rate_prev1, funding_rate_prev2, funding_rate_prev3, updated_at
                FROM symbol_scores_15m_funding_rate_4bars
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_funding_rate_4bars(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, funding_rate_latest, funding_rate_prev1, funding_rate_prev2, funding_rate_prev3, updated_at
                FROM symbol_scores_15m_funding_rate_4bars
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def get_latest_round_scores_1m_close_gt_60m_open_with_oi_60m(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_1m_close_gt_60m_open_with_oi_60m").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute("SELECT symbol, decision_round_ts, score, reason, latest_1m_close, open_60m_ago, latest_open_interest, open_interest_60m_ago, updated_at FROM symbol_scores_1m_close_gt_60m_open_with_oi_60m WHERE decision_round_ts = ? ORDER BY score DESC, symbol ASC", (round_ts,)).fetchall()
        return round_ts, rows

    def _get_round_scores_1m_close_gt_60m_open_with_oi_60m(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute("SELECT symbol, decision_round_ts, score, reason, latest_1m_close, open_60m_ago, latest_open_interest, open_interest_60m_ago, updated_at FROM symbol_scores_1m_close_gt_60m_open_with_oi_60m WHERE decision_round_ts = ? ORDER BY symbol ASC", (round_ts,)).fetchall()

    def get_latest_round_scores_oi_loss_rate_240m(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_oi_loss_rate_240m").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute("SELECT symbol, decision_round_ts, score, reason, latest_open_interest, open_interest_240m_ago, oi_loss_rate, updated_at FROM symbol_scores_oi_loss_rate_240m WHERE decision_round_ts = ? ORDER BY score DESC, symbol ASC", (round_ts,)).fetchall()
        return round_ts, rows

    def _get_round_scores_oi_loss_rate_240m(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute("SELECT symbol, decision_round_ts, score, reason, latest_open_interest, open_interest_240m_ago, oi_loss_rate, updated_at FROM symbol_scores_oi_loss_rate_240m WHERE decision_round_ts = ? ORDER BY symbol ASC", (round_ts,)).fetchall()
    def _get_round_scores_1h_latest_highest_24(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, latest_high, prev_23_max_high AS prev_24_max_high, updated_at FROM symbol_scores_1h_latest_highest_24 WHERE decision_round_ts = ? ORDER BY symbol ASC",
                (round_ts,),
            ).fetchall()


    def get_latest_round_scores_15m_close_increasing_3of4(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_close_increasing_3of4").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, updated_at FROM symbol_scores_15m_close_increasing_3of4 WHERE decision_round_ts = ? ORDER BY score DESC, symbol ASC",
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_close_increasing_3of4(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT symbol, decision_round_ts, score, reason, updated_at FROM symbol_scores_15m_close_increasing_3of4 WHERE decision_round_ts = ? ORDER BY symbol ASC",
                (round_ts,),
            ).fetchall()


    def get_latest_round_scores_15m_bullish_3of4(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_bullish_3of4").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, bullish_count, updated_at
                FROM symbol_scores_15m_bullish_3of4
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_bullish_3of4(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, bullish_count, updated_at
                FROM symbol_scores_15m_bullish_3of4
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def get_latest_round_scores_close_gt_ma20(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_close_gt_ma20").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1m_close, latest_15m_ma20, updated_at
                FROM symbol_scores_close_gt_ma20
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_close_gt_ma20(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1m_close, latest_15m_ma20, updated_at
                FROM symbol_scores_close_gt_ma20
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def get_latest_round_scores_1h_close_gt_prev(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_1h_close_gt_prev").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1h_close, prev_1h_close, updated_at
                FROM symbol_scores_1h_close_gt_prev
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_1h_close_gt_prev(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1h_close, prev_1h_close, updated_at
                FROM symbol_scores_1h_close_gt_prev
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()


    def get_latest_round_scores_1m_close_gt_5m_ma20(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_1m_close_gt_5m_ma20").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1m_close, latest_5m_ma20, updated_at
                FROM symbol_scores_1m_close_gt_5m_ma20
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_1m_close_gt_5m_ma20(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_1m_close, latest_5m_ma20, updated_at
                FROM symbol_scores_1m_close_gt_5m_ma20
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def _save_score(self, rec: SymbolScore) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores
                (symbol, decision_round_ts, score, reason, ma20_latest, ma20_prev1, ma20_prev2, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    ma20_latest=excluded.ma20_latest,
                    ma20_prev1=excluded.ma20_prev1,
                    ma20_prev2=excluded.ma20_prev2,
                    updated_at=excluded.updated_at
                """,
                (rec.symbol, rec.decision_round_ts, rec.score, rec.reason, rec.ma20_latest, rec.ma20_prev1, rec.ma20_prev2, rec.updated_at),
            )

    def get_latest_round_scores(self) -> tuple[int | None, List[SymbolScore]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, ma20_latest, ma20_prev1, ma20_prev2, updated_at
                FROM symbol_scores
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, [
            SymbolScore(
                symbol=str(r["symbol"]),
                decision_round_ts=int(r["decision_round_ts"]),
                score=int(r["score"]),
                reason=str(r["reason"]),
                ma20_latest=float(r["ma20_latest"]),
                ma20_prev1=float(r["ma20_prev1"]),
                ma20_prev2=float(r["ma20_prev2"]),
                updated_at=int(r["updated_at"]),
            )
            for r in rows
        ]

    def _get_round_scores(self, round_ts: int) -> List[SymbolScore]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, ma20_latest, ma20_prev1, ma20_prev2, updated_at
                FROM symbol_scores
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return [
            SymbolScore(
                symbol=str(r["symbol"]),
                decision_round_ts=int(r["decision_round_ts"]),
                score=int(r["score"]),
                reason=str(r["reason"]),
                ma20_latest=float(r["ma20_latest"]),
                ma20_prev1=float(r["ma20_prev1"]),
                ma20_prev2=float(r["ma20_prev2"]),
                updated_at=int(r["updated_at"]),
            )
            for r in rows
        ]

    def get_latest_round_total_scores(self) -> tuple[int | None, list[SymbolTotalScore]]:
        rule1_ts, _ = self.get_latest_round_scores()
        rule2_ts, _ = self.get_latest_round_scores_close_gt_ma20()
        rule3_ts, _ = self.get_latest_round_scores_1h_close_gt_prev()
        rule4_ts, _ = self.get_latest_round_scores_15m_bullish_3of4()
        rule5_ts, _ = self.get_latest_round_scores_15m_close_increasing_3of4()
        rule6_ts, _ = self.get_latest_round_scores_1m_close_gt_5m_ma20()
        rule7_ts, _ = self.get_latest_round_scores_15m_close_near_high_2of4()
        rule8_ts, _ = self.get_latest_round_scores_1h_latest_highest_24()
        rule9_ts, _ = self.get_latest_round_scores_15m_close_desc_3_with_oi_45m()
        rule10_ts, _ = self.get_latest_round_scores_1m_close_gt_60m_open_with_oi_60m()
        rule11_ts, _ = self.get_latest_round_scores_oi_loss_rate_240m()
        rule12_ts, _ = self.get_latest_round_scores_15m_funding_rate_4bars()
        rule13_ts, _ = self.get_latest_round_scores_15m_bullish_volume_breakout()
        if rule1_ts is None or rule2_ts is None or rule3_ts is None or rule4_ts is None or rule5_ts is None or rule6_ts is None or rule7_ts is None or rule8_ts is None or rule9_ts is None or rule10_ts is None or rule11_ts is None or rule12_ts is None or rule13_ts is None:
            return None, []
        round_ts = min(rule1_ts, rule2_ts, rule3_ts, rule4_ts, rule5_ts, rule6_ts, rule7_ts, rule8_ts, rule9_ts, rule10_ts, rule11_ts, rule12_ts, rule13_ts)
        rule1_rows = self._get_round_scores(round_ts)
        rule2_rows = self._get_round_scores_close_gt_ma20(round_ts)
        rule3_rows = self._get_round_scores_1h_close_gt_prev(round_ts)
        rule4_rows = self._get_round_scores_15m_bullish_3of4(round_ts)
        rule5_rows = self._get_round_scores_15m_close_increasing_3of4(round_ts)
        rule6_rows = self._get_round_scores_1m_close_gt_5m_ma20(round_ts)
        rule7_rows = self._get_round_scores_15m_close_near_high_2of4(round_ts)
        rule8_rows = self._get_round_scores_1h_latest_highest_24(round_ts)
        rule9_rows = self._get_round_scores_15m_close_desc_3_with_oi_45m(round_ts)
        rule10_rows = self._get_round_scores_1m_close_gt_60m_open_with_oi_60m(round_ts)
        rule11_rows = self._get_round_scores_oi_loss_rate_240m(round_ts)
        rule12_rows = self._get_round_scores_15m_funding_rate_4bars(round_ts)
        rule13_rows = self._get_round_scores_15m_bullish_volume_breakout(round_ts)

        rule1_map = {r.symbol: r.score for r in rule1_rows}
        rule2_map = {str(r["symbol"]): int(r["score"]) for r in rule2_rows}
        rule3_map = {str(r["symbol"]): int(r["score"]) for r in rule3_rows}
        rule4_map = {str(r["symbol"]): int(r["score"]) for r in rule4_rows}
        rule5_map = {str(r["symbol"]): int(r["score"]) for r in rule5_rows}
        rule6_map = {str(r["symbol"]): int(r["score"]) for r in rule6_rows}
        rule7_map = {str(r["symbol"]): int(r["score"]) for r in rule7_rows}
        rule8_map = {str(r["symbol"]): int(r["score"]) for r in rule8_rows}
        rule9_map = {str(r["symbol"]): int(r["score"]) for r in rule9_rows}
        rule10_map = {str(r["symbol"]): int(r["score"]) for r in rule10_rows}
        rule11_map = {str(r["symbol"]): int(r["score"]) for r in rule11_rows}
        rule12_map = {str(r["symbol"]): int(r["score"]) for r in rule12_rows}
        rule13_map = {str(r["symbol"]): int(r["score"]) for r in rule13_rows}
        symbols = sorted(set(rule1_map.keys()) | set(rule2_map.keys()) | set(rule3_map.keys()) | set(rule4_map.keys()) | set(rule5_map.keys()) | set(rule6_map.keys()) | set(rule7_map.keys()) | set(rule8_map.keys()) | set(rule9_map.keys()) | set(rule10_map.keys()) | set(rule11_map.keys()) | set(rule12_map.keys()) | set(rule13_map.keys()))

        totals = [
            SymbolTotalScore(
                symbol=s,
                decision_round_ts=round_ts,
                rule1_score=rule1_map.get(s, 0),
                rule2_score=rule2_map.get(s, 0),
                rule3_score=rule3_map.get(s, 0),
                rule4_score=rule4_map.get(s, 0),
                rule5_score=rule5_map.get(s, 0),
                rule6_score=rule6_map.get(s, 0),
                rule7_score=rule7_map.get(s, 0),
                rule8_score=rule8_map.get(s, 0),
                rule9_score=rule9_map.get(s, 0),
                rule10_score=rule10_map.get(s, 0),
                rule11_score=rule11_map.get(s, 0),
                rule12_score=rule12_map.get(s, 0),
                rule13_score=rule13_map.get(s, 0),
                total_score=rule1_map.get(s, 0) + rule2_map.get(s, 0) + rule3_map.get(s, 0) + rule4_map.get(s, 0) + rule5_map.get(s, 0) + rule6_map.get(s, 0) + rule7_map.get(s, 0) + rule8_map.get(s, 0) + rule9_map.get(s, 0) + rule10_map.get(s, 0) + rule11_map.get(s, 0) + rule12_map.get(s, 0) + rule13_map.get(s, 0),
            )
            for s in symbols
        ]
        totals.sort(key=lambda x: (-x.total_score, x.symbol))
        return round_ts, totals
