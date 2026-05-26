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
        if rule1_ts is None or rule2_ts is None or rule3_ts is None or rule4_ts is None or rule5_ts is None:
            return None, []
        round_ts = min(rule1_ts, rule2_ts, rule3_ts, rule4_ts, rule5_ts)
        rule1_rows = self._get_round_scores(round_ts)
        rule2_rows = self._get_round_scores_close_gt_ma20(round_ts)
        rule3_rows = self._get_round_scores_1h_close_gt_prev(round_ts)
        rule4_rows = self._get_round_scores_15m_bullish_3of4(round_ts)
        rule5_rows = self._get_round_scores_15m_close_increasing_3of4(round_ts)

        rule1_map = {r.symbol: r.score for r in rule1_rows}
        rule2_map = {str(r["symbol"]): int(r["score"]) for r in rule2_rows}
        rule3_map = {str(r["symbol"]): int(r["score"]) for r in rule3_rows}
        rule4_map = {str(r["symbol"]): int(r["score"]) for r in rule4_rows}
        rule5_map = {str(r["symbol"]): int(r["score"]) for r in rule5_rows}
        symbols = sorted(set(rule1_map.keys()) | set(rule2_map.keys()) | set(rule3_map.keys()) | set(rule4_map.keys()) | set(rule5_map.keys()))

        totals = [
            SymbolTotalScore(
                symbol=s,
                decision_round_ts=round_ts,
                rule1_score=rule1_map.get(s, 0),
                rule2_score=rule2_map.get(s, 0),
                rule3_score=rule3_map.get(s, 0),
                rule4_score=rule4_map.get(s, 0),
                rule5_score=rule5_map.get(s, 0),
                total_score=rule1_map.get(s, 0) + rule2_map.get(s, 0) + rule3_map.get(s, 0) + rule4_map.get(s, 0) + rule5_map.get(s, 0),
            )
            for s in symbols
        ]
        totals.sort(key=lambda x: (-x.total_score, x.symbol))
        return round_ts, totals
