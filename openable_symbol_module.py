"""Current-round openable symbol evaluator.

This task must run after total scores are persisted for a decision round. It
selects symbols with total_score >= 65 that are not in the cooldown table, then
performs an extra stop-loss-distance screening using rule18's distance_ratio.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import List


@dataclass
class OpenableSymbol:
    symbol: str
    decision_round_ts: int
    total_score: int
    score_band: str
    stop_loss_distance_ratio: float | None
    distance_threshold: float | None
    stop_loss_distance_tier: str
    opening_leverage: str
    distance_qualified: bool
    qualified: bool
    reason: str
    evaluated_at: int


class OpenableSymbolModule:
    """Persist symbols that can be considered for opening in this round."""

    TABLE_NAME = "current_round_openable_symbols"
    MIN_TOTAL_SCORE = 65

    def __init__(self, db_path: str = "data/klines.db") -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    total_score INTEGER NOT NULL,
                    score_band TEXT NOT NULL,
                    stop_loss_distance_ratio REAL,
                    distance_threshold REAL,
                    stop_loss_distance_tier TEXT NOT NULL DEFAULT 'NA',
                    opening_leverage TEXT NOT NULL DEFAULT 'NA',
                    distance_qualified INTEGER NOT NULL,
                    qualified INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    evaluated_at INTEGER NOT NULL,
                    PRIMARY KEY (symbol, decision_round_ts)
                )
                """
            )
            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({self.TABLE_NAME})").fetchall()}
            if "stop_loss_distance_tier" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.TABLE_NAME} "
                    "ADD COLUMN stop_loss_distance_tier TEXT NOT NULL DEFAULT 'NA'"
                )
            if "opening_leverage" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.TABLE_NAME} "
                    "ADD COLUMN opening_leverage TEXT NOT NULL DEFAULT 'NA'"
                )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_round "
                f"ON {self.TABLE_NAME}(decision_round_ts DESC, qualified DESC, total_score DESC, symbol ASC)"
            )

    @staticmethod
    def score_band_for_total(total_score: int) -> str:
        if 65 <= total_score <= 72:
            return "低档试错单"
        if 73 <= total_score <= 80:
            return "标准试错单"
        if 81 <= total_score <= 88:
            return "趋势标准单"
        if 89 <= total_score <= 100:
            return "确定性强趋势单"
        return "NA"

    @staticmethod
    def distance_threshold_for_total(total_score: int) -> float | None:
        if 65 <= total_score <= 72:
            return 0.05
        if 73 <= total_score <= 80:
            return 0.06
        if 81 <= total_score <= 88:
            return 0.07
        if 89 <= total_score <= 100:
            return 0.08
        return None

    @staticmethod
    def stop_loss_distance_tier_for_ratio(distance_ratio: float | None) -> str:
        if distance_ratio is None or distance_ratio <= 0:
            return "NA"
        if distance_ratio <= 0.02:
            return "A档"
        if distance_ratio <= 0.03:
            return "B档"
        if distance_ratio <= 0.04:
            return "C档"
        return "D档"

    @classmethod
    def opening_leverage_for_total_and_distance(
        cls,
        total_score: int,
        distance_ratio: float | None,
        distance_tier: str,
    ) -> str:
        """Return this-round opening leverage formatted as a multiplier string."""
        if total_score >= 81 and distance_ratio == 0:
            return "5x"

        leverage_map = {
            (65, 72): {"A档": "4x", "B档": "3x", "C档": "2x", "D档": "1x"},
            (73, 80): {"A档": "8x", "B档": "6x", "C档": "4x", "D档": "2x"},
            (81, 88): {"A档": "10x", "B档": "7x", "C档": "5x", "D档": "3x"},
            (89, 100): {"A档": "12x", "B档": "8x", "C档": "6x", "D档": "4x"},
        }
        for (lower, upper), tier_leverages in leverage_map.items():
            if lower <= total_score <= upper:
                return tier_leverages.get(distance_tier, "NA")
        return "NA"

    def run_round(
        self,
        decision_round_ts: int,
        evaluated_at: int | None = None,
    ) -> List[OpenableSymbol]:
        """Evaluate total-score candidates after scoring has completed."""
        evaluated_at = int(time.time() * 1000) if evaluated_at is None else int(evaluated_at)
        with self._connect() as conn:
            total_round = conn.execute(
                "SELECT 1 FROM symbol_total_scores WHERE decision_round_ts = ? LIMIT 1",
                (int(decision_round_ts),),
            ).fetchone()
            if total_round is None:
                return []

            rows = conn.execute(
                """
                SELECT
                    t.symbol,
                    t.decision_round_ts,
                    t.total_score,
                    r18.stop_loss_distance_ratio
                FROM symbol_total_scores AS t
                LEFT JOIN current_round_cooldown_symbols AS c
                  ON c.symbol = t.symbol
                 AND c.decision_round_ts = t.decision_round_ts
                LEFT JOIN symbol_scores_structural_stop_loss_distance AS r18
                  ON r18.symbol = t.symbol
                 AND r18.decision_round_ts = t.decision_round_ts
                WHERE t.decision_round_ts = ?
                  AND t.total_score >= ?
                  AND c.symbol IS NULL
                ORDER BY t.total_score DESC, t.symbol ASC
                """,
                (int(decision_round_ts), self.MIN_TOTAL_SCORE),
            ).fetchall()

            symbols_in_scope = [str(row["symbol"]) for row in rows]
            if symbols_in_scope:
                placeholders = ",".join(["?"] * len(symbols_in_scope))
                conn.execute(
                    f"""
                    DELETE FROM {self.TABLE_NAME}
                    WHERE decision_round_ts = ?
                      AND symbol NOT IN ({placeholders})
                    """,
                    [int(decision_round_ts), *symbols_in_scope],
                )
            else:
                conn.execute(
                    f"DELETE FROM {self.TABLE_NAME} WHERE decision_round_ts = ?",
                    (int(decision_round_ts),),
                )

            results = [self._row_to_openable(row, evaluated_at) for row in rows]
            self._save_rows(conn, results)
            return results

    def _row_to_openable(self, row: sqlite3.Row, evaluated_at: int) -> OpenableSymbol:
        total_score = int(row["total_score"])
        ratio = float(row["stop_loss_distance_ratio"]) if row["stop_loss_distance_ratio"] is not None else None
        threshold = self.distance_threshold_for_total(total_score)
        distance_tier = self.stop_loss_distance_tier_for_ratio(ratio)
        opening_leverage = self.opening_leverage_for_total_and_distance(total_score, ratio, distance_tier)
        distance_qualified = ratio is not None and threshold is not None and 0 <= ratio <= threshold
        qualified = distance_qualified
        if threshold is None:
            reason = "total_score_not_in_openable_distance_band"
        elif ratio is None:
            reason = "rule18_distance_ratio_missing"
        elif ratio < 0:
            reason = "stop_loss_distance_negative"
        elif distance_qualified:
            reason = "total_score_not_cooldown_and_stop_loss_distance_qualified"
        else:
            reason = "stop_loss_distance_not_qualified"

        return OpenableSymbol(
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]),
            total_score=total_score,
            score_band=self.score_band_for_total(total_score),
            stop_loss_distance_ratio=ratio,
            distance_threshold=threshold,
            stop_loss_distance_tier=distance_tier,
            opening_leverage=opening_leverage,
            distance_qualified=distance_qualified,
            qualified=qualified,
            reason=reason,
            evaluated_at=evaluated_at,
        )

    def _save_rows(self, conn: sqlite3.Connection, rows: list[OpenableSymbol]) -> None:
        if not rows:
            return
        conn.executemany(
            f"""
            INSERT INTO {self.TABLE_NAME}
            (symbol, decision_round_ts, total_score, score_band, stop_loss_distance_ratio,
             distance_threshold, stop_loss_distance_tier, opening_leverage, distance_qualified,
             qualified, reason, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                total_score=excluded.total_score,
                score_band=excluded.score_band,
                stop_loss_distance_ratio=excluded.stop_loss_distance_ratio,
                distance_threshold=excluded.distance_threshold,
                stop_loss_distance_tier=excluded.stop_loss_distance_tier,
                opening_leverage=excluded.opening_leverage,
                distance_qualified=excluded.distance_qualified,
                qualified=excluded.qualified,
                reason=excluded.reason,
                evaluated_at=excluded.evaluated_at
            """,
            [
                (
                    row.symbol,
                    row.decision_round_ts,
                    row.total_score,
                    row.score_band,
                    row.stop_loss_distance_ratio,
                    row.distance_threshold,
                    row.stop_loss_distance_tier,
                    row.opening_leverage,
                    int(row.distance_qualified),
                    int(row.qualified),
                    row.reason,
                    row.evaluated_at,
                )
                for row in rows
            ],
        )

    def get_latest_round_symbols(self, decision_round_ts: int | None = None) -> tuple[int | None, List[OpenableSymbol]]:
        with self._connect() as conn:
            latest_round_ts = decision_round_ts
            if latest_round_ts is None:
                latest_round_row = conn.execute(
                    f"SELECT MAX(decision_round_ts) AS latest_round_ts FROM {self.TABLE_NAME}"
                ).fetchone()
                latest_round_ts = latest_round_row["latest_round_ts"]
            if latest_round_ts is None:
                return None, []

            rows = conn.execute(
                f"""
                SELECT symbol, decision_round_ts, total_score, score_band,
                       stop_loss_distance_ratio, distance_threshold, stop_loss_distance_tier,
                       opening_leverage, distance_qualified, qualified, reason, evaluated_at
                FROM {self.TABLE_NAME}
                WHERE decision_round_ts = ?
                ORDER BY qualified DESC, total_score DESC, symbol ASC
                """,
                (int(latest_round_ts),),
            ).fetchall()

        return int(latest_round_ts), [self._row_to_dataclass(row) for row in rows]

    @staticmethod
    def _row_to_dataclass(row: sqlite3.Row) -> OpenableSymbol:
        return OpenableSymbol(
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]),
            total_score=int(row["total_score"]),
            score_band=str(row["score_band"]),
            stop_loss_distance_ratio=float(row["stop_loss_distance_ratio"])
            if row["stop_loss_distance_ratio"] is not None
            else None,
            distance_threshold=float(row["distance_threshold"]) if row["distance_threshold"] is not None else None,
            stop_loss_distance_tier=str(row["stop_loss_distance_tier"]),
            opening_leverage=str(row["opening_leverage"]),
            distance_qualified=bool(row["distance_qualified"]),
            qualified=bool(row["qualified"]),
            reason=str(row["reason"]),
            evaluated_at=int(row["evaluated_at"]),
        )
