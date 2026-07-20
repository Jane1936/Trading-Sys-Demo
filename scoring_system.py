"""Symbol scoring system executed every 15-minute decision round."""

from __future__ import annotations

import json
import os
import sqlite3
import db_config
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, List


DEFAULT_RULE_SCORE_WEIGHTS: dict[int, int] = {
    1: 4,
    2: 6,
    3: 6,
    4: 6,
    5: 4,
    6: 6,
    7: 3,
    8: 15,
    9: 10,
    10: 5,
    11: 5,
    12: 8,
    13: 3,
    14: 5,
    15: 2,
    16: 2,
    17: 5,
    18: 5,
}

RULE_SCORE_WEIGHTS_PATH = Path(__file__).with_name("scoring_rule_weights.json")
DEFAULT_STRUCTURAL_STOP_LOSS_COEFFICIENT = 0.98


def _strip_json_comments_and_trailing_commas(raw: str) -> str:
    """Return JSON text with common hand-edited config conveniences removed."""
    result: list[str] = []
    in_string = False
    escape = False
    i = 0
    while i < len(raw):
        ch = raw[i]
        nxt = raw[i + 1] if i + 1 < len(raw) else ""
        if in_string:
            result.append(ch)
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            result.append(ch)
            i += 1
            continue
        if ch == "#":
            while i < len(raw) and raw[i] not in "\r\n":
                i += 1
            continue
        if ch == "/" and nxt == "/":
            i += 2
            while i < len(raw) and raw[i] not in "\r\n":
                i += 1
            continue
        result.append(ch)
        i += 1

    without_comments = "".join(result)
    cleaned: list[str] = []
    in_string = False
    escape = False
    i = 0
    while i < len(without_comments):
        ch = without_comments[i]
        if in_string:
            cleaned.append(ch)
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            cleaned.append(ch)
            i += 1
            continue
        if ch == ",":
            j = i + 1
            while j < len(without_comments) and without_comments[j].isspace():
                j += 1
            if j < len(without_comments) and without_comments[j] in "}]":
                i += 1
                continue
        cleaned.append(ch)
        i += 1
    return "".join(cleaned)


def _load_json_config(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = json.loads(_strip_json_comments_and_trailing_commas(raw))
    if not isinstance(data, dict):
        raise ValueError("Rule score config must be a JSON object")
    return data

def load_rule_score_weights(config_path: str | Path = RULE_SCORE_WEIGHTS_PATH) -> dict[int, int]:
    """Load rule score weights from JSON config, falling back to defaults."""
    weights = DEFAULT_RULE_SCORE_WEIGHTS.copy()
    path = Path(config_path)
    if not path.exists():
        return weights

    data = _load_json_config(path)
    rules = data.get("rules", data)
    if not isinstance(rules, dict):
        raise ValueError("Rule score config must be a JSON object or contain a 'rules' object")

    for rule_id_text, raw_weight in rules.items():
        rule_id = int(rule_id_text)
        if rule_id not in weights:
            raise ValueError(f"Unknown scoring rule id in config: {rule_id}")
        weight = int(raw_weight)
        if weight < 0:
            raise ValueError(f"Scoring rule {rule_id} weight must be non-negative")
        weights[rule_id] = weight
    return weights


def load_structural_stop_loss_coefficient(
    config_path: str | Path = RULE_SCORE_WEIGHTS_PATH,
) -> float:
    """Load structural stop loss coefficient from JSON config."""
    path = Path(config_path)
    if not path.exists():
        return DEFAULT_STRUCTURAL_STOP_LOSS_COEFFICIENT

    data = _load_json_config(path)
    raw_coefficient = data.get(
        "structural_stop_loss_coefficient",
        DEFAULT_STRUCTURAL_STOP_LOSS_COEFFICIENT,
    )
    coefficient = float(raw_coefficient)
    if coefficient <= 0:
        raise ValueError("Structural stop loss coefficient must be positive")
    return coefficient


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
    rule14_score: int
    rule15_score: int
    rule16_score: int
    rule17_score: int
    rule18_score: int
    total_score: int


@dataclass(frozen=True)
class MA20Readiness:
    target_open_time: int
    ready_symbols: list[str]
    missing_symbols: list[str]

    @property
    def ready(self) -> bool:
        return not self.missing_symbols


@dataclass(frozen=True)
class MA20SkipRecord:
    decision_round_ts: int
    target_open_time: int
    universe_count: int
    ready_count: int
    missing_count: int
    missing_symbols: list[str]
    created_at: int


@dataclass(frozen=True)
class ScoringSymbolError:
    symbol: str
    decision_round_ts: int
    error: str
    created_at: int


class ScoringSystem:
    """Score symbols after pre-safety using latest 3 rows of 15m MA20."""

    def __init__(
        self,
        db_path: str = "data/klines.db",
        rule_weights_path: str | Path = RULE_SCORE_WEIGHTS_PATH,
    ) -> None:
        self.db_path = db_path
        self.rule_score_weights = load_rule_score_weights(rule_weights_path)
        self.structural_stop_loss_coefficient = load_structural_stop_loss_coefficient(
            rule_weights_path
        )

    def _score_weight(self, rule_id: int) -> int:
        return self.rule_score_weights[rule_id]

    def _adjust_structural_stop_loss(self, structural_stop_loss: float) -> float:
        if structural_stop_loss <= 0:
            return 0.0
        return structural_stop_loss * self.structural_stop_loss_coefficient

    def _connect(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH)])
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_volume_spike_2of3 (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    qualified_count INTEGER NOT NULL,
                    volume_latest REAL NOT NULL,
                    volume_avg_latest REAL NOT NULL,
                    volume_prev1 REAL NOT NULL,
                    volume_avg_prev1 REAL NOT NULL,
                    volume_prev2 REAL NOT NULL,
                    volume_avg_prev2 REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_volume_spike_2of3_round ON symbol_scores_15m_volume_spike_2of3(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_1h_volume_spike_latest (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_volume REAL NOT NULL,
                    volume_avg REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_1h_volume_spike_latest_round ON symbol_scores_1h_volume_spike_latest(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_pullback_low_volume (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_open REAL NOT NULL,
                    latest_close REAL NOT NULL,
                    latest_body REAL NOT NULL,
                    latest_volume REAL NOT NULL,
                    prev_open REAL NOT NULL,
                    prev_close REAL NOT NULL,
                    prev_body REAL NOT NULL,
                    prev_volume REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_pullback_low_volume_round ON symbol_scores_15m_pullback_low_volume(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_15m_low_rebound_3bars (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    latest_low REAL NOT NULL,
                    latest_close REAL NOT NULL,
                    prev1_low REAL NOT NULL,
                    prev1_close REAL NOT NULL,
                    prev2_low REAL NOT NULL,
                    prev2_close REAL NOT NULL,
                    lowest_low REAL NOT NULL,
                    prev1_rebound_ratio REAL NOT NULL,
                    latest_rebound_ratio REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_15m_low_rebound_3bars_round ON symbol_scores_15m_low_rebound_3bars(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_scores_structural_stop_loss_distance (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    structural_stop_loss REAL NOT NULL,
                    latest_1m_close REAL NOT NULL,
                    stop_loss_distance_ratio REAL NOT NULL,
                    latest_15m_close REAL NOT NULL,
                    prev_15m_close REAL NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_scores_structural_stop_loss_distance_round ON symbol_scores_structural_stop_loss_distance(decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_total_scores (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    rule1_score INTEGER NOT NULL,
                    rule2_score INTEGER NOT NULL,
                    rule3_score INTEGER NOT NULL,
                    rule4_score INTEGER NOT NULL,
                    rule5_score INTEGER NOT NULL,
                    rule6_score INTEGER NOT NULL,
                    rule7_score INTEGER NOT NULL,
                    rule8_score INTEGER NOT NULL,
                    rule9_score INTEGER NOT NULL,
                    rule10_score INTEGER NOT NULL,
                    rule11_score INTEGER NOT NULL,
                    rule12_score INTEGER NOT NULL,
                    rule13_score INTEGER NOT NULL,
                    rule14_score INTEGER NOT NULL,
                    rule15_score INTEGER NOT NULL,
                    rule16_score INTEGER NOT NULL,
                    rule17_score INTEGER NOT NULL,
                    rule18_score INTEGER NOT NULL,
                    total_score INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_total_scores_round ON symbol_total_scores(decision_round_ts DESC, total_score DESC, symbol ASC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_total_scores_symbol_round ON symbol_total_scores(symbol, decision_round_ts DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scoring_ma20_skip_records (
                    decision_round_ts INTEGER PRIMARY KEY,
                    target_open_time INTEGER NOT NULL,
                    universe_count INTEGER NOT NULL,
                    ready_count INTEGER NOT NULL,
                    missing_count INTEGER NOT NULL,
                    missing_symbols_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scoring_ma20_skip_records_created ON scoring_ma20_skip_records(created_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scoring_symbol_error_records (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    error TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scoring_symbol_error_records_round ON scoring_symbol_error_records(decision_round_ts DESC, symbol ASC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_structural_stop_losses (
                    symbol TEXT NOT NULL,
                    decision_round_ts INTEGER NOT NULL,
                    structural_stop_loss REAL NOT NULL,
                    reason TEXT NOT NULL,
                    matched_open_time INTEGER,
                    matched_low REAL NOT NULL,
                    matched_volume REAL NOT NULL,
                    volume_avg REAL NOT NULL,
                    rule_name TEXT NOT NULL DEFAULT 'none',
                    window_start_open_time INTEGER,
                    window_end_open_time INTEGER,
                    window_size INTEGER NOT NULL DEFAULT 0,
                    highest_high REAL NOT NULL DEFAULT 0,
                    lowest_low REAL NOT NULL DEFAULT 0,
                    close_below_mid_count INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(symbol, decision_round_ts)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_structural_stop_losses_round ON symbol_structural_stop_losses(decision_round_ts DESC)"
            )
            structural_stop_loss_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(symbol_structural_stop_losses)").fetchall()
            }
            structural_stop_loss_column_defs = {
                "rule_name": "TEXT NOT NULL DEFAULT 'none'",
                "window_start_open_time": "INTEGER",
                "window_end_open_time": "INTEGER",
                "window_size": "INTEGER NOT NULL DEFAULT 0",
                "highest_high": "REAL NOT NULL DEFAULT 0",
                "lowest_low": "REAL NOT NULL DEFAULT 0",
                "close_below_mid_count": "INTEGER NOT NULL DEFAULT 0",
            }
            for column_name, column_def in structural_stop_loss_column_defs.items():
                if column_name not in structural_stop_loss_columns:
                    conn.execute(
                        f"ALTER TABLE symbol_structural_stop_losses ADD COLUMN {column_name} {column_def}"
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
            try:
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
                self._save_15m_volume_spike_2of3_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                self._save_1h_volume_spike_latest_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                self._save_15m_pullback_low_volume_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                self._save_15m_low_rebound_3bars_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                self._save_structural_stop_loss(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                self._save_structural_stop_loss_distance_score(symbol=symbol, decision_round_ts=decision_round_ts, updated_at=now_ms)
                ma20s = self._latest_three_ma20_15m(symbol)
                if ma20s is None:
                    error = "missing_latest_three_15m_ma20_records"
                    self.record_symbol_error_for_round(
                        decision_round_ts=decision_round_ts,
                        symbol=symbol,
                        error=error,
                        created_at=now_ms,
                    )
                    print(
                        f"⚠️ scoring symbol skipped round={decision_round_ts} "
                        f"symbol={symbol}: {error}"
                    )
                    continue
                m1, m2, m3 = ma20s
                hit = m1 > m2 > m3
                score = self._score_weight(1) if hit else 0
                reason = "ma20_15m_desc_3bars" if hit else "ma20_15m_rule_not_met"
                rec = SymbolScore(symbol, decision_round_ts, score, reason, m1, m2, m3, now_ms)
                results.append(rec)
                self._save_score(rec)
            except Exception as exc:
                self.record_symbol_error_for_round(
                    decision_round_ts=decision_round_ts,
                    symbol=symbol,
                    error=str(exc),
                    created_at=now_ms,
                )
                print(f"⚠️ scoring symbol failed round={decision_round_ts} symbol={symbol}: {exc}")
        self.persist_total_scores_for_round(decision_round_ts=decision_round_ts, updated_at=now_ms)
        return results

    def record_symbol_error_for_round(
        self,
        decision_round_ts: int,
        symbol: str,
        error: str,
        created_at: int | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scoring_symbol_error_records
                (symbol, decision_round_ts, error, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    error=excluded.error,
                    created_at=excluded.created_at
                """,
                (
                    symbol,
                    int(decision_round_ts),
                    error[:500],
                    int(created_at if created_at is not None else time.time() * 1000),
                ),
            )

    def get_15m_ma20_readiness_for_round(
        self, decision_round_ts: int, symbols: Iterable[str]
    ) -> MA20Readiness:
        target_open_time = decision_round_ts - 15 * 60_000
        symbol_list = sorted(set(symbols))
        if not symbol_list:
            return MA20Readiness(target_open_time, [], [])
        placeholders = ",".join(["?"] * len(symbol_list))
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT symbol
                FROM ma20_indicators
                WHERE interval = '15m' AND open_time = ? AND symbol IN ({placeholders})
                """,
                [target_open_time, *symbol_list],
            ).fetchall()
        ready_set = {str(row["symbol"]) for row in rows}
        ready_symbols = [symbol for symbol in symbol_list if symbol in ready_set]
        missing_symbols = [symbol for symbol in symbol_list if symbol not in ready_set]
        return MA20Readiness(target_open_time, ready_symbols, missing_symbols)


    def wait_for_15m_ma20_readiness_for_round(
        self,
        decision_round_ts: int,
        symbols: Iterable[str],
        *,
        retries: int = 0,
        retry_delay_seconds: float = 5.0,
    ) -> MA20Readiness:
        """Check 15m MA20 readiness; callers opt in if they need retries."""
        readiness = self.get_15m_ma20_readiness_for_round(decision_round_ts, symbols)
        remaining_retries = max(0, int(retries))
        while readiness.missing_symbols and remaining_retries > 0:
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)
            retry_readiness = self.get_15m_ma20_readiness_for_round(
                decision_round_ts, readiness.missing_symbols
            )
            ready_symbols = sorted(set(readiness.ready_symbols) | set(retry_readiness.ready_symbols))
            missing_symbols = retry_readiness.missing_symbols
            readiness = MA20Readiness(readiness.target_open_time, ready_symbols, missing_symbols)
            remaining_retries -= 1
        return readiness

    def is_15m_ma20_ready_for_round(self, decision_round_ts: int, symbols: Iterable[str]) -> bool:
        return self.get_15m_ma20_readiness_for_round(decision_round_ts, symbols).ready

    def record_ma20_skip_for_round(
        self,
        decision_round_ts: int,
        readiness: MA20Readiness,
        universe_count: int,
        created_at: int | None = None,
    ) -> None:
        if not readiness.missing_symbols:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scoring_ma20_skip_records
                (decision_round_ts, target_open_time, universe_count, ready_count, missing_count, missing_symbols_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_round_ts) DO UPDATE SET
                    target_open_time=excluded.target_open_time,
                    universe_count=excluded.universe_count,
                    ready_count=excluded.ready_count,
                    missing_count=excluded.missing_count,
                    missing_symbols_json=excluded.missing_symbols_json,
                    created_at=excluded.created_at
                """,
                (
                    int(decision_round_ts),
                    int(readiness.target_open_time),
                    int(universe_count),
                    len(readiness.ready_symbols),
                    len(readiness.missing_symbols),
                    json.dumps(readiness.missing_symbols, ensure_ascii=False),
                    int(created_at if created_at is not None else time.time() * 1000),
                ),
            )

    def _row_to_ma20_skip_record(self, row: sqlite3.Row | None) -> MA20SkipRecord | None:
        if row is None:
            return None
        try:
            missing_symbols = json.loads(row["missing_symbols_json"])
        except json.JSONDecodeError:
            missing_symbols = []
        if not isinstance(missing_symbols, list):
            missing_symbols = []
        return MA20SkipRecord(
            decision_round_ts=int(row["decision_round_ts"]),
            target_open_time=int(row["target_open_time"]),
            universe_count=int(row["universe_count"]),
            ready_count=int(row["ready_count"]),
            missing_count=int(row["missing_count"]),
            missing_symbols=[str(symbol) for symbol in missing_symbols],
            created_at=int(row["created_at"]),
        )

    def get_latest_ma20_skip_record(self) -> MA20SkipRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT decision_round_ts, target_open_time, universe_count, ready_count,
                       missing_count, missing_symbols_json, created_at
                FROM scoring_ma20_skip_records
                ORDER BY decision_round_ts DESC
                LIMIT 1
                """
            ).fetchone()
        return self._row_to_ma20_skip_record(row)

    def get_ma20_skip_record_for_round(self, decision_round_ts: int | None) -> MA20SkipRecord | None:
        if decision_round_ts is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT decision_round_ts, target_open_time, universe_count, ready_count,
                       missing_count, missing_symbols_json, created_at
                FROM scoring_ma20_skip_records
                WHERE decision_round_ts = ?
                LIMIT 1
                """,
                (int(decision_round_ts),),
            ).fetchone()
        return self._row_to_ma20_skip_record(row)

    def get_symbol_errors_for_round(
        self, decision_round_ts: int | None
    ) -> list[ScoringSymbolError]:
        if decision_round_ts is None:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, error, created_at
                FROM scoring_symbol_error_records
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (int(decision_round_ts),),
            ).fetchall()
        return [
            ScoringSymbolError(
                symbol=str(row["symbol"]),
                decision_round_ts=int(row["decision_round_ts"]),
                error=str(row["error"]),
                created_at=int(row["created_at"]),
            )
            for row in rows
        ]

    def get_latest_symbol_error_round(self) -> tuple[int | None, list[ScoringSymbolError]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(decision_round_ts) AS ts FROM scoring_symbol_error_records"
            ).fetchone()
        if row is None or row["ts"] is None:
            return None, []
        round_ts = int(row["ts"])
        return round_ts, self.get_symbol_errors_for_round(round_ts)

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
        score = self._score_weight(2) if hit else 0
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
        score = self._score_weight(6) if hit else 0
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
        score = self._score_weight(3) if hit else 0
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
        score = self._score_weight(4) if hit else 0
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
        score = self._score_weight(5) if hit else 0
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
        score = self._score_weight(7) if hit else 0
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
        score = self._score_weight(8) if hit else 0
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
        score = self._score_weight(9) if hit else 0
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
        score = self._score_weight(10) if hit else 0
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
        score = self._score_weight(11) if hit else 0
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
        score = self._score_weight(12) if hit else 0
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
        score = self._score_weight(13) if volume_hit else 0
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

    def _latest_19_15m_volumes(self, symbol: str) -> list[float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT volume
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 19
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 19:
            return None
        return [float(row["volume"]) for row in rows]

    def _save_15m_volume_spike_2of3_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        volumes = self._latest_19_15m_volumes(symbol)
        if volumes is None:
            return

        # rows are ordered latest -> oldest. For each of the latest 3 bars, compare
        # its volume with the average of the 16 bars immediately before that bar.
        volume_avgs = [sum(volumes[i + 1 : i + 17]) / 16 for i in range(3)]
        qualified_count = sum(1 for i in range(3) if volumes[i] > 1.5 * volume_avgs[i])
        hit = qualified_count >= 2
        score = self._score_weight(14) if hit else 0
        reason = "volume_15m_spike_2of3" if hit else "rule14_not_met"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_volume_spike_2of3
                (symbol, decision_round_ts, score, reason, qualified_count, volume_latest, volume_avg_latest, volume_prev1, volume_avg_prev1, volume_prev2, volume_avg_prev2, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    qualified_count=excluded.qualified_count,
                    volume_latest=excluded.volume_latest,
                    volume_avg_latest=excluded.volume_avg_latest,
                    volume_prev1=excluded.volume_prev1,
                    volume_avg_prev1=excluded.volume_avg_prev1,
                    volume_prev2=excluded.volume_prev2,
                    volume_avg_prev2=excluded.volume_avg_prev2,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    decision_round_ts,
                    score,
                    reason,
                    qualified_count,
                    volumes[0],
                    volume_avgs[0],
                    volumes[1],
                    volume_avgs[1],
                    volumes[2],
                    volume_avgs[2],
                    updated_at,
                ),
            )

    def get_latest_round_scores_15m_volume_spike_2of3(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_volume_spike_2of3").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, qualified_count, volume_latest, volume_avg_latest, volume_prev1, volume_avg_prev1, volume_prev2, volume_avg_prev2, updated_at
                FROM symbol_scores_15m_volume_spike_2of3
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_volume_spike_2of3(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, qualified_count, volume_latest, volume_avg_latest, volume_prev1, volume_avg_prev1, volume_prev2, volume_avg_prev2, updated_at
                FROM symbol_scores_15m_volume_spike_2of3
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def _latest_13_1h_volumes(self, symbol: str) -> list[float] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT volume
                FROM klines_1h
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 13
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 13:
            return None
        return [float(row["volume"]) for row in rows]

    def _save_1h_volume_spike_latest_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        volumes = self._latest_13_1h_volumes(symbol)
        if volumes is None:
            return

        latest_volume = volumes[0]
        volume_avg = sum(volumes[1:13]) / 12
        hit = latest_volume > 1.5 * volume_avg
        score = self._score_weight(15) if hit else 0
        reason = "latest_1h_volume_gt_1_5_avg_prev_12" if hit else "rule15_not_met"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_1h_volume_spike_latest
                (symbol, decision_round_ts, score, reason, latest_volume, volume_avg, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_volume=excluded.latest_volume,
                    volume_avg=excluded.volume_avg,
                    updated_at=excluded.updated_at
                """,
                (symbol, decision_round_ts, score, reason, latest_volume, volume_avg, updated_at),
            )

    def get_latest_round_scores_1h_volume_spike_latest(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_1h_volume_spike_latest").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_volume, volume_avg, updated_at
                FROM symbol_scores_1h_volume_spike_latest
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_1h_volume_spike_latest(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_volume, volume_avg, updated_at
                FROM symbol_scores_1h_volume_spike_latest
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()


    def _latest_2_15m_ohlcv(self, symbol: str) -> list[sqlite3.Row] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open, close, volume
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 2
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 2:
            return None
        return rows

    def _save_15m_pullback_low_volume_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_2_15m_ohlcv(symbol)
        if rows is None:
            return

        latest_open = float(rows[0]["open"])
        latest_close = float(rows[0]["close"])
        latest_volume = float(rows[0]["volume"])
        prev_open = float(rows[1]["open"])
        prev_close = float(rows[1]["close"])
        prev_volume = float(rows[1]["volume"])
        latest_body = abs(latest_close - latest_open)
        prev_body = abs(prev_close - prev_open)

        price_or_body_hit = latest_close < prev_close or latest_body < prev_body
        volume_hit = latest_volume < 0.7 * prev_volume
        hit = price_or_body_hit and volume_hit
        score = self._score_weight(16) if hit else 0
        if hit:
            reason = "latest_15m_pullback_or_smaller_body_with_low_volume"
        elif price_or_body_hit:
            reason = "rule16_volume_not_met"
        else:
            reason = "rule16_price_body_not_met"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_pullback_low_volume
                (symbol, decision_round_ts, score, reason, latest_open, latest_close, latest_body, latest_volume, prev_open, prev_close, prev_body, prev_volume, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_open=excluded.latest_open,
                    latest_close=excluded.latest_close,
                    latest_body=excluded.latest_body,
                    latest_volume=excluded.latest_volume,
                    prev_open=excluded.prev_open,
                    prev_close=excluded.prev_close,
                    prev_body=excluded.prev_body,
                    prev_volume=excluded.prev_volume,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    decision_round_ts,
                    score,
                    reason,
                    latest_open,
                    latest_close,
                    latest_body,
                    latest_volume,
                    prev_open,
                    prev_close,
                    prev_body,
                    prev_volume,
                    updated_at,
                ),
            )

    def get_latest_round_scores_15m_pullback_low_volume(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_pullback_low_volume").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_open, latest_close, latest_body, latest_volume, prev_open, prev_close, prev_body, prev_volume, updated_at
                FROM symbol_scores_15m_pullback_low_volume
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_pullback_low_volume(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_open, latest_close, latest_body, latest_volume, prev_open, prev_close, prev_body, prev_volume, updated_at
                FROM symbol_scores_15m_pullback_low_volume
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def _latest_3_15m_low_close(self, symbol: str) -> list[sqlite3.Row] | None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT low, close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 3
                """,
                (symbol,),
            ).fetchall()
        if len(rows) < 3:
            return None
        return rows

    def _save_15m_low_rebound_3bars_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_3_15m_low_close(symbol)
        if rows is None:
            return

        latest_low = float(rows[0]["low"])
        latest_close = float(rows[0]["close"])
        prev1_low = float(rows[1]["low"])
        prev1_close = float(rows[1]["close"])
        prev2_low = float(rows[2]["low"])
        prev2_close = float(rows[2]["close"])
        lowest_low = min(latest_low, prev1_low, prev2_low)
        if lowest_low <= 0:
            prev1_rebound_ratio = 0.0
            latest_rebound_ratio = 0.0
            hit = False
            reason = "rule17_invalid_lowest_low"
        else:
            prev1_rebound_ratio = (prev1_close - lowest_low) / lowest_low
            latest_rebound_ratio = (latest_close - lowest_low) / lowest_low
            prev2_low_is_lowest = prev2_low == lowest_low
            prev1_low_is_lowest = prev1_low == lowest_low
            if prev2_low_is_lowest:
                hit = prev1_rebound_ratio >= 0.06 or latest_rebound_ratio >= 0.06
                reason = "third_15m_lowest_and_rebound_ge_6pct" if hit else "rule17_third_lowest_rebound_not_met"
            elif prev1_low_is_lowest:
                hit = latest_rebound_ratio >= 0.06
                reason = "second_15m_lowest_and_latest_rebound_ge_6pct" if hit else "rule17_second_lowest_rebound_not_met"
            else:
                hit = False
                reason = "rule17_latest_lowest_or_no_qualified_lowest"
        score = self._score_weight(17) if hit else 0

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_15m_low_rebound_3bars
                (symbol, decision_round_ts, score, reason, latest_low, latest_close, prev1_low, prev1_close, prev2_low, prev2_close, lowest_low, prev1_rebound_ratio, latest_rebound_ratio, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    latest_low=excluded.latest_low,
                    latest_close=excluded.latest_close,
                    prev1_low=excluded.prev1_low,
                    prev1_close=excluded.prev1_close,
                    prev2_low=excluded.prev2_low,
                    prev2_close=excluded.prev2_close,
                    lowest_low=excluded.lowest_low,
                    prev1_rebound_ratio=excluded.prev1_rebound_ratio,
                    latest_rebound_ratio=excluded.latest_rebound_ratio,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    decision_round_ts,
                    score,
                    reason,
                    latest_low,
                    latest_close,
                    prev1_low,
                    prev1_close,
                    prev2_low,
                    prev2_close,
                    lowest_low,
                    prev1_rebound_ratio,
                    latest_rebound_ratio,
                    updated_at,
                ),
            )

    def get_latest_round_scores_15m_low_rebound_3bars(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_15m_low_rebound_3bars").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_low, latest_close, prev1_low, prev1_close, prev2_low, prev2_close, lowest_low, prev1_rebound_ratio, latest_rebound_ratio, updated_at
                FROM symbol_scores_15m_low_rebound_3bars
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_15m_low_rebound_3bars(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, latest_low, latest_close, prev1_low, prev1_close, prev2_low, prev2_close, lowest_low, prev1_rebound_ratio, latest_rebound_ratio, updated_at
                FROM symbol_scores_15m_low_rebound_3bars
                WHERE decision_round_ts = ?
                ORDER BY symbol ASC
                """,
                (round_ts,),
            ).fetchall()

    def _latest_24_15m_ohlcv(self, symbol: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT open_time, open, high, low, close, volume
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 24
                """,
                (symbol,),
            ).fetchall()

    def _save_structural_stop_loss(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        rows = self._latest_24_15m_ohlcv(symbol)
        structural_stop_loss = 0.0
        reason = "structural_stop_loss_not_found"
        matched_open_time = None
        matched_low = 0.0
        matched_volume = 0.0
        volume_avg = 0.0
        rule_name = "none"
        window_start_open_time = None
        window_end_open_time = None
        window_size = 0
        highest_high = 0.0
        lowest_low = 0.0
        close_below_mid_count = 0

        # Rule 1: traverse the latest 8 closed 15m bars from newest to oldest.
        # For each bar, compare its volume with the average volume of the 16 bars
        # immediately before it.
        for row_index, row in enumerate(rows[:8]):
            prev_rows = rows[row_index + 1 : row_index + 17]
            if len(prev_rows) < 16:
                continue

            current_open = float(row["open"])
            current_high = float(row["high"])
            current_low = float(row["low"])
            current_close = float(row["close"])
            current_volume = float(row["volume"])
            if current_open <= 0:
                continue

            body_ratio = (current_close - current_open) / current_open
            close_position_hit = current_close >= (current_high + current_low) / 2
            if body_ratio < 0.025 or not close_position_hit:
                continue

            current_volume_avg = sum(float(prev_row["volume"]) for prev_row in prev_rows) / 16
            if current_volume >= 1.5 * current_volume_avg:
                structural_stop_loss = current_low
                reason = "structural_stop_loss_rule1_found"
                matched_open_time = int(row["open_time"])
                matched_low = current_low
                matched_volume = current_volume
                volume_avg = current_volume_avg
                rule_name = "rule1"
                window_start_open_time = matched_open_time
                window_end_open_time = matched_open_time
                window_size = 1
                highest_high = current_high
                lowest_low = current_low
                close_below_mid_count = 0
                break

        # Rule 2 only runs when Rule 1 did not find a structural stop loss. Scan
        # contiguous windows inside the latest 12 closed 15m bars, starting from the
        # most recent bar and then moving backward. For the same start point, check
        # smaller windows before larger ones so the first match is the nearest and
        # tightest valid window.
        if structural_stop_loss == 0:
            latest_12_rows = rows[:12]
            for window_start_index in range(max(0, len(latest_12_rows) - 3)):
                max_window_size = min(12, len(latest_12_rows) - window_start_index)
                rule2_matched = False
                for current_window_size in range(4, max_window_size + 1):
                    window_rows = latest_12_rows[
                        window_start_index : window_start_index + current_window_size
                    ]
                    current_highest_high = max(float(window_row["high"]) for window_row in window_rows)
                    current_lowest_low = min(float(window_row["low"]) for window_row in window_rows)
                    if current_lowest_low <= 0:
                        continue

                    current_close_below_mid_count = sum(
                        1
                        for window_row in window_rows
                        if float(window_row["close"])
                        <= (float(window_row["high"]) + float(window_row["low"])) / 2
                    )
                    range_ratio = (current_highest_high - current_lowest_low) / current_lowest_low
                    if current_close_below_mid_count >= 2 and range_ratio <= 0.03:
                        structural_stop_loss = current_highest_high
                        reason = "structural_stop_loss_rule2_found"
                        matched_open_time = int(window_rows[0]["open_time"])
                        matched_low = current_lowest_low
                        matched_volume = 0.0
                        volume_avg = 0.0
                        rule_name = "rule2"
                        window_start_open_time = int(window_rows[0]["open_time"])
                        window_end_open_time = int(window_rows[-1]["open_time"])
                        window_size = current_window_size
                        highest_high = current_highest_high
                        lowest_low = current_lowest_low
                        close_below_mid_count = current_close_below_mid_count
                        rule2_matched = True
                        break
                if rule2_matched:
                    break

        # Rule 3 only runs when Rule 1 and Rule 2 did not find a structural stop
        # loss. It checks whether the latest 24 closed 15m bars form an overall
        # upward structure and then looks for the nearest qualifying pullback before
        # the highest-high bar.
        if structural_stop_loss == 0 and len(rows) >= 24:
            latest_24_rows = rows[:24]
            highest_index, highest_row = max(
                enumerate(latest_24_rows), key=lambda item: float(item[1]["high"])
            )
            lowest_index, lowest_row = min(
                enumerate(latest_24_rows), key=lambda item: float(item[1]["low"])
            )
            current_highest_high = float(highest_row["high"])
            current_lowest_low = float(lowest_row["low"])
            highest_position = highest_index + 1
            lowest_position = lowest_index + 1

            if (
                current_lowest_low > 0
                and highest_position <= lowest_position
                and highest_position >= 3
                and (current_highest_high - current_lowest_low) / current_lowest_low >= 0.05
            ):
                for pullback_index in range(1, highest_index):
                    pullback_row = latest_24_rows[pullback_index]
                    prev_row = latest_24_rows[pullback_index - 1]
                    pullback_close = float(pullback_row["close"])
                    pullback_low = float(pullback_row["low"])
                    pullback_open = float(pullback_row["open"])
                    prev_close = float(prev_row["close"])

                    if (
                        (current_highest_high - pullback_close) / current_highest_high >= 0.03
                        and pullback_low > current_lowest_low
                        and prev_close > pullback_open
                    ):
                        structural_stop_loss = pullback_low
                        reason = "structural_stop_loss_rule3_found"
                        matched_open_time = int(pullback_row["open_time"])
                        matched_low = pullback_low
                        matched_volume = float(pullback_row["volume"])
                        volume_avg = 0.0
                        rule_name = "rule3"
                        window_start_open_time = int(latest_24_rows[0]["open_time"])
                        window_end_open_time = int(latest_24_rows[-1]["open_time"])
                        window_size = 24
                        highest_high = current_highest_high
                        lowest_low = current_lowest_low
                        close_below_mid_count = 0
                        break

        structural_stop_loss = self._adjust_structural_stop_loss(structural_stop_loss)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_structural_stop_losses
                (
                    symbol, decision_round_ts, structural_stop_loss, reason,
                    matched_open_time, matched_low, matched_volume, volume_avg,
                    rule_name, window_start_open_time, window_end_open_time,
                    window_size, highest_high, lowest_low, close_below_mid_count,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    structural_stop_loss=excluded.structural_stop_loss,
                    reason=excluded.reason,
                    matched_open_time=excluded.matched_open_time,
                    matched_low=excluded.matched_low,
                    matched_volume=excluded.matched_volume,
                    volume_avg=excluded.volume_avg,
                    rule_name=excluded.rule_name,
                    window_start_open_time=excluded.window_start_open_time,
                    window_end_open_time=excluded.window_end_open_time,
                    window_size=excluded.window_size,
                    highest_high=excluded.highest_high,
                    lowest_low=excluded.lowest_low,
                    close_below_mid_count=excluded.close_below_mid_count,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    decision_round_ts,
                    structural_stop_loss,
                    reason,
                    matched_open_time,
                    matched_low,
                    matched_volume,
                    volume_avg,
                    rule_name,
                    window_start_open_time,
                    window_end_open_time,
                    window_size,
                    highest_high,
                    lowest_low,
                    close_below_mid_count,
                    updated_at,
                ),
            )

    def get_latest_round_structural_stop_losses(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_structural_stop_losses").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT
                    symbol, decision_round_ts, structural_stop_loss, reason,
                    matched_open_time, matched_low, matched_volume, volume_avg,
                    rule_name, window_start_open_time, window_end_open_time,
                    window_size, highest_high, lowest_low, close_below_mid_count,
                    updated_at
                FROM symbol_structural_stop_losses
                WHERE decision_round_ts = ?
                ORDER BY structural_stop_loss DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _structural_stop_loss_for_round(self, symbol: str, decision_round_ts: int) -> float:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT structural_stop_loss
                FROM symbol_structural_stop_losses
                WHERE symbol = ? AND decision_round_ts = ?
                """,
                (symbol, decision_round_ts),
            ).fetchone()
        if not row:
            return 0.0
        return float(row["structural_stop_loss"])

    def _latest_1m_close_and_two_15m_close(self, symbol: str) -> tuple[float, float, float] | None:
        with self._connect() as conn:
            close_1m_row = conn.execute(
                """
                SELECT close
                FROM klines_1m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            close_15m_rows = conn.execute(
                """
                SELECT close
                FROM klines_15m
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT 2
                """,
                (symbol,),
            ).fetchall()
        if not close_1m_row or len(close_15m_rows) < 2:
            return None
        return float(close_1m_row["close"]), float(close_15m_rows[0]["close"]), float(close_15m_rows[1]["close"])

    def _save_structural_stop_loss_distance_score(self, symbol: str, decision_round_ts: int, updated_at: int) -> None:
        structural_stop_loss = self._structural_stop_loss_for_round(symbol, decision_round_ts)
        latest_1m_close = 0.0
        stop_loss_distance_ratio = 0.0
        latest_15m_close = 0.0
        prev_15m_close = 0.0
        score = 0

        if structural_stop_loss <= 0:
            reason = "rule18_structural_stop_loss_not_positive"
        else:
            values = self._latest_1m_close_and_two_15m_close(symbol)
            if values is None:
                reason = "rule18_insufficient_kline_data"
            else:
                latest_1m_close, latest_15m_close, prev_15m_close = values
                if latest_1m_close <= 0:
                    reason = "rule18_invalid_latest_1m_close"
                else:
                    stop_loss_distance_ratio = (latest_1m_close - structural_stop_loss) / latest_1m_close
                    distance_hit = (0 < stop_loss_distance_ratio <= 0.035)
                    rhythm_hit = latest_15m_close >= prev_15m_close
                    if distance_hit and rhythm_hit:
                        score = self._score_weight(18)
                        reason = "structural_stop_loss_distance_qualified_and_15m_close_not_down"
                    elif distance_hit:
                        reason = "rule18_15m_close_not_met"
                    else:
                        reason = "rule18_stop_loss_distance_not_met"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_scores_structural_stop_loss_distance
                (symbol, decision_round_ts, score, reason, structural_stop_loss, latest_1m_close, stop_loss_distance_ratio, latest_15m_close, prev_15m_close, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    score=excluded.score,
                    reason=excluded.reason,
                    structural_stop_loss=excluded.structural_stop_loss,
                    latest_1m_close=excluded.latest_1m_close,
                    stop_loss_distance_ratio=excluded.stop_loss_distance_ratio,
                    latest_15m_close=excluded.latest_15m_close,
                    prev_15m_close=excluded.prev_15m_close,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    decision_round_ts,
                    score,
                    reason,
                    structural_stop_loss,
                    latest_1m_close,
                    stop_loss_distance_ratio,
                    latest_15m_close,
                    prev_15m_close,
                    updated_at,
                ),
            )

    def get_latest_round_scores_structural_stop_loss_distance(self) -> tuple[int | None, list[sqlite3.Row]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_scores_structural_stop_loss_distance").fetchone()
            if row["ts"] is None:
                return None, []
            round_ts = int(row["ts"])
            rows = conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, structural_stop_loss, latest_1m_close, stop_loss_distance_ratio, latest_15m_close, prev_15m_close, updated_at
                FROM symbol_scores_structural_stop_loss_distance
                WHERE decision_round_ts = ?
                ORDER BY score DESC, symbol ASC
                """,
                (round_ts,),
            ).fetchall()
        return round_ts, rows

    def _get_round_scores_structural_stop_loss_distance(self, round_ts: int) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT symbol, decision_round_ts, score, reason, structural_stop_loss, latest_1m_close, stop_loss_distance_ratio, latest_15m_close, prev_15m_close, updated_at
                FROM symbol_scores_structural_stop_loss_distance
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

    def _row_to_total_score(self, row: sqlite3.Row) -> SymbolTotalScore:
        return SymbolTotalScore(
            symbol=str(row["symbol"]),
            decision_round_ts=int(row["decision_round_ts"]),
            rule1_score=int(row["rule1_score"]),
            rule2_score=int(row["rule2_score"]),
            rule3_score=int(row["rule3_score"]),
            rule4_score=int(row["rule4_score"]),
            rule5_score=int(row["rule5_score"]),
            rule6_score=int(row["rule6_score"]),
            rule7_score=int(row["rule7_score"]),
            rule8_score=int(row["rule8_score"]),
            rule9_score=int(row["rule9_score"]),
            rule10_score=int(row["rule10_score"]),
            rule11_score=int(row["rule11_score"]),
            rule12_score=int(row["rule12_score"]),
            rule13_score=int(row["rule13_score"]),
            rule14_score=int(row["rule14_score"]),
            rule15_score=int(row["rule15_score"]),
            rule16_score=int(row["rule16_score"]),
            rule17_score=int(row["rule17_score"]),
            rule18_score=int(row["rule18_score"]),
            total_score=int(row["total_score"]),
        )

    def _build_total_scores_for_round(self, round_ts: int) -> list[SymbolTotalScore]:
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
        rule14_rows = self._get_round_scores_15m_volume_spike_2of3(round_ts)
        rule15_rows = self._get_round_scores_1h_volume_spike_latest(round_ts)
        rule16_rows = self._get_round_scores_15m_pullback_low_volume(round_ts)
        rule17_rows = self._get_round_scores_15m_low_rebound_3bars(round_ts)
        rule18_rows = self._get_round_scores_structural_stop_loss_distance(round_ts)

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
        rule14_map = {str(r["symbol"]): int(r["score"]) for r in rule14_rows}
        rule15_map = {str(r["symbol"]): int(r["score"]) for r in rule15_rows}
        rule16_map = {str(r["symbol"]): int(r["score"]) for r in rule16_rows}
        rule17_map = {str(r["symbol"]): int(r["score"]) for r in rule17_rows}
        rule18_map = {str(r["symbol"]): int(r["score"]) for r in rule18_rows}
        symbols = sorted(
            set(rule1_map.keys())
            | set(rule2_map.keys())
            | set(rule3_map.keys())
            | set(rule4_map.keys())
            | set(rule5_map.keys())
            | set(rule6_map.keys())
            | set(rule7_map.keys())
            | set(rule8_map.keys())
            | set(rule9_map.keys())
            | set(rule10_map.keys())
            | set(rule11_map.keys())
            | set(rule12_map.keys())
            | set(rule13_map.keys())
            | set(rule14_map.keys())
            | set(rule15_map.keys())
            | set(rule16_map.keys())
            | set(rule17_map.keys())
            | set(rule18_map.keys())
        )

        totals: list[SymbolTotalScore] = []
        for s in symbols:
            rule_scores = [
                rule1_map.get(s, 0), rule2_map.get(s, 0), rule3_map.get(s, 0),
                rule4_map.get(s, 0), rule5_map.get(s, 0), rule6_map.get(s, 0),
                rule7_map.get(s, 0), rule8_map.get(s, 0), rule9_map.get(s, 0),
                rule10_map.get(s, 0), rule11_map.get(s, 0), rule12_map.get(s, 0),
                rule13_map.get(s, 0), rule14_map.get(s, 0), rule15_map.get(s, 0),
                rule16_map.get(s, 0), rule17_map.get(s, 0), rule18_map.get(s, 0),
            ]
            totals.append(SymbolTotalScore(s, round_ts, *rule_scores, total_score=sum(rule_scores)))
        totals.sort(key=lambda x: (-x.total_score, x.symbol))
        return totals

    def _save_total_scores(self, totals: list[SymbolTotalScore], updated_at: int) -> None:
        if not totals:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO symbol_total_scores
                (symbol, decision_round_ts, rule1_score, rule2_score, rule3_score, rule4_score, rule5_score, rule6_score, rule7_score, rule8_score, rule9_score, rule10_score, rule11_score, rule12_score, rule13_score, rule14_score, rule15_score, rule16_score, rule17_score, rule18_score, total_score, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, decision_round_ts) DO UPDATE SET
                    rule1_score=excluded.rule1_score,
                    rule2_score=excluded.rule2_score,
                    rule3_score=excluded.rule3_score,
                    rule4_score=excluded.rule4_score,
                    rule5_score=excluded.rule5_score,
                    rule6_score=excluded.rule6_score,
                    rule7_score=excluded.rule7_score,
                    rule8_score=excluded.rule8_score,
                    rule9_score=excluded.rule9_score,
                    rule10_score=excluded.rule10_score,
                    rule11_score=excluded.rule11_score,
                    rule12_score=excluded.rule12_score,
                    rule13_score=excluded.rule13_score,
                    rule14_score=excluded.rule14_score,
                    rule15_score=excluded.rule15_score,
                    rule16_score=excluded.rule16_score,
                    rule17_score=excluded.rule17_score,
                    rule18_score=excluded.rule18_score,
                    total_score=excluded.total_score,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        t.symbol, t.decision_round_ts, t.rule1_score, t.rule2_score, t.rule3_score,
                        t.rule4_score, t.rule5_score, t.rule6_score, t.rule7_score, t.rule8_score,
                        t.rule9_score, t.rule10_score, t.rule11_score, t.rule12_score, t.rule13_score,
                        t.rule14_score, t.rule15_score, t.rule16_score, t.rule17_score, t.rule18_score,
                        t.total_score, updated_at,
                    )
                    for t in totals
                ],
            )

    def persist_total_scores_for_round(self, decision_round_ts: int, updated_at: int | None = None) -> list[SymbolTotalScore]:
        totals = self._build_total_scores_for_round(decision_round_ts)
        self._save_total_scores(totals, updated_at or int(time.time() * 1000))
        return totals


    def get_total_score_round_updated_at(self, decision_round_ts: int | None) -> int | None:
        if decision_round_ts is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(updated_at) AS updated_at
                FROM symbol_total_scores
                WHERE decision_round_ts = ?
                """,
                (int(decision_round_ts),),
            ).fetchone()
        if row is None or row["updated_at"] is None:
            return None
        return int(row["updated_at"])

    def get_latest_round_total_scores(self) -> tuple[int | None, list[SymbolTotalScore]]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(decision_round_ts) AS ts FROM symbol_total_scores").fetchone()
            if row["ts"] is not None:
                round_ts = int(row["ts"])
                rows = conn.execute(
                    """
                    SELECT symbol, decision_round_ts, rule1_score, rule2_score, rule3_score, rule4_score, rule5_score, rule6_score, rule7_score, rule8_score, rule9_score, rule10_score, rule11_score, rule12_score, rule13_score, rule14_score, rule15_score, rule16_score, rule17_score, rule18_score, total_score
                    FROM symbol_total_scores
                    WHERE decision_round_ts = ?
                    ORDER BY total_score DESC, symbol ASC
                    """,
                    (round_ts,),
                ).fetchall()
                return round_ts, [self._row_to_total_score(r) for r in rows]

        rule_timestamps = [
            self.get_latest_round_scores()[0],
            self.get_latest_round_scores_close_gt_ma20()[0],
            self.get_latest_round_scores_1h_close_gt_prev()[0],
            self.get_latest_round_scores_15m_bullish_3of4()[0],
            self.get_latest_round_scores_15m_close_increasing_3of4()[0],
            self.get_latest_round_scores_1m_close_gt_5m_ma20()[0],
            self.get_latest_round_scores_15m_close_near_high_2of4()[0],
            self.get_latest_round_scores_1h_latest_highest_24()[0],
            self.get_latest_round_scores_15m_close_desc_3_with_oi_45m()[0],
            self.get_latest_round_scores_1m_close_gt_60m_open_with_oi_60m()[0],
            self.get_latest_round_scores_oi_loss_rate_240m()[0],
            self.get_latest_round_scores_15m_funding_rate_4bars()[0],
            self.get_latest_round_scores_15m_bullish_volume_breakout()[0],
            self.get_latest_round_scores_15m_volume_spike_2of3()[0],
            self.get_latest_round_scores_1h_volume_spike_latest()[0],
            self.get_latest_round_scores_15m_pullback_low_volume()[0],
            self.get_latest_round_scores_15m_low_rebound_3bars()[0],
            self.get_latest_round_scores_structural_stop_loss_distance()[0],
        ]
        if any(ts is None for ts in rule_timestamps):
            return None, []
        round_ts = min(int(ts) for ts in rule_timestamps if ts is not None)
        totals = self.persist_total_scores_for_round(decision_round_ts=round_ts)
        return round_ts, totals

    def get_total_score_symbols(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM symbol_total_scores
                ORDER BY symbol ASC
                """
            ).fetchall()
        return [str(r["symbol"]) for r in rows]

    def get_total_score_trend(self, symbol: str, days: int = 3) -> list[sqlite3.Row]:
        cutoff_ts = int(time.time() * 1000) - days * 24 * 60 * 60 * 1000
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT decision_round_ts, total_score
                FROM symbol_total_scores
                WHERE symbol = ? AND decision_round_ts >= ?
                ORDER BY decision_round_ts ASC
                """,
                (symbol, cutoff_ts),
            ).fetchall()
