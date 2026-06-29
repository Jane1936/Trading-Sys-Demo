import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pre_safety_module import PreSafetyModule


def test_trading_position_snapshots_render_after_trade_records():
    template = Path("templates/abnormal_wicks.html").read_text()

    trade_records_index = template.index("<strong>交易实验交易记录</strong>")
    position_snapshots_index = template.index("<strong>交易实验持仓快照</strong>")
    error_records_index = template.index("<strong>交易实验错误信息记录</strong>")

    assert trade_records_index < position_snapshots_index < error_records_index


def test_experiment_equity_trend_chart_renders_under_equity_metric():
    template = Path("templates/abnormal_wicks.html").read_text()

    equity_metric_index = template.index('aria-label="实验组USDT净值"')
    trend_chart_index = template.index('aria-label="近7天实验组USDT净值变化趋势图"')
    trade_records_index = template.index("<strong>交易实验交易记录</strong>")

    assert equity_metric_index < trend_chart_index < trade_records_index
    assert "每15分钟自动刷新" in template
    assert "experiment-equity-trend-chart" in template


def test_filled_orders_summary_includes_expectancy_metric():
    template = Path("templates/abnormal_wicks.html").read_text()

    summary_index = template.index('id="filled-orders-summary"')
    expectancy_metric_index = template.index('id="filled-expectancy"')
    table_index = template.index('class="table-wrap filled-orders-wrap"')

    assert summary_index < expectancy_metric_index < table_index
    assert "已完成订单期望" in template
    assert "const expectancy = winRate * profitLossRatio - (1 - winRate);" in template
    assert "setPnl('filled-expectancy', summary?.expectancy ?? 0);" in template



def test_filled_orders_query_supports_configurable_days_dropdown():
    template = Path("templates/abnormal_wicks.html").read_text()

    panel_index = template.index('<div id="strategy-filled-orders"')
    select_index = template.index('id="filled-orders-days"')
    button_index = template.index('id="query-filled-sell-orders"')

    assert panel_index < select_index < button_index
    assert 'value="1"' in template
    assert 'value="30"' in template
    assert 'function getFilledOrdersDays()' in template
    assert 'Math.max(1, Math.min(Math.trunc(days), 30))' in template
    assert 'days=${encodeURIComponent(days)}' in template

def _insert_abnormal_wick_event(conn, symbol, detected_at):
    conn.execute(
        """
        INSERT INTO abnormal_wick_events (
            symbol, decision_round_ts, candle_index,
            first_candle_open_time, first_candle_close_time,
            open, high, low, close,
            cond1_ratio, cond2_ratio, detected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (symbol, detected_at, 1, detected_at - 300000, detected_at, 1, 2, 0.9, 1.1, 0.7, 0.1, detected_at),
    )


def test_abnormal_wick_recent_event_queries_support_since_filter():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        module = PreSafetyModule(db_path=db_path)
        module.init_table()
        with sqlite3.connect(db_path) as conn:
            _insert_abnormal_wick_event(conn, "OLD", 1_000)
            _insert_abnormal_wick_event(conn, "NEW", 10_000)

        recent_events = module.get_recent_events(limit=10, since_ms=5_000)
        recent_symbols = module.get_event_symbols(since_ms=5_000)
        old_symbol_events = module.get_recent_events_by_symbol("OLD", limit=10, since_ms=5_000)

    assert [event.symbol for event in recent_events] == ["NEW"]
    assert recent_symbols == ["NEW"]
    assert old_symbol_events == []


def test_abnormal_wicks_template_mentions_recent_limits():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "异常插针记录最多只显示近3天数据" in template
    assert "仅展示最近3天数据" in template
    assert "刷新整个页面不会拉取 BTC 数据" in template
    assert "图表在点击刷新按钮后展示最近3天完整5分钟K线（约864根）" in template
    assert "refreshBtcData(1)" in template
    assert "/api/btc/5m?page=" in template


def test_score_page_includes_ma20_skip_warning_at_top():
    template = Path("templates/abnormal_wicks.html").read_text()

    score_header_index = template.index("<h2>评分系统</h2>")
    warning_index = template.index("MA20缺失跳过提示")
    score_tabs_index = template.index('class="toolbar" style="padding-top:0;"')

    assert score_header_index < warning_index < score_tabs_index
    assert "scoring_ma20_skip_record.missing_symbols" in template
    assert "scoring_ma20_skip_record.created_at" in template
    assert "避免卡住整个评分系统" in template


def test_score_page_does_not_require_manual_rule_detail_refresh():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "刷新查看各规则详细数据" not in template
    assert "show-score-rule-details" not in template
    assert 'id="score-rule-details" hidden' not in template


def test_score_page_shows_total_score_actual_completion_time():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "实际计算完成时间" in template
    assert "score_total_updated_at" in template


def test_abnormal_wicks_template_uses_business_friendly_wick_labels():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "candle_index_open_time" in template
    assert "candle_index_close_time" in template
    assert "first_candle_open_time" not in template
    assert "first_candle_close_time" not in template
    assert "长上/下影占比" in template
    assert "振幅度大小" in template
    assert "candle_index_open" in template
    assert "candle_index_high" in template
    assert "candle_index_low" in template
    assert "candle_index_close" in template
    assert "同方向长影/实体 ratio" in template
    assert "是否≥2.5倍" in template
