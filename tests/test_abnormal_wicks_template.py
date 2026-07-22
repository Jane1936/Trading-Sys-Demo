import sqlite3
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pre_safety_module import PreSafetyModule
from web_app import (
    DEFAULT_TRADING_EQUITY_USDT,
    _latest_trading_equity_usdt,
    _trading_open_increase_blocked,
    _trading_used_margin_text,
)


def test_trading_position_snapshots_show_used_margin_summary():
    template = Path("templates/abnormal_wicks.html").read_text()

    snapshot_index = template.index("<strong>交易实验持仓快照</strong>")
    used_margin_index = template.index("已用资金：{{ trading_used_margin_usdt }} USDT")
    block_notice_index = template.index("禁止任何新开仓/加仓")
    table_index = template.index("<th>position_amt</th>", snapshot_index)

    assert snapshot_index < used_margin_index < block_notice_index < table_index
    assert "trading_open_increase_blocked" in template
    assert "trading_equity_usdt" in template


def test_trading_used_margin_uses_reserved_margin_budget_from_notional_divided_by_leverage():
    snapshots = [
        SimpleNamespace(position_amt="2", notional="21", mark_price="10.5", leverage="3"),
        SimpleNamespace(position_amt="-3", notional="-60", mark_price="20", leverage="4"),
        SimpleNamespace(position_amt="1", notional="100", mark_price="100", leverage="0"),
        SimpleNamespace(position_amt="1", notional="100", mark_price="100", leverage="bad"),
        SimpleNamespace(position_amt="bad", notional="100", mark_price="100", leverage="5"),
    ]

    assert _trading_used_margin_text(snapshots) == "22"




def test_trading_open_increase_blocked_when_used_margin_exceeds_equity():
    snapshots = [SimpleNamespace(position_amt="2", notional="200", unrealized_pnl="0", mark_price="100", leverage="1")]

    assert _trading_open_increase_blocked("199.99", snapshots) is True
    assert _trading_open_increase_blocked("200", snapshots) is False


def test_trading_open_increase_blocked_allows_unrealized_pnl_buffer():
    snapshots = [SimpleNamespace(position_amt="2", notional="210", unrealized_pnl="15", mark_price="100", leverage="1")]

    assert _trading_open_increase_blocked("200", snapshots) is False
    assert _trading_open_increase_blocked("194.99", snapshots) is True


def test_latest_trading_equity_usdt_reads_last_trend_row_or_default():
    rows = [
        {"account_equity_usdt": 100.0},
        {"account_equity_usdt": 125.5},
    ]

    assert _latest_trading_equity_usdt(rows) == 125.5
    assert _latest_trading_equity_usdt([]) == DEFAULT_TRADING_EQUITY_USDT


def test_zombie_force_liquidation_records_render_above_trade_records():
    template = Path("templates/abnormal_wicks.html").read_text()

    trend_chart_index = template.index('aria-label="近7天实验组USDT净值变化趋势图"')
    zombie_records_index = template.index("<strong>僵尸单强平操作记录</strong>")
    trade_records_index = template.index("<strong>交易实验交易记录</strong>")
    position_snapshots_index = template.index("<strong>交易实验持仓快照</strong>")
    error_records_index = template.index("<strong>交易实验错误信息记录</strong>")

    assert trend_chart_index < zombie_records_index < trade_records_index < position_snapshots_index < error_records_index
    assert "只显示最近7天内记录" in template
    assert "zombie_force_liquidation_records" in template



def test_zombie_force_liquidation_records_hide_raw_response_column():
    template = Path("templates/abnormal_wicks.html").read_text()

    zombie_records_index = template.index("<strong>僵尸单强平操作记录</strong>")
    trade_records_index = template.index("<strong>交易实验交易记录</strong>")
    zombie_section = template[zombie_records_index:trade_records_index]

    assert "<th>raw_response</th>" not in zombie_section
    assert "row.raw_response" not in zombie_section
    assert 'colspan="10"' in zombie_section

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


def test_filled_orders_exit_reason_tip_includes_zombie_force_liquidation():
    template = Path("templates/abnormal_wicks.html").read_text()

    tip_index = template.index("止盈/止损原因显示规则")
    zombie_index = template.index("僵尸强平（僵尸单强平操作记录）", tip_index)
    structural_index = template.index("结构止损（止损记录）", tip_index)

    assert tip_index < zombie_index < structural_index



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


def test_holding_reduction_metrics_have_threshold_highlight_classes():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "<th>ATR(14)</th>" in template
    assert "price_drawdown_percent >= 3.5" not in template
    assert "price_drawdown_percent >= 3" not in template
    assert "reduction-pnl-profit" in template
    assert "reduction-pnl-strong-profit" in template
    assert "pnl_r_multiple >= 2" in template
    assert "pnl_r_multiple >= 1" in template


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




def test_holding_increase_refresh_updates_module_without_page_reload():
    template = Path("templates/abnormal_wicks.html").read_text()
    init_index = template.index("function renderHoldingIncreaseSummary")
    refresh_index = template.index("/api/holding-increase/refresh-pretrigger")

    assert 'id="holding-increase-checks-body"' in template
    assert 'id="holding-increase-records-body"' in template
    assert "renderHoldingIncreaseSummary(payload)" in template
    assert "已更新加仓模块" in template
    assert "window.location.reload()" not in template[init_index:refresh_index + 500]


def test_trailing_reduction_refresh_updates_module_without_page_reload():
    template = Path("templates/abnormal_wicks.html").read_text()
    init_index = template.index("function renderTrailingReductionSummary")
    refresh_index = template.index("/api/trailing-reduction/refresh-pretrigger")

    assert 'id="trailing-reduction-round-note"' in template
    assert 'id="trailing-reduction-pretrigger-chips"' in template
    assert 'id="trailing-reduction-checks-body"' in template
    assert 'id="trailing-reduction-records-body"' in template
    assert "renderTrailingReductionSummary(payload)" in template
    assert "window.location.reload()" not in template[init_index:refresh_index + 500]


def test_trailing_reduction_current_price_is_red_below_lowest():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "lowest_15m_low_decimal > 0 and current_price_decimal < lowest_15m_low_decimal" in template
    assert "function trailingReductionCurrentPriceCell" in template
    assert "currentPrice < lowest" in template
    assert "reduction-current-price-danger" in template


def test_holding_increase_tags_have_requested_colors():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert ".reduction-tag-stale-pretrigger" in template
    assert ".reduction-tag-increase-completed" in template
    assert "row.tag == '已完成第一次加仓'" in template
    assert "tag === '已完成第一次加仓'" in template
    assert "latest_pretrigger_round" in template
    assert 'reduction-tag-stale-pretrigger">触发轮次' in template


def test_holding_reduction_rule5_lifecycle_tag_is_gray():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert ".reduction-tag-rule5-triggered" in template
    assert "tag == '已触发深度弱势'" in template
    assert 'reduction-tag-rule5-triggered">{{ tag }}' in template

def test_score_page_includes_ma20_skip_warning_at_top():
    template = Path("templates/abnormal_wicks.html").read_text()

    score_header_index = template.index("<h2>评分系统</h2>")
    warning_index = template.index("MA20缺失跳过提示")
    score_tabs_index = template.index('class="toolbar" style="padding-top:0;"')

    assert score_header_index < warning_index < score_tabs_index
    assert "scoring_ma20_skip_record.missing_symbols" in template
    assert "scoring_ma20_skip_record.created_at" in template
    assert "避免卡住整个评分系统" in template


def test_score_symbol_error_warning_uses_current_score_round_only():
    web_app_source = Path("web_app.py").read_text()

    assert "scoring_symbol_error_round_ts = score_total_round_ts" in web_app_source
    assert "scoring_symbol_errors = scoring.get_symbol_errors_for_round(score_total_round_ts)" in web_app_source
    assert "scoring_symbol_error_round_ts, scoring_symbol_errors = scoring.get_latest_symbol_error_round()" not in web_app_source


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


def test_trading_trade_records_highlight_current_round_new_open_symbols():
    template = Path("templates/abnormal_wicks.html").read_text()

    trade_records_index = template.index("<strong>交易实验交易记录</strong>")
    highlight_index = template.index('class="new-open-symbol-badge" title="本轮新开仓"')

    assert trade_records_index < highlight_index
    assert "trading_new_open_symbols" in template
    assert "本轮新开仓 symbol 会用红色徽标高亮" in template


def test_dynamic_profit_protection_has_scoped_refresh_button():
    template = Path("templates/abnormal_wicks.html").read_text()

    section_index = template.index('<div id="holding-module-dynamic-profit-protection"')
    button_index = template.index('id="refresh-dynamic-profit-protection"', section_index)
    status_index = template.index('id="dynamic-profit-protection-refresh-status"', section_index)
    endpoint_index = template.index("fetch('/api/dynamic-profit-protection/summary'")
    trailing_index = template.index('<div id="holding-module-trailing-stop"')

    assert section_index < button_index < status_index < trailing_index
    assert "仅刷新动态利润保护板块与动态利润保护记录" in template
    assert "renderDynamicProfitProtectionSummary(payload);" in template
    assert endpoint_index > button_index


def test_web_page_creates_missing_db_parent_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "nested" / "missing" / "klines.db"
        import web_app

        original_db_path = web_app.DB_PATH
        web_app.DB_PATH = str(db_path)
        try:
            response = web_app.app.test_client().get("/safety/abnormal-wicks")
        finally:
            web_app.DB_PATH = original_db_path

    assert response.status_code == 200


def test_experiment_equity_trend_rows_returns_empty_for_malformed_database():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "klines.db"
        db_path.write_bytes(b"not a sqlite database")
        import web_app

        original_db_path = web_app.DB_PATH
        web_app.DB_PATH = str(db_path)
        try:
            rows = web_app._experiment_equity_trend_rows(0)
        finally:
            web_app.DB_PATH = original_db_path

    assert rows == []


def test_market_filter_uses_collapsible_recent_records_ui():
    template = Path("templates/abnormal_wicks.html").read_text()

    section_index = template.index('<section id="tab-market-filter"')
    module_index = template.index("独立市场过滤模块（每15分钟执行）", section_index)
    collapsible_index = template.index('class="collapsible-section is-collapsed"', section_index)
    button_index = template.index('class="collapsible-toggle"', module_index)
    table_index = template.index("market_filter_results", button_index)

    assert collapsible_index < module_index < button_index < table_index
    assert "只显示最近7天内记录" in template[section_index:table_index]
    assert 'class="{% if loop.index > 10 %}collapsed-extra{% endif %}"' in template[section_index:template.index('</section>', section_index)]


def test_openable_section_highlights_current_round_open_block_notice():
    template = Path("templates/abnormal_wicks.html").read_text()

    openable_index = template.index("本轮可开仓symbol情况")
    notice_index = template.index("open_block_notice", openable_index)
    text_index = template.index("本轮禁止新开仓", notice_index)
    table_index = template.index("openable_symbols", text_index)

    assert notice_index < text_index < table_index


def test_market_filter_includes_dynamic_add_position_threshold_ui():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section_index = template.index('id="tab-market-filter"')
    module_index = template.index("动态加仓阈值（每15分钟执行）", section_index)
    assert "2R成功率 = 触发笔数 / 样本笔数" in template[module_index:]
    assert "dynamic_add_position_threshold_results" in template[module_index:]
