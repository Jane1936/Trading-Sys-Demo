from html.parser import HTMLParser
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


def test_zombie_force_liquidation_copy_describes_24h_hard_limit():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")

    assert "持仓时间一旦达到24小时" in template
    assert "无论是否已有保本止盈保护" in template
    assert "reduceOnly 市价单将当前持仓全部平仓" in template

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


def test_filled_orders_query_supports_explicit_time_range_before_common_filter():
    template = Path("templates/abnormal_wicks.html").read_text()

    days_query_index = template.index('id="query-filled-sell-orders"')
    start_index = template.index('id="filled-orders-start-time"')
    end_index = template.index('id="filled-orders-end-time"')
    range_query_index = template.index('id="query-filled-orders-by-time"')
    score_filter_index = template.index('id="filled-orders-score-band"')

    assert days_query_index < start_index < end_index < range_query_index < score_filter_index
    assert 'type="datetime-local"' in template
    assert "new URLSearchParams({ start_time: String(startTime), end_time: String(endTime) })" in template
    assert "startTime >= endTime" in template


def test_holding_reduction_metrics_have_threshold_highlight_classes():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "<th>ATR(14)</th>" in template
    assert "price_drawdown_percent >= 3.5" not in template
    assert "price_drawdown_percent >= 3" not in template
    assert "reduction-pnl-profit" in template
    assert "reduction-pnl-strong-profit" in template
    assert "pnl_r_multiple >= 2" in template
    assert "pnl_r_multiple >= 1" in template


def test_reduction_stop_failure_liquidation_records_follow_reduction_records():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")

    reduction_index = template.index("减仓操作记录")
    liquidation_index = template.index("重挂止损失败后强平记录")
    assert liquidation_index > reduction_index
    assert "holding_reduction_stop_failure_liquidations" in template
    assert "近7天暂无重挂止损失败后强平记录" in template


def test_strategy_subpanels_are_not_nested_inside_holding_module_panels():
    class StrategyPanelNestingParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack = []
            self.nested_panels = []

        def handle_starttag(self, tag, attrs):
            attributes = dict(attrs)
            classes = set(attributes.get("class", "").split())
            if "strategy-subpanel" in classes:
                holding_parent = next(
                    (
                        item_id
                        for item_tag, item_id, item_classes in reversed(self.stack)
                        if item_tag == "div" and "holding-module-panel" in item_classes
                    ),
                    None,
                )
                if holding_parent:
                    self.nested_panels.append((attributes.get("id"), holding_parent))
            self.stack.append((tag, attributes.get("id"), classes))

        def handle_endtag(self, tag):
            for index in range(len(self.stack) - 1, -1, -1):
                if self.stack[index][0] == tag:
                    del self.stack[index:]
                    return

    parser = StrategyPanelNestingParser()
    parser.feed(Path("templates/abnormal_wicks.html").read_text(encoding="utf-8"))

    assert parser.nested_panels == []


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


def test_partial_take_profit_displays_merged_error_records():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")

    assert "分批止盈卖出错误记录" in template
    assert 'id="partial-take-profit-errors-body"' in template
    assert "renderPartialTakeProfitErrors(payload.errors || [])" in template
    assert "row.source" in template


def test_dynamic_profit_protection_displays_updated_profit_tiers():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert "浮盈到达过(2R, 3R] 回撤≥40%" in template
    assert "浮盈到达过(3R, 4R] 回撤≥30%" in template
    assert "浮盈到达过4R以上 回撤≥20%" in template
    assert "浮盈到达过(2R, 4R]" not in template
    assert "浮盈到达过7R以上" not in template


def test_dynamic_profit_protection_displays_highest_profit_time():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert template.count("最高浮盈出现时间") >= 2
    assert "row.highest_profit_at|fmt_ms_datetime" in template
    assert "formatMsDatetime(row.highest_profit_at)" in template


def test_dynamic_profit_protection_displays_historical_highest_profit_before_time():
    template = Path("templates/abnormal_wicks.html").read_text()

    assert template.count("历史最高浮盈") >= 2
    assert template.count("<th>历史最高浮盈</th><th>最高浮盈出现时间</th>") == 2
    assert "row.highest_unrealized_pnl" in template


def test_trailing_stop_action_records_show_atr_and_volatility_without_total_score():
    template = Path("templates/abnormal_wicks.html").read_text()
    section_start = template.index("<strong>移动追踪止盈操作记录</strong>")
    section_end = template.index("</table>", section_start)
    section = template[section_start:section_end]

    assert "<th>ATR(14)</th>" in section
    assert "<th>波动率</th>" in section
    assert "<th>价格回撤 / 阈值</th>" in section
    assert "盈利回撤幅度" not in section
    assert "total_score" not in section
    assert "row.atr14" in section
    assert "row.volatility" in section
    assert "row.price_drawdown }} / {{ row.drawdown_threshold" in section
    assert "row.current_profit_drawdown" not in section

    renderer_start = template.index("function renderTrailingStopRecords(rows)")
    renderer_end = template.index("function renderTrailingStopSummary(payload)", renderer_start)
    renderer = template[renderer_start:renderer_end]
    assert "row.price_drawdown)} / ${escapeHtml(row.drawdown_threshold)" in renderer
    assert "row.current_profit_drawdown" not in renderer


def test_trailing_stop_checks_display_holding_hours():
    template = Path("templates/abnormal_wicks.html").read_text()
    section_start = template.index("<strong>移动追踪止盈规则：1分钟扫描结果</strong>")
    section_end = template.index("<strong>移动追踪止盈操作记录</strong>", section_start)
    section = template[section_start:section_end]

    assert "<th>持仓时间（小时）</th>" in section
    assert "row.holding_hours" in section
    assert 'colspan="18">暂无移动追踪止盈扫描结果' in section


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
    assert "{% if loop.index > 10 %} collapsed-extra{% endif %}" in template[section_index:template.index('</section>', section_index)]


def test_market_filter_includes_weak_market_profit_adjustment_ui():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section_index = template.index('<section id="tab-market-filter"')
    module_index = template.index("弱势市场止盈动态调整（每15分钟执行）", section_index)
    assert template.index("weak_market_profit_adjustment_results", module_index) > module_index
    assert "1.4R" in template[module_index:]


def test_openable_section_highlights_current_round_open_block_notice():
    template = Path("templates/abnormal_wicks.html").read_text()

    openable_index = template.index("本轮可开仓symbol情况")
    notice_index = template.index("open_block_notice", openable_index)
    text_index = template.index("本轮禁止新开仓", notice_index)
    table_index = template.index("openable_symbols", text_index)

    assert notice_index < text_index < table_index


def test_openable_table_shows_previous_round_score_before_distance_ratio():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('id="strategy-openable"'):]

    previous_score_index = section.index("<th>上一轮总分</th>")
    previous_band_index = section.index("<th>上一轮评分档位</th>")
    distance_ratio_index = section.index("<th>distance_ratio</th>")

    assert previous_score_index < previous_band_index < distance_ratio_index
    assert "row.previous_total_score" in section
    assert "row.previous_score_band" in section
    assert "上一轮评分档位低于本轮评分档位，则本轮最终不可开仓" in section


def test_market_filter_includes_dynamic_add_position_threshold_ui():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section_index = template.index('id="tab-market-filter"')
    module_index = template.index("动态加仓阈值（每15分钟执行）", section_index)
    assert "2R成功率 = 触发笔数 / 样本笔数" in template[module_index:]
    assert "dynamic_add_position_threshold_results" in template[module_index:]


def test_increase_condition_copy_uses_dynamic_round_threshold():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('aria-label="加仓条件模块"'):]

    assert "条件1：当前未变现盈利 ≥ 本轮加仓阈值" in section
    assert "加仓模块每轮执行前" in section
    assert "动态加仓阈值（每15分钟执行）" in section
    assert "当前未变现盈利 ≥ 1.3R" not in section


def test_market_filter_highlights_latest_decisions_and_permission_statuses():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('id="tab-market-filter"'):]

    assert section.count("latest-decision-row") >= 5
    assert section.count("latest-decision-badge") >= 5
    assert section.count("'success' if row.allow_") >= 3
    assert section.count("'danger'") >= 3


def test_add_position_modules_use_light_purple_theme():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('id="tab-market-filter"'):]

    permission_index = section.index("加仓权限（每15分钟执行）")
    threshold_index = section.index("动态加仓阈值（每15分钟执行）")
    assert section.rfind("purple-module", 0, permission_index) > 0
    assert section.rfind("purple-module", 0, threshold_index) > permission_index
    assert ".purple-module .collapsible-header" in template


def test_feature_flags_page_contains_all_rule_weight_controls_and_save_logic():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('id="tab-feature-flags"'):]

    assert "评分规则权重" in section
    assert 'id="score-weight-form"' in section
    assert 'data-rule-id="{{ rule.rule_id }}"' in section
    assert "保存全部权重" in section
    assert "fetch('/api/scoring-rule-weights'" in section


def test_feature_flags_page_contains_scoring_rule_election_below_weights():
    template = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    section = template[template.index('id="tab-feature-flags"'):]

    assert section.index("评分规则权重") < section.index("评分规则选举")
    assert 'id="score-election-form"' in section
    assert "必须" in section and "可选" in section and "无要求" in section
    assert 'id="score-election-optional-min"' in section
    assert "fetch('/api/scoring-rule-election'" in section
