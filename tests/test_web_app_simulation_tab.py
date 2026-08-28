from pathlib import Path


TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "templates" / "abnormal_wicks.html"


def test_simulation_tab_owns_paper_trading_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    strategy_start = template.index('<section id="tab-strategy"')
    simulation_start = template.index('<section id="tab-simulation"')
    live_start = template.index('<section id="tab-live"')
    feature_flags_start = template.index('<section id="tab-feature-flags"')
    strategy_section = template[strategy_start:simulation_start]
    simulation_section = template[simulation_start:live_start]

    assert 'data-tab="tab-simulation">模拟盘数据</button>' in template
    assert strategy_start < simulation_start < live_start < feature_flags_start
    assert "持仓评分系统" not in strategy_section
    assert "已成交订单分析" not in strategy_section
    assert "账户信息" not in strategy_section
    assert 'data-strategy-tab="strategy-openable"' in strategy_section
    assert 'data-strategy-tab="strategy-cooldown"' in strategy_section
    assert 'data-simulation-tab="strategy-holding-score">持仓评分系统' in simulation_section
    assert 'data-simulation-tab="strategy-filled-orders">已成交订单分析' in simulation_section
    assert 'data-simulation-tab="strategy-account">账户信息' in simulation_section


def test_live_tab_has_production_filled_orders_and_account_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    live_start = template.index('<section id="tab-live"')
    feature_flags_start = template.index('<section id="tab-feature-flags"')
    live_section = template[live_start:feature_flags_start]

    assert 'data-tab="tab-live">实盘数据</button>' in template
    assert 'data-live-tab="live-filled-orders">已成交订单分析' in live_section
    assert 'data-live-tab="live-account">账户信息' in live_section
    assert '/api/live/account/balance' in template
    assert '/api/live/account/filled-orders' in template
    assert 'https://fapi.binance.com' in live_section


def test_live_holding_score_tabs_target_separate_module_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    live_start = template.index('<section id="tab-live"')
    feature_flags_start = template.index('<section id="tab-feature-flags"')
    live_section = template[live_start:feature_flags_start]
    module_names = ("stop-loss", "reduction", "increase", "portfolio-risk")

    for module_name in module_names:
        panel_id = f"live-holding-module-{module_name}"
        assert f'data-holding-module-tab="{panel_id}"' in live_section
        assert f'id="{panel_id}" class="holding-module-panel' in live_section

    assert live_section.count('class="holding-module-panel active"') == 1
    assert live_section.count('class="holding-module-panel"') == 4
    assert "live_high_frequency_modules" in live_section


def test_live_holding_modules_render_full_operation_record_tables() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    live_start = template.index('<section id="tab-live"')
    feature_flags_start = template.index('<section id="tab-feature-flags"')
    live_section = template[live_start:feature_flags_start]

    assert "止损操作记录（实盘）" in live_section
    assert "减仓操作记录（实盘）" in live_section
    assert "加仓操作记录（实盘，最近7天）" in live_section
    assert "{% for row in live_holding_stop_loss_records %}" in live_section
    assert "{% for row in live_holding_reduction_records %}" in live_section
    assert "{% for row in live_holding_increase_records %}" in live_section
    assert live_section.count("记录存储于 real_trading_core.db") == 3
    assert "row.realized_pnl" in live_section
    assert "row.market_order_id" in live_section
    assert "row.required_margin_usdt" in live_section


def test_holding_module_tab_script_scopes_updates_to_current_account_panel() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "b.closest('.simulation-subpanel, .live-subpanel')" in template
    assert "container.querySelectorAll('.holding-module-tab[data-holding-module-tab]')" in template
    assert "container.querySelectorAll('.holding-module-panel')" in template


def test_feature_flags_page_exposes_live_trading_switch() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "实盘交易系统" in template
    assert "real_trading_system" in template
    assert "实盘交易系统”默认关闭" in template


def test_simulation_subtab_script_targets_only_simulation_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "querySelectorAll('.bookmark-tab[data-simulation-tab]')" in template
    assert "querySelectorAll('.simulation-subpanel')" in template
    assert "document.getElementById(b.dataset.simulationTab)" in template
    assert "b.dataset.simulationTab === 'strategy-account'" in template
