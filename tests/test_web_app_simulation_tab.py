from pathlib import Path


TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "templates" / "abnormal_wicks.html"


def test_simulation_tab_owns_paper_trading_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    strategy_start = template.index('<section id="tab-strategy"')
    simulation_start = template.index('<section id="tab-simulation"')
    feature_flags_start = template.index('<section id="tab-feature-flags"')
    strategy_section = template[strategy_start:simulation_start]
    simulation_section = template[simulation_start:feature_flags_start]

    assert 'data-tab="tab-simulation">模拟盘数据</button>' in template
    assert strategy_start < simulation_start < feature_flags_start
    assert "持仓评分系统" not in strategy_section
    assert "已成交订单分析" not in strategy_section
    assert "账户信息" not in strategy_section
    assert 'data-strategy-tab="strategy-openable"' in strategy_section
    assert 'data-strategy-tab="strategy-cooldown"' in strategy_section
    assert 'data-simulation-tab="strategy-holding-score">持仓评分系统' in simulation_section
    assert 'data-simulation-tab="strategy-filled-orders">已成交订单分析' in simulation_section
    assert 'data-simulation-tab="strategy-account">账户信息' in simulation_section


def test_simulation_subtab_script_targets_only_simulation_panels() -> None:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "querySelectorAll('.bookmark-tab[data-simulation-tab]')" in template
    assert "querySelectorAll('.simulation-subpanel')" in template
    assert "document.getElementById(b.dataset.simulationTab)" in template
    assert "b.dataset.simulationTab === 'strategy-account'" in template
