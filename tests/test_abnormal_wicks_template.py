from pathlib import Path


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
