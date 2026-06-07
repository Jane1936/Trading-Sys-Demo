from pathlib import Path


def test_trading_position_snapshots_render_after_trade_records():
    template = Path("templates/abnormal_wicks.html").read_text()

    trade_records_index = template.index("<strong>交易实验交易记录</strong>")
    position_snapshots_index = template.index("<strong>交易实验持仓快照</strong>")
    error_records_index = template.index("<strong>交易实验错误信息记录</strong>")

    assert trade_records_index < position_snapshots_index < error_records_index
