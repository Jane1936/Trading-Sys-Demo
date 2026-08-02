import inspect

import app


def test_profit_task_runs_break_even_before_partial_take_profit_each_loop():
    source = inspect.getsource(app.start_break_even_take_profit_task)

    assert "for _ in range(5)" not in source
    assert "while True:" in source
    assert source.index("result = strategy.run_round()") < source.index(
        "partial_result = partial_strategy.run_round"
    )
    assert "time.sleep(60)" in source
