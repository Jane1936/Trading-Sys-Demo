from pathlib import Path
import re

import app as worker_app
import web_app


def test_web_factory_runs_unified_schema_barrier(monkeypatch):
    calls = []
    monkeypatch.setattr(worker_app, "initialize_worker_databases", lambda: calls.append("barrier"))

    created_app = web_app.create_app()

    assert created_app is web_app.app
    assert calls == ["barrier"]


def test_deployed_web_process_uses_schema_barrier_factory():
    entrypoint = Path("docker_entrypoint.sh").read_text(encoding="utf-8")

    assert "web_app:create_app()" in entrypoint


def test_standalone_process_entries_use_unified_schema_barrier():
    for entrypoint in ("app.py", "collector.py", "trading_experiment.py"):
        source = Path(entrypoint).read_text(encoding="utf-8")
        main_block = source.rsplit('if __name__ == "__main__":', maxsplit=1)[1]

        assert "initialize_worker_databases()" in main_block


def test_runtime_modules_do_not_defensively_call_self_init_tables():
    runtime_sources = list(Path(".").glob("*.py"))

    offenders = {
        str(path): match.start()
        for path in runtime_sources
        if (match := re.search(r"\bself\.init_tables\(\)", path.read_text(encoding="utf-8")))
    }

    assert offenders == {}
