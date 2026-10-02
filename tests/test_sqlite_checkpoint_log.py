import json

import app


def test_checkpoint_log_uses_configured_log_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(app, "SQLITE_CHECKPOINT_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(app, "_checkpoint_last_log_cleanup_date", None)

    app._write_checkpoint_log({"event": "sqlite_checkpoint"})

    log_files = list(tmp_path.glob("sqlite-checkpoint-*.jsonl"))
    assert len(log_files) == 1
    assert json.loads(log_files[0].read_text(encoding="utf-8")) == {
        "event": "sqlite_checkpoint"
    }
