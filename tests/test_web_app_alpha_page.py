from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import alpha_observer
from web_app import app


def test_empty_alpha_page_reports_database_actually_read():
    status = alpha_observer.AlphaDatabaseStatus(
        path="/app/custom/alpha.db",
        size_bytes=4096,
        total_rows=0,
        snapshot_count=0,
    )
    with (
        patch("web_app.alpha_observer.latest_snapshot", return_value=(None, [])),
        patch("web_app.alpha_observer.database_status", return_value=status),
    ):
        response = app.test_client().get("/market/alpha")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "/app/custom/alpha.db" in body
    assert "表内共有" in body
    assert "ALPHA_DB_PATH" in body


def test_populated_alpha_page_marks_standalone_panel_visible():
    row = {
        "symbol": "ALPHA",
        "name": "Alpha",
        "chain_id": "56",
        "contract_address": "0x1",
        "icon_url": "",
        "volume_24h": "123.4",
        "market_cap": "567.8",
        "observed_at": 1_789_399_183_493,
    }
    status = alpha_observer.AlphaDatabaseStatus(
        path="/app/data/alpha.db",
        size_bytes=4096,
        total_rows=1,
        snapshot_count=1,
    )
    with (
        patch(
            "web_app.alpha_observer.latest_snapshot",
            return_value=(row["observed_at"], [row]),
        ),
        patch("web_app.alpha_observer.database_status", return_value=status),
    ):
        response = app.test_client().get("/market/alpha")

    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert '<section class="panel alpha-panel active">' in body
    assert "ALPHA" in body
    assert "2天活跃度" in body
    assert "5天活跃度" in body
