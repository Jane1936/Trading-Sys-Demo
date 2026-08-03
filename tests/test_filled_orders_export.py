from io import BytesIO
from zipfile import ZipFile
from xml.etree import ElementTree

import web_app


def test_filled_orders_export_downloads_current_rows_as_xlsx():
    client = web_app.app.test_client()
    response = client.post(
        "/api/account/filled-orders/export",
        json={
            "orders": [
                {
                    "time": 1710000000000,
                    "symbol": "BTCUSDT",
                    "open_score_band": "强趋势",
                    "open_total_score": 82,
                    "exit_reason": "分批止盈",
                    "side": "SELL",
                    "order_id": "123",
                    "price": "68000.5",
                    "quantity": "0.01",
                    "quote_quantity": "680.005",
                    "realized_pnl": "12.5",
                    "commission": "0.2",
                    "commission_asset": "USDT",
                    "maker": True,
                    "trade_id": "456",
                }
            ]
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "attachment" in response.headers["Content-Disposition"]
    with ZipFile(BytesIO(response.data)) as workbook:
        sheet = ElementTree.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows = sheet.findall(".//x:sheetData/x:row", namespace)
    assert [cell.find("x:is/x:t", namespace).text for cell in rows[0]] == [
        label for label, _ in web_app.FILLED_ORDER_EXPORT_COLUMNS
    ]
    assert rows[1][1].find("x:is/x:t", namespace).text == "BTCUSDT"
    assert rows[1][4].find("x:is/x:t", namespace).text == "分批止盈"
    assert rows[1][13].find("x:is/x:t", namespace).text == "是"


def test_filled_orders_export_rejects_empty_rows():
    response = web_app.app.test_client().post(
        "/api/account/filled-orders/export", json={"orders": []}
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "没有可导出的已成交订单数据"
