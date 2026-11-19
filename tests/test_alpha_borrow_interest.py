import sqlite3

import pytest
import alpha_borrow_interest as borrow


def payload(rate='0.00012345'):
    return {'code': '000000', 'data': [
        {'assetName': 'BTC', 'specs': [
            {'vipLevel': 1, 'dailyInterestRate': '1'},
            {'vipLevel': 0, 'dailyInterestRate': rate}]}]}


def test_only_regular_user_rates():
    assert borrow.parse_rates(payload())['BTC'][0] == '0.00012345'


@pytest.mark.parametrize('value', ['NaN', '-1', 'Infinity', 'invalid'])
def test_invalid_rates_rejected(value):
    with pytest.raises(ValueError):
        borrow.parse_rates(payload(value))


def test_hourly_upsert_and_preserve_history(tmp_path):
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return payload()
    class Session:
        def get(self, url, timeout):
            assert url == borrow.API_URL
            assert timeout == 30
            return Response()
    path = str(tmp_path / 'alpha.db')
    for timestamp in (3600001, 3600100, 7200000):
        assert borrow.collect_once(path, session=Session(), observed_at=timestamp) == 1
    with sqlite3.connect(path) as conn:
        assert conn.execute(f'SELECT symbol, vip_level, observed_at, daily_interest_rate, annual_interest_rate FROM {borrow.TABLE} ORDER BY observed_at').fetchall() == [
            ('BTC', 0, 3600000, '0.00012345', '0.04505925'), ('BTC', 0, 7200000, '0.00012345', '0.04505925')]


def test_no_vip_zero_is_not_silently_saved():
    with pytest.raises(ValueError):
        borrow.parse_rates({'code': '000000', 'data': [{'vipLevel': 1}]})
