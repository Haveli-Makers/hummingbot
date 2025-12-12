from decimal import Decimal

from hummingbot.connector.exchange.coindcx import coindcx_utils as utils


def test_coindcx_pair_conversions_and_validity():
    assert utils.coindcx_pair_to_hb_pair("B-BTC_USDT") == "BTC-USDT"
    assert utils.coindcx_pair_to_hb_pair("BTCUSDT") == "BTC-USDT"
    assert utils.coindcx_pair_to_hb_pair("UNKNOWNPAIR") == "UNKNOWNPAIR"

    assert utils.hb_pair_to_coindcx_symbol("BTC-USDT") == "BTCUSDT"
    assert utils.hb_pair_to_coindcx_pair("BTC-USDT", ecode="B") == "B-BTC_USDT"

    assert utils.is_exchange_information_valid({"status": "active"}) is True
    assert utils.is_exchange_information_valid({"status": "inactive"}) is False
