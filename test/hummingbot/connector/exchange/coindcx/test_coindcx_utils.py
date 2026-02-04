
from decimal import Decimal

from hummingbot.connector.exchange.coindcx import coindcx_utils


def test_is_exchange_information_valid_true():
    info = {
        "status": "active",
        "min_quantity": 0.001,
        "max_quantity": 1000000
    }
    assert coindcx_utils.is_exchange_information_valid(info)


def test_is_exchange_information_valid_false():
    info = {"status": "inactive"}
    assert not coindcx_utils.is_exchange_information_valid(info)


def test_pair_conversions():
    assert coindcx_utils.coindcx_pair_to_hb_pair("B-BTC_USDT") == "BTC-USDT"
    assert coindcx_utils.coindcx_pair_to_hb_pair("BTCUSDT") == "BTC-USDT"
    assert coindcx_utils.hb_pair_to_coindcx_symbol("BTC-USDT") == "BTCUSDT"
    assert coindcx_utils.hb_pair_to_coindcx_pair("BTC-USDT", ecode="B") == "B-BTC_USDT"


def test_default_fees_object():
    fees = coindcx_utils.DEFAULT_FEES
    assert isinstance(fees.maker_percent_fee_decimal, Decimal)
    assert isinstance(fees.taker_percent_fee_decimal, Decimal)


def test_config_keys_and_ecode():
    assert hasattr(coindcx_utils, "KEYS")
    assert getattr(coindcx_utils.KEYS, "connector", "") == "coindcx"
    assert coindcx_utils.hb_pair_to_coindcx_pair("BTC-USDT", ecode="I") == "I-BTC_USDT"


def test_coindcx_pair_to_hb_pair_unknown_quote():
    assert coindcx_utils.coindcx_pair_to_hb_pair("UNKNOWNPAIR") == "UNKNOWNPAIR"


def test_config_map_reflection_and_construct():
    from hummingbot.connector.exchange.coindcx.coindcx_utils import CoinDCXConfigMap

    mc = CoinDCXConfigMap.model_config
    title = None
    if hasattr(mc, "title"):
        title = mc.title
    elif isinstance(mc, dict):
        title = mc.get("title")
    assert title == "coindcx"

    cm = CoinDCXConfigMap.model_construct()
    has_field = hasattr(cm, "coindcx_api_key") or (hasattr(cm, "model_fields") and "coindcx_api_key" in cm.model_fields)
    assert has_field
