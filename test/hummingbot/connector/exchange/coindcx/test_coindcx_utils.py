
from decimal import Decimal

from hummingbot.connector.exchange.coindcx import coindcx_utils


def test_is_exchange_information_valid_true():
    info = {"status": "active"}
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
    # Ensure default fees are set and have Decimal values
    fees = coindcx_utils.DEFAULT_FEES
    assert isinstance(fees.maker_percent_fee_decimal, Decimal)
    assert isinstance(fees.taker_percent_fee_decimal, Decimal)


def test_config_keys_and_ecode():
    # Ensure KEYS object exists and connector name is present
    assert hasattr(coindcx_utils, "KEYS")
    assert getattr(coindcx_utils.KEYS, "connector", "") == "coindcx"
    # hb_pair_to_coindcx_pair supports different ecodes
    assert coindcx_utils.hb_pair_to_coindcx_pair("BTC-USDT", ecode="I") == "I-BTC_USDT"


def test_coindcx_pair_to_hb_pair_unknown_quote():
    # If quote not in known list, return original
    assert coindcx_utils.coindcx_pair_to_hb_pair("UNKNOWNPAIR") == "UNKNOWNPAIR"


def test_config_map_reflection_and_construct():
    from hummingbot.connector.exchange.coindcx.coindcx_utils import CoinDCXConfigMap

    # Access model config
    assert CoinDCXConfigMap.model_config.title == "coindcx"

    # Construct a model instance via model_construct (no validation)
    cm = CoinDCXConfigMap.model_construct()
    assert hasattr(cm, "coindcx_api_key")
