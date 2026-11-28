
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


def test_proxy_url_field_is_an_optional_secret_connect_key():
    from hummingbot.connector.exchange.coindcx.coindcx_utils import CoinDCXConfigMap

    field = CoinDCXConfigMap.model_fields["coindcx_proxy_url"]
    assert field.default.get_secret_value() == ""
    extra = field.json_schema_extra
    assert extra["is_secure"] is True
    assert extra["prompt_on_new"] is True
    # Only connect keys reach the connector constructor; with False the proxy would be saved and then ignored.
    assert extra["is_connect_key"] is True


def test_proxy_url_reaches_the_connector_arguments():
    from hummingbot.client.config.config_helpers import ClientConfigAdapter, api_keys_from_connector_config_map
    from hummingbot.connector.exchange.coindcx.coindcx_utils import CoinDCXConfigMap

    config = ClientConfigAdapter(CoinDCXConfigMap(
        coindcx_api_key="key", coindcx_api_secret="secret", coindcx_proxy_url="http://user:pass@proxy.local:3128"))

    assert api_keys_from_connector_config_map(config) == {
        "coindcx_api_key": "key",
        "coindcx_api_secret": "secret",
        "coindcx_proxy_url": "http://user:pass@proxy.local:3128",
    }


def test_proxy_url_is_blank_for_a_config_saved_without_it():
    from hummingbot.client.config.config_helpers import ClientConfigAdapter, api_keys_from_connector_config_map
    from hummingbot.connector.exchange.coindcx.coindcx_utils import CoinDCXConfigMap

    config = ClientConfigAdapter(CoinDCXConfigMap(coindcx_api_key="key", coindcx_api_secret="secret"))

    assert api_keys_from_connector_config_map(config)["coindcx_proxy_url"] == ""


def test_default_pair_code_guesses_the_usual_route():
    assert coindcx_utils.default_pair_code("ETH-INR") == "I-ETH_INR"
    assert coindcx_utils.default_pair_code("BTC-USDT") == "B-BTC_USDT"


def test_timestamp_in_seconds_reads_every_format_coindcx_uses():
    assert coindcx_utils.timestamp_in_seconds(1795675620118) == 1795675620.118  # milliseconds
    assert coindcx_utils.timestamp_in_seconds(1545896665076.92) == 1545896665.07692  # a trade time with a fraction
    assert coindcx_utils.timestamp_in_seconds("1795675620118") == 1795675620.118
    assert coindcx_utils.timestamp_in_seconds(1795675620) == 1795675620.0  # seconds already
    assert abs(coindcx_utils.timestamp_in_seconds("2026-11-26T06:47:00.118Z") - 1795675620.118) < 1e-6
    assert abs(coindcx_utils.timestamp_in_seconds("2026-11-26T06:47:00.118") - 1795675620.118) < 1e-6  # UTC
    for unreadable in (None, "", "yesterday", 0, -5, True, [1]):
        assert coindcx_utils.timestamp_in_seconds(unreadable) is None
