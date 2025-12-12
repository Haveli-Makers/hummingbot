import asyncio
from decimal import Decimal

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType, TradeType


def make_instance():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    inst.api_key = "k"
    inst.secret_key = "s"
    inst._domain = "test"
    inst._trading_required = True
    inst._trading_pairs = ["BTC-USDT"]
    return inst


def test_properties_and_helpers():
    inst = make_instance()
    assert inst.name == "coindcx"
    assert inst.domain == "test"
    assert inst.client_order_id_prefix != ""
    assert inst.supported_order_types()


def test_authenticator_and_factory_and_data_sources():
    inst = make_instance()
    # authenticator should return an object (CoinDCXAuth) when time provider present
    inst._time_synchronizer = type("T", (), {"time": lambda self: 1.0})()
    auth = inst.authenticator
    assert auth is not None

    inst._web_assistants_factory = object()
    inst._auth = object()
    ob_ds = inst._create_order_book_data_source()
    us_ds = inst._create_user_stream_data_source()
    assert ob_ds is not None
    assert us_ds is not None


def test_get_fee_and_place_cancel_and_place_order(monkeypatch):
    inst = make_instance()

    inst.estimate_fee_pct = lambda is_maker: Decimal("0.001")
    fee = inst._get_fee("BTC", "USDT", OrderType.LIMIT, TradeType.BUY, Decimal("1"))
    assert fee is not None

    class Tracked:
        exchange_order_id = "exid"

    async def fake_post(path_url=None, data=None, is_auth_required=False):
        return [{"id": "ex123", "created_at": 1600000000000}]

    inst._api_post = fake_post

    async def run_place():
        o_id, t = await inst._place_order("cid", "BTC-USDT", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("1"))
        assert o_id is not None

        res = await inst._place_cancel("cid", Tracked())
        assert res is True

    # Prevent exchange_base logic by overriding symbol resolution
    async def fake_symbol(trading_pair: str):
        return "BTCUSDT"

    inst.exchange_symbol_associated_to_pair = fake_symbol
    asyncio.run(run_place())
