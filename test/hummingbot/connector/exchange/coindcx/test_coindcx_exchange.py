from decimal import Decimal

import pytest

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder


@pytest.mark.asyncio
async def test_order_type_mappings_are_consistent():
    assert CoindcxExchange.coindcx_order_type(OrderType.MARKET) == CONSTANTS.ORDER_TYPE_MARKET
    assert CoindcxExchange.coindcx_order_type(OrderType.LIMIT).startswith(CONSTANTS.ORDER_TYPE_LIMIT)
    assert CoindcxExchange.to_hb_order_type(CONSTANTS.ORDER_TYPE_MARKET) == OrderType.MARKET


@pytest.mark.asyncio
async def test_place_order_and_cancel_are_called(monkeypatch):
    ex = CoindcxExchange(coindcx_api_key="k", coindcx_api_secret="s", trading_pairs=["BTC-USDT"], trading_required=False, domain="")

    async def fake_exchange_symbol_associated_to_pair(trading_pair: str):
        return "BTCUSDT"

    monkeypatch.setattr(ex, "exchange_symbol_associated_to_pair", fake_exchange_symbol_associated_to_pair)

    async def fake_api_post(path_url=None, data=None, is_auth_required: bool = False, **kwargs):
        if path_url == CONSTANTS.CREATE_ORDER_PATH_URL:
            return [{"id": "123", "created_at": 1640000000000}]
        if path_url == CONSTANTS.CANCEL_ORDER_PATH_URL:
            return {"status": "cancelled"}
        return None

    monkeypatch.setattr(ex, "_api_post", fake_api_post)

    # Test place order
    o_id, ts = await ex._place_order(
        order_id="cid1",
        trading_pair="BTC-USDT",
        amount=Decimal("1"),
        trade_type=TradeType.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("100"),
    )
    assert o_id == "123"

    # Test cancel
    order = InFlightOrder(
        client_order_id="cid1",
        exchange_order_id="123",
        trading_pair="BTC-USDT",
        order_type=OrderType.LIMIT,
        trade_type=TradeType.BUY,
        amount=Decimal("1"),
        price=Decimal("100"),
        creation_timestamp=0,
    )

    cancelled = await ex._place_cancel(order_id="cid1", tracked_order=order)
    assert cancelled is True
