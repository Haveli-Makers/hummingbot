import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


def test_subscribe_channels_records_payloads():
    sends = []

    class WSStub:
        async def send(self, payload):
            # payload is WSJSONRequest
            if isinstance(payload, WSJSONRequest):
                sends.append(payload.payload)

    api_factory = None
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT", "ETH-USDT"], connector=None, api_factory=api_factory)

    ws = WSStub()
    asyncio.run(ds._subscribe_channels(ws))

    # Expect at least two join payloads per trading pair (orderbook + trades)
    assert len(sends) >= 4


def test_parse_diff_message_asks_or_bids():
    class ConnectorStub:
        async def trading_pair_associated_to_exchange_symbol(self, symbol: str):
            return "BTC-USDT"

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=ConnectorStub(), api_factory=None)
    q = asyncio.Queue()
    raw_asks = {"asks": {"2": "1"}, "channel": "B-BTC_USDT@orderbook@20"}
    asyncio.run(ds._parse_order_book_diff_message(raw_asks, q))
    assert not q.empty()

    q2 = asyncio.Queue()
    raw_bids = {"bids": {"1": "1"}, "channel": "B-BTC_USDT@orderbook@20"}
    asyncio.run(ds._parse_order_book_diff_message(raw_bids, q2))
    assert not q2.empty()


def test_connected_websocket_assistant_uses_factory():
    connected = {"connected": False}

    class WS:
        async def connect(self, ws_url=None, ping_timeout=None, message_timeout=None):
            connected["connected"] = True

    class APIFactory:
        async def get_ws_assistant(self):
            return WS()

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=APIFactory())
    ws = asyncio.run(ds._connected_websocket_assistant())
    assert connected["connected"] is True
    assert ws is not None
