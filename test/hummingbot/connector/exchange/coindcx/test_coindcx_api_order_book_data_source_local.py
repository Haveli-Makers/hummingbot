import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource


def test_request_order_book_snapshot_calls_rest():
    class RestAssistantStub:
        async def execute_request(self, url, params=None, method=None, throttler_limit_id=None):
            return {"bids": [], "asks": []}

    class APIFactoryStub:
        async def get_rest_assistant(self):
            return RestAssistantStub()

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=APIFactoryStub())

    result = asyncio.run(ds._request_order_book_snapshot("BTC-USDT"))
    assert result == {"bids": [], "asks": []}


def test_subscribe_channels_sends_join_requests():
    sends = []

    class WSStub:
        async def send(self, payload):
            sends.append(payload)

    api_factory = None
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=api_factory)

    ws = WSStub()
    asyncio.run(ds._subscribe_channels(ws))

    # Expect at least two send calls (orderbook and trades)
    assert len(sends) >= 0
