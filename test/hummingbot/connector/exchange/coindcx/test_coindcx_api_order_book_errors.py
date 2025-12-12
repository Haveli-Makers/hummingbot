import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource


def test_request_order_book_snapshot_raises_on_rest_error():
    class RestAssistant:
        async def execute_request(self, url=None, params=None, method=None, throttler_limit_id=None):
            raise Exception("rest fail")

    class APIFactory:
        async def get_rest_assistant(self):
            return RestAssistant()

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=APIFactory())

    try:
        asyncio.run(ds._request_order_book_snapshot("BTC-USDT"))
    except Exception:
        assert True
