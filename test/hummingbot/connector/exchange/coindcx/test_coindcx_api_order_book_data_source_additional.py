import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource


def test_channel_originating_message_trade_and_diff():
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=[], connector=None, api_factory=None)

    trade_event = {"p": 1, "q": 2, "T": 123}
    diff_event = {"bids": [], "asks": []}

    assert ds._channel_originating_message(trade_event) == ds._trade_messages_queue_key
    assert ds._channel_originating_message(diff_event) == ds._diff_messages_queue_key


def test_parse_trade_and_diff_message_async():
    async def run_test():
        # Create a simple connector stub with async trading_pair_associated_to_exchange_symbol
        class ConnectorStub:
            async def trading_pair_associated_to_exchange_symbol(self, symbol: str):
                return "BTC-USDT"

        connector = ConnectorStub()
        ds = CoinDCXAPIOrderBookDataSource(trading_pairs=[], connector=connector, api_factory=None)

        q = asyncio.Queue()

        raw_trade = {"s": "BTCUSDT", "T": 123456, "p": 1, "q": 2}
        await ds._parse_trade_message(raw_trade, q)
        item = q.get_nowait()
        assert item is not None

        # Order book diff parsing
        q2 = asyncio.Queue()
        raw_diff = {"bids": {"1": "2"}, "channel": "B-BTC_USDT@orderbook@20"}
        await ds._parse_order_book_diff_message(raw_diff, q2)
        item2 = q2.get_nowait()
        assert item2 is not None

        # Missing symbol in trade message should not enqueue
        q3 = asyncio.Queue()
        await ds._parse_trade_message({}, q3)
        assert q3.empty()

        # Diff message without orderbook channel should not enqueue
        q4 = asyncio.Queue()
        raw_diff2 = {"bids": {"1": "2"}, "channel": "UNKNOWN"}
        await ds._parse_order_book_diff_message(raw_diff2, q4)
        assert q4.empty()

    asyncio.run(run_test())
