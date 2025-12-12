import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource


def test_process_websocket_messages_enqueueing():
    async def run_test():
        # Minimal auth and api factory stubs
        class AuthStub:
            def generate_ws_auth_payload(self):
                return {}

        class FakeResp:
            def __init__(self, data):
                self.data = data

        class FakeWS:
            def __init__(self, messages):
                self._messages = messages

            async def iter_messages(self):
                for m in self._messages:
                    yield FakeResp(m)

        # Build data source with minimal dependencies
        ds = CoinDCXAPIUserStreamDataSource(auth=AuthStub(), trading_pairs=[], connector=None, api_factory=None)

        q = asyncio.Queue()

        messages = [
            {"event": "order-update", "o": {"id": 1}},
            {"event": "balance-update", "a": "BTC", "balance": 1},
            {"event": "ping"},
            {"foo": "bar"},
            {"data": {"nested": 1}},
        ]

        fake_ws = FakeWS(messages)
        await ds._process_websocket_messages(fake_ws, q)

        # Items enqueued: order-update, balance-update, foo, nested
        results = []
        while not q.empty():
            results.append(q.get_nowait())

        assert any(isinstance(r, dict) for r in results)

    asyncio.run(run_test())
