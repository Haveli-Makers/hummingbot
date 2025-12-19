import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource


class DummyConnector:
    def __init__(self):
        self._last_recv_time = 0


@pytest.mark.asyncio
async def test_process_websocket_messages_puts_events_in_queue():
    data_source = CoinDCXAPIUserStreamDataSource(auth=None, trading_pairs=["BTC-USDT"], connector=DummyConnector(), api_factory=None, domain="")

    async def _message_gen():
        msgs = [
            {"event": "order-update", "data": {"id": "1", "client_order_id": "c1", "status": "open"}},
            {"event": "trade-update", "data": {"order_id": "1", "quantity": 1}},
        ]
        for m in msgs:
            yield SimpleNamespace(data=m)

    mock_ws = AsyncMock()
    mock_ws.iter_messages = _message_gen

    q = asyncio.Queue()
    await data_source._process_websocket_messages(mock_ws, q)

    collected = []
    while not q.empty():
        collected.append(q.get_nowait())

    assert len(collected) == 2
    assert collected[0]["event"] == "order-update"
    assert collected[1]["event"] == "trade-update"


@pytest.mark.asyncio
async def test_process_websocket_messages_handles_nested_data():
    data_source = CoinDCXAPIUserStreamDataSource(auth=None, trading_pairs=["BTC-USDT"], connector=DummyConnector(), api_factory=None, domain="")

    async def _message_gen():
        yield SimpleNamespace(data={"data": {"currency": "BTC", "balance": 1}})

    mock_ws = AsyncMock()
    mock_ws.iter_messages = _message_gen

    q = asyncio.Queue()
    await data_source._process_websocket_messages(mock_ws, q)

    item = q.get_nowait()
    assert isinstance(item, dict)
    assert item["currency"] == "BTC"


@pytest.mark.asyncio
async def test_process_websocket_messages_ignores_ping():
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=None,
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    async def _message_gen():
        yield SimpleNamespace(data={"event": "ping"})

    mock_ws = AsyncMock()
    mock_ws.iter_messages = _message_gen

    q = asyncio.Queue()
    await data_source._process_websocket_messages(mock_ws, q)

    assert q.empty()


@pytest.mark.asyncio
async def test_process_websocket_messages_enqueues_fallback_dict():
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=None,
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    async def _message_gen():
        yield SimpleNamespace(data={"foo": "bar"})

    mock_ws = AsyncMock()
    mock_ws.iter_messages = _message_gen

    q = asyncio.Queue()
    await data_source._process_websocket_messages(mock_ws, q)

    item = q.get_nowait()
    assert item == {"foo": "bar"}


@pytest.mark.asyncio
async def test_process_websocket_messages_handles_non_dict_data():
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=None,
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    async def _message_gen():
        yield SimpleNamespace(data="raw-string")

    mock_ws = AsyncMock()
    mock_ws.iter_messages = _message_gen

    q = asyncio.Queue()
    await data_source._process_websocket_messages(mock_ws, q)

    assert q.empty()

def test_process_websocket_messages_enqueueing():
    async def run_test():
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

        results = []
        while not q.empty():
            results.append(q.get_nowait())

        assert any(isinstance(r, dict) for r in results)

    asyncio.run(run_test())
