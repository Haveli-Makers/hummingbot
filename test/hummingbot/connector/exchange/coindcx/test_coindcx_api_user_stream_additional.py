import asyncio

import pytest

from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource


class _AuthStub:
    def generate_ws_auth_payload(self):
        return {"auth": "ok"}


class _WSMsg:
    def __init__(self, data):
        self.data = data


class _FakeWS:
    def __init__(self, msgs):
        self._msgs = msgs

    async def iter_messages(self):
        for m in self._msgs:
            yield _WSMsg(m)


@pytest.mark.asyncio
async def test_process_websocket_messages_various_cases():
    auth = _AuthStub()
    ds = CoinDCXAPIUserStreamDataSource(auth=auth, trading_pairs=[], connector=None, api_factory=None)

    q = asyncio.Queue()

    msgs = [
        {"event": "ping"},
        {"event": "order-update", "data": {"c": "1"}},
        {"some": "value"},
        {"data": {"x": 1}},
    ]

    ws = _FakeWS(msgs)
    await ds._process_websocket_messages(ws, q)

    # ping ignored, others enqueued -> 3 items
    out = [q.get_nowait() for _ in range(3)]
    assert any(isinstance(i, dict) for i in out)
