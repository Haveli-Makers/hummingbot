import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

try:
    import socketio  # noqa: F401
except ImportError:
    socketio = types.ModuleType("socketio")

    class AsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        def event(self, func):
            return func

        def on(self, event_type):
            return lambda func: func

        async def connect(self, *args, **kwargs):
            pass

        async def wait(self):
            pass

        async def disconnect(self):
            pass

        async def emit(self, *args, **kwargs):
            pass

    socketio.AsyncClient = AsyncClient
    sys.modules["socketio"] = socketio

from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource


class DummyAuth:
    def generate_ws_auth_payload(self):
        return {"channel": "coindcx", "authToken": "test_token"}


class DummyConnector:
    def __init__(self):
        self._last_recv_time = 0


@pytest.mark.asyncio
async def test_handle_message_puts_message_in_queue():
    """Test that _handle_message puts messages in the output queue"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    q = asyncio.Queue()
    test_message = {"id": "1", "client_order_id": "c1", "status": "open"}

    await data_source._handle_message(test_message, q)

    assert not q.empty()
    item = q.get_nowait()
    assert item == test_message


@pytest.mark.asyncio
async def test_handle_message_updates_last_recv_time():
    """Test that _handle_message updates the last received time"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    initial_time = data_source.last_recv_time
    q = asyncio.Queue()

    await asyncio.sleep(0.01)  # Ensure time passes
    await data_source._handle_message({"test": "data"}, q)

    assert data_source.last_recv_time > initial_time


@pytest.mark.asyncio
async def test_build_client_creates_socketio_client():
    """Test that _build_client creates a Socket.IO client with proper handlers"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    q = asyncio.Queue()

    with patch("hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.socketio.AsyncClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda func: func)
        mock_client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
        mock_client_class.return_value = mock_client

        data_source._build_client(q)

        mock_client_class.assert_called_once_with(
            logger=False,
            reconnection=False,
            ssl_verify=False
        )

        assert mock_client.event.call_count >= 2
        assert mock_client.on.call_count >= 3


@pytest.mark.asyncio
async def test_disconnect_closes_client():
    """Test that _disconnect properly closes the Socket.IO client"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    mock_client = AsyncMock()
    mock_client.disconnect = AsyncMock()
    data_source._client = mock_client

    await data_source._disconnect()

    mock_client.disconnect.assert_called_once()
    assert data_source._client is None


@pytest.mark.asyncio
async def test_disconnect_handles_exception():
    """Test that _disconnect handles exceptions gracefully"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    mock_client = AsyncMock()
    mock_client.disconnect = AsyncMock(side_effect=Exception("Disconnect failed"))
    data_source._client = mock_client

    # Should not raise exception
    await data_source._disconnect()

    # Client should still be set to None
    assert data_source._client is None


@pytest.mark.asyncio
async def test_last_recv_time_property():
    """Test that last_recv_time property returns correct value"""
    data_source = CoinDCXAPIUserStreamDataSource(
        auth=DummyAuth(),
        trading_pairs=["BTC-USDT"],
        connector=DummyConnector(),
        api_factory=None,
        domain=""
    )

    assert data_source.last_recv_time == 0.0

    data_source._last_recv_time = 12345.67
    assert data_source.last_recv_time == 12345.67
