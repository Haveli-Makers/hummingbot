"""
Fixtures and configuration for CoinDCX connector tests.
Provides socketio stub for test environments where python-socketio is not available.
"""

import sys
import types

import pytest


@pytest.fixture(scope="session", autouse=True)
def inject_socketio_stub():
    """
    Inject a socketio stub into sys.modules before any coindcx test imports.
    This prevents ModuleNotFoundError when python-socketio is not installed.
    """
    if "socketio" not in sys.modules:
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

    yield
