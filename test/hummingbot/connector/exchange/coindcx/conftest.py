"""
Configuration for CoinDCX connector tests.
Provides socketio stub for test environments where python-socketio is not available.
This must execute at module load time (before test import) to prevent ModuleNotFoundError.
"""

import sys
import types

# Inject socketio stub at module load time, before test collection
# This executes when conftest.py is first loaded by pytest
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
