"""
Shared pytest configuration for Hummingbot connector E2E tests.

Event-loop management is handled by pytest-asyncio 1.x via:
  - asyncio_default_fixture_loop_scope = module  (pytest.ini in this directory)
  - @pytest.mark.asyncio(loop_scope="module")    (TestConnectorE2E class)

No custom event_loop fixture is defined here: that pattern was deprecated in
pytest-asyncio 0.21 and causes a loop-mismatch in 1.x (fixture and test methods
end up on different loops, breaking async connectors whose background tasks are
bound to the creation loop).
"""
