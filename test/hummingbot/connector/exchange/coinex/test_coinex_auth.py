import asyncio
import hashlib
import hmac
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoinexAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testAccessId"
        self.secret_key = "testSecretKey"
        self.seconds = 1700000000.0       # TimeSynchronizer.time() returns seconds
        self.ms = 1700000000000            # auth signs in milliseconds
        time_provider = MagicMock()
        time_provider.time.return_value = self.seconds
        self.auth = CoinexAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _sign(self, message: str) -> str:
        return hmac.new(self.secret_key.encode("latin-1"), message.encode("latin-1"), hashlib.sha256).hexdigest().lower()

    def test_rest_get_signs_method_path_query_timestamp(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.coinex.com/v2/spot/order-status",
            params={"market": "BTCUSDT", "order_id": "1"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        expected = self._sign(f"GET/v2/spot/order-status?market=BTCUSDT&order_id=1{self.ms}")
        self.assertEqual(self.api_key, request.headers["X-COINEX-KEY"])
        self.assertEqual(str(self.ms), request.headers["X-COINEX-TIMESTAMP"])
        self.assertEqual(expected, request.headers["X-COINEX-SIGN"])

    def test_rest_post_signs_method_path_body_timestamp(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.coinex.com/v2/spot/order",
            data={"market": "BTCUSDT", "side": "buy"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertEqual('{"market":"BTCUSDT","side":"buy"}', request.data)
        expected = self._sign(f"POST/v2/spot/order{request.data}{self.ms}")
        self.assertEqual(expected, request.headers["X-COINEX-SIGN"])
        self.assertEqual("application/json", request.headers["Content-Type"])

    def test_ws_auth_payload_signs_timestamp_only(self):
        payload = self.auth.ws_auth_payload(request_id=15)
        self.assertEqual("server.sign", payload["method"])
        self.assertEqual(15, payload["id"])
        self.assertEqual(self.api_key, payload["params"]["access_id"])
        self.assertEqual(self.ms, payload["params"]["timestamp"])
        self.assertEqual(self._sign(str(self.ms)), payload["params"]["signed_str"])

    def test_no_credentials_returns_request_unmodified(self):
        auth = CoinexAuth(api_key="", secret_key="", time_provider=MagicMock())
        request = RESTRequest(method=RESTMethod.GET, url="https://api.coinex.com/v2/spot/market")
        result = self.async_run_with_timeout(auth.rest_authenticate(request))
        self.assertNotIn("X-COINEX-SIGN", result.headers or {})
