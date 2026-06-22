import asyncio
import hashlib
import hmac
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.exchange.valr.valr_auth import ValrAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class ValrAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"
        self.seconds = 1700000000.0       # TimeSynchronizer.time() returns seconds
        self.ms = 1700000000000            # auth signs in milliseconds
        time_provider = MagicMock()
        time_provider.time.return_value = self.seconds
        self.auth = ValrAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _sign(self, message: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha512).hexdigest()

    def test_rest_get_signs_timestamp_verb_path(self):
        request = RESTRequest(
            method=RESTMethod.GET, url="https://api.valr.com/v1/account/balances", is_auth_required=True)
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        expected = self._sign(f"{self.ms}GET/v1/account/balances")
        self.assertEqual(self.api_key, request.headers["X-VALR-API-KEY"])
        self.assertEqual(str(self.ms), request.headers["X-VALR-TIMESTAMP"])
        self.assertEqual(expected, request.headers["X-VALR-SIGNATURE"])

    def test_rest_get_includes_query_in_path(self):
        request = RESTRequest(
            method=RESTMethod.GET, url="https://api.valr.com/v1/orders/open",
            params={"pair": "BTCZAR"}, is_auth_required=True)
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertEqual(self._sign(f"{self.ms}GET/v1/orders/open?pair=BTCZAR"),
                         request.headers["X-VALR-SIGNATURE"])

    def test_rest_post_signs_body(self):
        request = RESTRequest(
            method=RESTMethod.POST, url="https://api.valr.com/v1/orders/limit",
            data={"side": "BUY", "pair": "BTCZAR"}, is_auth_required=True)
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertEqual('{"side":"BUY","pair":"BTCZAR"}', request.data)
        self.assertEqual(self._sign(f"{self.ms}POST/v1/orders/limit{request.data}"),
                         request.headers["X-VALR-SIGNATURE"])

    def test_ws_auth_headers_sign_path(self):
        headers = self.auth.ws_auth_headers("/ws/account")
        self.assertEqual(self.api_key, headers["X-VALR-API-KEY"])
        self.assertEqual(str(self.ms), headers["X-VALR-TIMESTAMP"])
        self.assertEqual(self._sign(f"{self.ms}GET/ws/account"), headers["X-VALR-SIGNATURE"])

    def test_no_credentials_returns_request_unmodified(self):
        auth = ValrAuth(api_key="", secret_key="", time_provider=MagicMock())
        request = RESTRequest(method=RESTMethod.GET, url="https://api.valr.com/v1/public/pairs")
        result = self.async_run_with_timeout(auth.rest_authenticate(request))
        self.assertNotIn("X-VALR-SIGNATURE", result.headers or {})
