import hashlib
import hmac
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.connector.derivative.delta_options.delta_options_auth import DeltaOptionsAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class DeltaOptionsAuthTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.secret = "secret"
        time_provider = MagicMock()
        time_provider.time.return_value = 1700000000.9
        self.auth = DeltaOptionsAuth(api_key="key", secret_key=self.secret, time_provider=time_provider)

    def _sign(self, message: str) -> str:
        return hmac.new(self.secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    async def test_get_request_signs_path_and_query_string(self):
        request = RESTRequest(method=RESTMethod.GET, url="https://api.india.delta.exchange/v2/positions/margined",
                              params={"contract_types": "call_options,put_options"}, is_auth_required=True)

        await self.auth.rest_authenticate(request)

        expected = self._sign("GET1700000000/v2/positions/margined?contract_types=call_options,put_options")
        self.assertEqual(expected, request.headers["signature"])
        self.assertEqual("key", request.headers["api-key"])
        self.assertEqual("1700000000", request.headers["timestamp"])
        self.assertIn("User-Agent", request.headers)

    async def test_post_request_signs_the_compact_json_body(self):
        request = RESTRequest(method=RESTMethod.POST, url="https://api.india.delta.exchange/v2/orders",
                              data={"product_id": 101, "size": 1}, is_auth_required=True)

        await self.auth.rest_authenticate(request)

        body = json.dumps({"product_id": 101, "size": 1}, separators=(",", ":"))
        self.assertEqual(body, request.data)
        self.assertEqual(self._sign(f"POST1700000000/v2/orders{body}"), request.headers["signature"])

    def test_websocket_uses_key_auth(self):
        payload = self.auth.ws_auth_payload()

        self.assertEqual("key-auth", payload["type"])
        self.assertEqual({"api-key": "key", "signature": self._sign("GET1700000000/live"), "timestamp": "1700000000"},
                         payload["payload"])
