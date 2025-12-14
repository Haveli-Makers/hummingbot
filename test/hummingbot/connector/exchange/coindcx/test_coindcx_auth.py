import json
import time
import asyncio
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import patch

from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoinDCXAuthTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "test_api"
        self.secret_key = "test_secret"
        self.auth = CoinDCXAuth(self.api_key, self.secret_key, time_provider=None)

    def test_header_for_authentication(self):
        headers = self.auth.header_for_authentication("sig")
        self.assertEqual(headers.get("X-AUTH-APIKEY"), self.api_key)
        self.assertEqual(headers.get("X-AUTH-SIGNATURE"), "sig")

    async def test_generate_ws_auth_payload(self):
        with patch("hummingbot.connector.exchange.coindcx.coindcx_auth.time", autospec=True) as mock_time:
            mock_time.time.return_value = 1.23
            payload = self.auth.generate_ws_auth_payload()
            self.assertIn("channelName", payload)
            self.assertIn("authSignature", payload)
            self.assertEqual(payload.get("apiKey"), self.api_key)

    async def test_rest_authenticate_adds_headers_and_body(self):
        # Fix time to make signature deterministic
        fixed_time = 1234.0
        with patch("time.time", return_value=fixed_time):
            request = RESTRequest(method=RESTMethod.POST, url="https://api.test", data={"a": 1})
            ret = await self.auth.rest_authenticate(request)
            # Should return the same RESTRequest object
            self.assertIs(request, ret)
            # Data should be JSON string and include timestamp
            data_obj = json.loads(request.data)
            self.assertEqual(data_obj.get("a"), 1)
            self.assertEqual(data_obj.get("timestamp"), int(fixed_time * 1000))
            # Headers should include auth headers and content-type
            headers = request.headers
            self.assertIsNotNone(headers.get("X-AUTH-APIKEY"))
            self.assertIsNotNone(headers.get("X-AUTH-SIGNATURE"))
            self.assertEqual(headers.get("Content-Type"), "application/json")
