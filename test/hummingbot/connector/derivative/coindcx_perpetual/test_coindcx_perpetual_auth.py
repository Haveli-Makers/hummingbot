import asyncio
import hashlib
import hmac
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_auth import CoinDCXPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class CoinDCXPerpetualAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"
        self.seconds = 1700000000.0
        self.ms = 1700000000000
        time_provider = MagicMock()
        time_provider.time.return_value = self.seconds
        self.auth = CoinDCXPerpetualAuth(self.api_key, self.secret_key, time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _sign(self, payload: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), payload.encode(), hashlib.sha256).hexdigest()

    def test_post_signs_compact_json_body_with_timestamp(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.coindcx.com/exchange/v1/derivatives/futures/orders/create",
            data={"order": {"pair": "B-BTC_USDT"}},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected_body = json.dumps({"order": {"pair": "B-BTC_USDT"}, "timestamp": self.ms}, separators=(",", ":"))
        self.assertEqual(expected_body, request.data)
        self.assertEqual(self.api_key, request.headers["X-AUTH-APIKEY"])
        self.assertEqual(self._sign(expected_body), request.headers["X-AUTH-SIGNATURE"])
        self.assertEqual("application/json", request.headers["Content-Type"])

    def test_get_request_is_also_signed_with_a_body(self):
        # The futures wallets endpoint is a GET that still carries a signed body,
        # unlike the spot connector which only signs POST requests.
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.coindcx.com/exchange/v1/derivatives/futures/wallets",
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected_body = json.dumps({"timestamp": self.ms}, separators=(",", ":"))
        self.assertEqual(expected_body, request.data)
        self.assertEqual(self._sign(expected_body), request.headers["X-AUTH-SIGNATURE"])

    def test_existing_timestamp_is_preserved(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.coindcx.com/exchange/v1/derivatives/futures/positions",
            data={"timestamp": 123},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertEqual(json.dumps({"timestamp": 123}, separators=(",", ":")), request.data)

    def test_string_body_is_reparsed_and_signed(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.coindcx.com/exchange/v1/derivatives/futures/orders/cancel",
            data=json.dumps({"id": "abc"}),
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        expected_body = json.dumps({"id": "abc", "timestamp": self.ms}, separators=(",", ":"))
        self.assertEqual(expected_body, request.data)

    def test_ws_auth_payload_signs_the_channel(self):
        payload = self.auth.generate_ws_auth_payload()
        self.assertEqual("coindcx", payload["channelName"])
        self.assertEqual(self.api_key, payload["apiKey"])
        self.assertEqual(self._sign('{"channel":"coindcx"}'), payload["authSignature"])
