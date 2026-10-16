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

    def test_signing_matches_the_guides_code_sample(self):
        # §13 "Verify balance": body {"timestamp": …}, compact JSON, HMAC-SHA256 hex, JSON content type.
        request = RESTRequest(method=RESTMethod.GET, url="https://fapi.example/fapi/v2/balance",
                              is_auth_required=True)
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        raw = json.dumps({"timestamp": self.ms}, separators=(",", ":"))
        self.assertEqual(raw, request.data)
        self.assertEqual({"Content-Type": "application/json", "X-AUTH-APIKEY": self.api_key,
                          "X-AUTH-SIGNATURE": self._sign(raw)}, request.headers)

    def test_delete_carries_its_parameters_in_the_signed_body(self):
        request = RESTRequest(method=RESTMethod.DELETE, url="https://fapi.example/fapi/v1/order",
                              data=json.dumps({"symbol": "BTCUSDT", "origClientOrderId": "haveli-1"}),
                              is_auth_required=True)
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        raw = json.dumps({"symbol": "BTCUSDT", "origClientOrderId": "haveli-1", "timestamp": self.ms},
                         separators=(",", ":"))
        self.assertEqual(raw, request.data)
        self.assertEqual(self._sign(raw), request.headers["X-AUTH-SIGNATURE"])
        self.assertIsNone(request.params)

    def test_websocket_requests_pass_through(self):
        request = MagicMock()
        self.assertIs(request, self.async_run_with_timeout(self.auth.ws_authenticate(request)))
