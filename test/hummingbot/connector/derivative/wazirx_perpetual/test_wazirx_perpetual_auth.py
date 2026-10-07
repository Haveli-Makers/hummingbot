import hashlib
import hmac
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from urllib.parse import parse_qsl

from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_auth import WazirxPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class _FixedTime:
    def __init__(self, now: float = 1_700_000_000.0):
        self.now = now

    def time(self) -> float:
        return self.now


class WazirxPerpetualAuthTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.clock = _FixedTime()
        self.auth = WazirxPerpetualAuth(api_key="the-key", secret_key="the-secret", time_provider=self.clock)

    def _sign(self, payload: str) -> str:
        return hmac.new(b"the-secret", payload.encode(), hashlib.sha256).hexdigest()

    async def test_get_signs_the_query_string_and_puts_it_in_the_url(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.wazirx.com/fapi/v1/positionRisk",
            params={"symbol": "BTCINR"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)

        expected_payload = "symbol=BTCINR&timestamp=1700000000000"
        self.assertEqual(
            f"https://api.wazirx.com/fapi/v1/positionRisk?{expected_payload}&signature={self._sign(expected_payload)}",
            signed.url)
        # The signed string travels verbatim; nothing is left for aiohttp to re-encode.
        self.assertIsNone(signed.params)
        self.assertIsNone(signed.data)
        self.assertEqual("the-key", signed.headers["X-Api-Key"])

    async def test_post_signs_the_form_body_built_from_the_json_data(self):
        # RESTAssistant json-dumps ``data`` before auth sees it.
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.wazirx.com/fapi/v1/order",
            data=json.dumps({"requestId": "haveliBBCIR1", "symbol": "BTCINR", "side": "BUY", "type": "LIMIT",
                             "quantity": "0.010", "price": "5700000", "leverage": 10}),
            headers={"Content-Type": "application/json"},
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)

        expected_payload = ("requestId=haveliBBCIR1&symbol=BTCINR&side=BUY&type=LIMIT&quantity=0.010"
                            "&price=5700000&leverage=10&timestamp=1700000000000")
        self.assertEqual(f"{expected_payload}&signature={self._sign(expected_payload)}", signed.data)
        self.assertEqual("application/x-www-form-urlencoded", signed.headers["Content-Type"])
        self.assertEqual("https://api.wazirx.com/fapi/v1/order", signed.url)
        self.assertIsNone(signed.params)

    async def test_delete_sends_a_signed_form_body(self):
        request = RESTRequest(
            method=RESTMethod.DELETE,
            url="https://api.wazirx.com/fapi/v1/order",
            data=json.dumps({"symbol": "BTCINR", "orderId": "1234567"}),
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)

        fields = dict(parse_qsl(signed.data))
        self.assertEqual({"symbol", "orderId", "timestamp", "signature"}, set(fields))
        payload = signed.data.rsplit("&signature=", 1)[0]
        self.assertEqual(self._sign(payload), fields["signature"])

    async def test_post_without_data_still_signs_the_timestamp(self):
        # create_auth_token is a POST with no parameters of its own.
        request = RESTRequest(method=RESTMethod.POST, url="https://api.wazirx.com/sapi/v1/create_auth_token",
                              is_auth_required=True)
        signed = await self.auth.rest_authenticate(request)
        self.assertEqual(f"timestamp=1700000000000&signature={self._sign('timestamp=1700000000000')}", signed.data)

    def test_timestamps_strictly_increase_within_the_same_millisecond(self):
        # A reused timestamp on a non-GET request is rejected with 2006 ("tonce").
        first, _ = self.auth.add_auth_to_params({"a": 1})
        second, _ = self.auth.add_auth_to_params({"a": 1})
        self.assertEqual(1700000000000, first["timestamp"])
        self.assertEqual(1700000000001, second["timestamp"])

        self.clock.now += 5
        third, _ = self.auth.add_auth_to_params({})
        self.assertEqual(1700000005000, third["timestamp"])

    def test_none_values_are_not_sent(self):
        params, payload = self.auth.add_auth_to_params({"symbol": "BTCINR", "orderId": None})
        self.assertNotIn("orderId", params)
        self.assertTrue(payload.startswith("symbol=BTCINR&timestamp="))

    def test_signature_excludes_itself(self):
        params, payload = self.auth.add_auth_to_params({"symbol": "BTCINR"})
        unsigned = payload.rsplit("&signature=", 1)[0]
        self.assertEqual(self._sign(unsigned), params["signature"])
