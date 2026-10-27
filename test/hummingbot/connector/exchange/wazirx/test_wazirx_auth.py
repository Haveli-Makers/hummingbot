import asyncio
import hashlib
import hmac
from unittest import TestCase
from unittest.mock import MagicMock, patch

from typing_extensions import Awaitable

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth, WazirxTimestampSequence
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class WazirxAuthTests(TestCase):

    def setUp(self) -> None:
        self._api_key = "testApiKey"
        self._secret = "testSecret"

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_authenticate(self):
        """Test that rest_authenticate adds the API key header.

        Note: Full authentication (timestamp, signature) happens in add_auth_params()
        which is called by _wazirx_request() in wazirx_exchange.py, not in rest_authenticate().
        This test verifies that rest_authenticate adds the API key header as expected.
        """
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        params = {
            "symbol": "LTCBTC",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": 1,
            "price": "0.1",
        }

        auth = WazirxAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)
        request = RESTRequest(method=RESTMethod.GET, params=params, is_auth_required=True)
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        self.assertEqual({"X-Api-Key": self._api_key}, configured_request.headers)
        self.assertEqual(params, configured_request.params)

    def test_add_auth_params_signs_with_back_dated_synced_time(self):
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = 1700000000.5
        auth = WazirxAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)

        # no HTTP call for the timestamp
        with patch("hummingbot.connector.exchange.wazirx.wazirx_auth.aiohttp.ClientSession",
                   side_effect=AssertionError("no request expected")):
            auth_params, query_string = self.async_run_with_timeout(auth.add_auth_params({"symbol": "btcinr"}))

        expected_timestamp = 1700000000500 - CONSTANTS.TIMESTAMP_SAFETY_MARGIN_MS
        unsigned = f"symbol=btcinr&recvWindow={WazirxAuth.RECV_WINDOW}&timestamp={expected_timestamp}"
        signature = hmac.new(self._secret.encode("utf-8"), unsigned.encode("utf-8"), hashlib.sha256).hexdigest()
        self.assertEqual(expected_timestamp, auth_params["timestamp"])
        self.assertEqual(signature, auth_params["signature"])
        self.assertEqual(f"{unsigned}&signature={signature}", query_string)

    def test_keys_sharing_a_sequence_never_reuse_a_timestamp(self):
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = 1700000000.0
        sequence = WazirxTimestampSequence()
        first = WazirxAuth(api_key="k1", secret_key="s1", time_provider=mock_time_provider, timestamp_sequence=sequence)
        second = WazirxAuth(api_key="k2", secret_key="s2", time_provider=mock_time_provider, timestamp_sequence=sequence)

        timestamps = [first._get_timestamp(), second._get_timestamp(), first._get_timestamp()]

        base = 1700000000000 - CONSTANTS.TIMESTAMP_SAFETY_MARGIN_MS
        self.assertEqual([base, base + 1, base + 2], timestamps)

    def test_timestamp_sequence_follows_the_clock_forward(self):
        sequence = WazirxTimestampSequence()
        self.assertEqual(1000, sequence.next(1000))
        self.assertEqual(1001, sequence.next(999))
        self.assertEqual(5000, sequence.next(5000))
