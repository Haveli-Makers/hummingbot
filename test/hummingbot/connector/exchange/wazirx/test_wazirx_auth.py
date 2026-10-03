import asyncio
import hashlib
import hmac
from unittest import TestCase
from unittest.mock import MagicMock

from typing_extensions import Awaitable

from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
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

    def test_add_auth_params_url_encodes_special_characters(self):
        """Emails contain '+' and '@'; these must be percent-encoded so the signature the server
        recomputes (over the decoded params) matches. Otherwise WazirX returns 'Signature is
        incorrect' (code 2005), as seen with sub-account fund transfers."""
        auth = WazirxAuth(api_key=self._api_key, secret_key=self._secret, time_provider=MagicMock())

        async def _fixed_timestamp():
            return 1700000000000

        auth._get_timestamp = _fixed_timestamp

        params = {
            "currency": "inr",
            "amount": "100",
            "fromEmail": "vinayak.a@havelimakers.com",
            "toEmail": "org+test@havelimakers.com",
        }
        auth_params, query_string = self.async_run_with_timeout(auth.add_auth_params(params))

        # '+' -> %2B and '@' -> %40 so the server decodes the original email back.
        self.assertIn("toEmail=org%2Btest%40havelimakers.com", query_string)
        self.assertNotIn("org+test@havelimakers.com", query_string)

        # Signature must be computed over the encoded string (everything before &signature=).
        signed_part = query_string.split("&signature=")[0]
        expected = hmac.new(self._secret.encode("utf-8"), signed_part.encode("utf-8"), hashlib.sha256).hexdigest()
        self.assertEqual(expected, auth_params["signature"])
        self.assertTrue(query_string.endswith(f"&signature={expected}"))
