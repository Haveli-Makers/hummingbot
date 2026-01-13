import asyncio
import hashlib
import hmac
from copy import copy
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
