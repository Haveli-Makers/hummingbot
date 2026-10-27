import hashlib
import hmac
import time
from typing import Any, Dict, Optional

import aiohttp

import hummingbot.connector.exchange.wazirx.wazirx_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class WazirxTimestampSequence:
    """Strictly increasing timestamps shared by all keys of an account (avoids WazirX error 2006)."""

    def __init__(self):
        self._last_timestamp_ms = 0

    def next(self, timestamp_ms: int) -> int:
        if timestamp_ms <= self._last_timestamp_ms:
            timestamp_ms = self._last_timestamp_ms + 1
        self._last_timestamp_ms = timestamp_ms
        return timestamp_ms


class WazirxAuth(AuthBase):
    """
    WazirX authentication handler for API requests.

    Handles HMAC-SHA256 signature generation and timestamp synchronization
    for authenticated API calls to the WazirX exchange.
    """

    RECV_WINDOW = 60000
    AUTH_TOKEN_TIMEOUT = 900

    def __init__(self,
                 api_key: str,
                 secret_key: str,
                 time_provider: TimeSynchronizer,
                 timestamp_sequence: Optional[WazirxTimestampSequence] = None):
        """
        Initialize the WazirX authentication handler.
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider
        self._timestamp_sequence = timestamp_sequence or WazirxTimestampSequence()
        self._auth_key: Optional[str] = None
        self._auth_key_timestamp: float = 0

    def _get_timestamp(self) -> int:
        # Back-dated: WazirX tolerates late timestamps but rejects early ones (see
        # TIMESTAMP_SAFETY_MARGIN_MS).
        timestamp = int(self.time_provider.time() * 1e3) - CONSTANTS.TIMESTAMP_SAFETY_MARGIN_MS
        return self._timestamp_sequence.next(timestamp)

    def _generate_query_string(self, params: Dict[str, Any]) -> str:
        """
        Generate query string from parameters, preserving order.
        """
        return "&".join([f"{k}={v}" for k, v in params.items()])

    def generate_signature(self, query_string: str) -> str:
        """Generate HMAC-SHA256 signature from query string."""
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def add_auth_params(self, params: Dict[str, Any]) -> tuple[Dict[str, Any], str]:
        auth_params = dict(params)
        auth_params["recvWindow"] = self.RECV_WINDOW
        auth_params["timestamp"] = self._get_timestamp()

        query_string = self._generate_query_string(auth_params)
        signature = self.generate_signature(query_string)
        auth_params["signature"] = signature
        final_query_string = query_string + f"&signature={signature}"

        return auth_params, final_query_string

    def get_headers(self) -> Dict[str, str]:
        return {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded"
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {} if request.headers is None else dict(request.headers)
        headers["X-Api-Key"] = self.api_key
        request.headers = headers
        return request

    async def get_ws_auth_key(self) -> str:
        current_time = time.time()

        if self._auth_key and (current_time - self._auth_key_timestamp) < self.AUTH_TOKEN_TIMEOUT:
            return self._auth_key

        url = f"{CONSTANTS.REST_URL}{CONSTANTS.CREATE_AUTH_TOKEN_PATH_URL}"

        params: Dict[str, Any] = {}
        auth_params, query_string = await self.add_auth_params(params)

        headers = self.get_headers()

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=query_string, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self._auth_key = data.get("auth_key")
                    self._auth_key_timestamp = current_time
                    return self._auth_key
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to get auth token: {response.status} - {error_text}")

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        await self.get_ws_auth_key()
        return request
