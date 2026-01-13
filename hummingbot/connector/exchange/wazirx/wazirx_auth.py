import asyncio
import hashlib
import hmac
import time
from typing import Any, Dict
from urllib.parse import urlencode

import aiohttp

import hummingbot.connector.exchange.wazirx.wazirx_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class WazirxAuth(AuthBase):
    """
    WazirX authentication.

    WazirX signed endpoints require:
    - API key in header `X-Api-Key`
    - HMAC-SHA256 signature computed from URL-encoded parameters
    - Content-Type: application/x-www-form-urlencoded for POST/DELETE
    - timestamp in milliseconds
    - recvWindow for clock skew tolerance
    """

    RECV_WINDOW = 60000

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider
        self._nonce_counter = 0
        self._nonce_lock = asyncio.Lock()
        self._last_timestamp = 0

    async def _get_timestamp(self) -> int:
        async with self._nonce_lock:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{CONSTANTS.REST_URL}{CONSTANTS.SERVER_TIME_PATH_URL}") as response:
                        if response.status == 200:
                            data = await response.json()
                            current_ts = int(data.get("serverTime", int(time.time() * 1000)))
                        else:
                            current_ts = int(time.time() * 1000)
            except Exception:
                current_ts = int(time.time() * 1000)
            
            if current_ts <= self._last_timestamp:
                current_ts = self._last_timestamp + 1
            
            self._last_timestamp = current_ts
            return current_ts

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
        """
        Add authentication parameters (timestamp, recvWindow, signature) to params.
        Returns (params_with_signature, query_string_for_body).
        Params are: original_params + recvWindow + timestamp + signature
        """
        auth_params = dict(params)
        auth_params["recvWindow"] = self.RECV_WINDOW
        auth_params["timestamp"] = await self._get_timestamp()
        
        query_string = self._generate_query_string(auth_params)
        signature = self.generate_signature(query_string)
        auth_params["signature"] = signature
        final_query_string = query_string + f"&signature={signature}"
        
        return auth_params, final_query_string

    def get_headers(self) -> Dict[str, str]:
        """Get headers required for authenticated requests."""
        return {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded"
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Note: This method is called by the REST assistant, but for WazirX we handle
        authentication differently in _wazirx_request() to ensure proper formatting.
        This is kept for compatibility with the framework.
        """
        headers = {} if request.headers is None else dict(request.headers)
        headers["X-Api-Key"] = self.api_key
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request
