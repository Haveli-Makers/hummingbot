import hashlib
import hmac
import json
from typing import Dict
from urllib.parse import urlparse

from yarl import URL

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class DeltaPerpetualAuth(AuthBase):
    """
    REST: HMAC-SHA256 of method + timestamp (seconds) + path + "?query" + body, valid for 5 seconds.
    WebSocket: one `key-auth` message signed over "GET" + timestamp + "/live".
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self.secret_key:
            return request

        timestamp = str(int(self._time_provider.time()))
        method = request.method.value
        path = urlparse(request.url).path

        query_string = ""
        if request.params:
            # Sign the exact query string aiohttp sends, which yarl builds.
            encoded = URL(request.url).extend_query(request.params).query_string
            query_string = f"?{encoded}"

        body_str = ""
        if request.data:
            if isinstance(request.data, str):
                body_str = request.data
            else:
                body_str = json.dumps(request.data, separators=(",", ":"))
                request.data = body_str

        prehash = f"{method}{timestamp}{path}{query_string}{body_str}"
        signature = self._sign(prehash)

        headers: Dict = request.headers or {}
        headers["api-key"] = self.api_key
        headers["signature"] = signature
        headers["timestamp"] = timestamp
        headers["User-Agent"] = CONSTANTS.BROKER_ID
        headers["Content-Type"] = "application/json"
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The socket is authenticated once, with ws_auth_payload().
        return request

    def ws_auth_payload(self) -> Dict:
        """Return the `key-auth` message to send right after the WebSocket connects."""
        timestamp = str(int(self._time_provider.time()))
        signature = self._sign(f"GET{timestamp}{CONSTANTS.WS_AUTH_PREHASH_PATH}")
        return {
            "type": CONSTANTS.WS_AUTH_TYPE,
            "payload": {
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp,
            },
        }

    def _sign(self, message: str) -> str:
        return hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
