import hashlib
import hmac
import json
from typing import Dict
from urllib.parse import urlparse

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class DeltaPerpetualAuth(AuthBase):
    """
    Delta Exchange authentication using HMAC-SHA256.

    REST: every authenticated request carries headers
      api-key    – public API key
      signature  – hex HMAC-SHA256 of the prehash string
      timestamp  – Unix time in **seconds**
      User-Agent – required by Delta to avoid 4xx rejections

    Prehash string (per the docs):
      method + timestamp + requestPath + query_string + body
        - requestPath includes the leading "/v2/…"
        - query_string includes the leading "?" (empty if no params)
        - body is the raw JSON string (empty if no body)

    Signatures are only valid for 5 seconds, so requests must be sent promptly.

    WebSocket: a single `auth` message is sent after connecting (see
    `ws_auth_payload`); the signature there is HMAC of  "GET" + timestamp + "/live".
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self.secret_key:
            return request

        timestamp = str(int(self._time_provider.time()))
        method = request.method.name if hasattr(request.method, "name") else str(request.method).upper()
        path = urlparse(request.url).path

        query_string = ""
        if request.params:
            qs = "&".join(f"{key}={value}" for key, value in request.params.items())
            query_string = f"?{qs}"

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
        # Delta authenticates the socket with a single `auth` message
        # (built via ws_auth_payload), not per-request signing.
        return request

    def ws_auth_payload(self) -> Dict:
        """Return the `auth` message to send right after the WebSocket connects."""
        timestamp = str(int(self._time_provider.time()))
        signature = self._sign(f"GET{timestamp}{CONSTANTS.WS_AUTH_PREHASH_PATH}")
        return {
            "type": "auth",
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
