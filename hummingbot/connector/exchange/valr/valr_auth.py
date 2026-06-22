import hashlib
import hmac
import json
from typing import Dict
from urllib.parse import urlparse

import hummingbot.connector.exchange.valr.valr_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class ValrAuth(AuthBase):
    """
    VALR authentication using HMAC-SHA512.

    REST: every authenticated request carries headers
      X-VALR-API-KEY    – the API key
      X-VALR-SIGNATURE  – lowercase hex HMAC-SHA512 of the signing string
      X-VALR-TIMESTAMP  – request time in **milliseconds**

    Signing string (per the docs):
      timestamp + verb + request_path + body
        - verb is upper-case (GET/POST/DELETE)
        - request_path includes the leading "/v1/…" and, for GET, the "?query"
        - body is the raw JSON string (empty for GET/DELETE without a body)

    WebSocket: the public market-data socket (/ws/trade) needs no auth. The
    private account socket (/ws/account) is authenticated with the same three
    headers on the connection upgrade (see ws_auth_headers), signing the path.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    def _timestamp_ms(self) -> int:
        return int(self._time_provider.time() * 1000)

    def _sign(self, timestamp: int, verb: str, path: str, body: str = "") -> str:
        payload = f"{timestamp}{verb.upper()}{path}{body}"
        return hmac.new(
            self.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self.secret_key:
            return request

        timestamp = self._timestamp_ms()
        verb = request.method.name if hasattr(request.method, "name") else str(request.method).upper()
        path = urlparse(request.url).path
        if request.params:
            path = f"{path}?" + "&".join(f"{k}={v}" for k, v in request.params.items())

        body_str = ""
        if request.data:
            if isinstance(request.data, str):
                body_str = request.data
            else:
                body_str = json.dumps(request.data, separators=(",", ":"))
                request.data = body_str

        signature = self._sign(timestamp, verb, path, body_str)
        headers: Dict = request.headers or {}
        headers[CONSTANTS.HEADER_API_KEY] = self.api_key
        headers[CONSTANTS.HEADER_SIGNATURE] = signature
        headers[CONSTANTS.HEADER_TIMESTAMP] = str(timestamp)
        headers["Content-Type"] = "application/json"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # VALR authenticates the account socket via upgrade headers (ws_auth_headers),
        # not via a per-message signature.
        return request

    def ws_auth_headers(self, path: str = "/ws/account") -> Dict[str, str]:
        """Return the headers to authenticate the private WebSocket upgrade (signs GET + path)."""
        timestamp = self._timestamp_ms()
        signature = self._sign(timestamp, "GET", path, "")
        return {
            CONSTANTS.HEADER_API_KEY: self.api_key,
            CONSTANTS.HEADER_SIGNATURE: signature,
            CONSTANTS.HEADER_TIMESTAMP: str(timestamp),
        }
