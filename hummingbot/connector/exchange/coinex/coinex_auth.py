import hashlib
import hmac
import json
from typing import Dict
from urllib.parse import urlparse

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoinexAuth(AuthBase):
    """
    CoinEx v2 authentication using HMAC-SHA256.

    REST: every authenticated request carries headers
      X-COINEX-KEY        – the access id (public key)
      X-COINEX-SIGN       – lowercase hex HMAC-SHA256 of the prehash string
      X-COINEX-TIMESTAMP  – request time in **milliseconds**

    Prehash string (per the docs):
      method + request_path + body + timestamp
        - request_path includes the leading "/v2/…" and, for GET, the
          "?query" string (empty for requests without query params)
        - body is the raw JSON string (empty for GET)
        - timestamp is appended directly (no separator), in milliseconds

    WebSocket: a single `server.sign` message authenticates the socket; its
    signature is HMAC-SHA256 of just the timestamp string (see ws_auth_payload).
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self.secret_key:
            return request

        timestamp = str(int(self._time_provider.time() * 1000))  # epoch milliseconds
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

        prehash = f"{method}{path}{query_string}{body_str}{timestamp}"
        signature = self._sign(prehash)

        headers: Dict = request.headers or {}
        headers["X-COINEX-KEY"] = self.api_key
        headers["X-COINEX-SIGN"] = signature
        headers["X-COINEX-TIMESTAMP"] = timestamp
        headers["Content-Type"] = "application/json"
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # CoinEx authenticates the socket with a single `server.sign` message
        # (built via ws_auth_payload), not per-request signing.
        return request

    def ws_auth_payload(self, request_id: int = 1) -> Dict:
        """Return the `server.sign` message to send right after the WebSocket connects."""
        timestamp = int(self._time_provider.time() * 1000)
        signature = self._sign(str(timestamp))
        return {
            "method": "server.sign",
            "params": {
                "access_id": self.api_key,
                "signed_str": signature,
                "timestamp": timestamp,
            },
            "id": request_id,
        }

    def _sign(self, message: str) -> str:
        return hmac.new(
            self.secret_key.encode("latin-1"),
            message.encode("latin-1"),
            hashlib.sha256,
        ).hexdigest().lower()
