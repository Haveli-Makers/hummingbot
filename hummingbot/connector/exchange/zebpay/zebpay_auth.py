import hashlib
import hmac
import json
from typing import Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class ZebpayAuth(AuthBase):
    """
    Zebpay spot authentication using HMAC-SHA256.

    Headers added to every authenticated request:
      x-auth-apikey    – public API key
      x-auth-signature – hex HMAC-SHA256 signature

    Signing scheme (per the Zebpay spot API reference):
      • A millisecond `timestamp` is added to the request.
      • GET / DELETE (query-parameter requests):
            sign the query string  "k1=v1&k2=v2&...&timestamp=<ms>"
            and send those same params (timestamp included) on the wire.
      • POST / PUT (body requests):
            add `timestamp` into the JSON body, serialise it, sign that exact
            string, and send the same string as the body.

    The HMAC is computed with the API secret as the key.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self.secret_key:
            return request

        timestamp = int(self._time_provider.time() * 1000)  # epoch milliseconds

        if request.data:
            # Body request (POST/PUT): sign the JSON body with timestamp embedded.
            raw = request.data
            if isinstance(raw, str):
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = {}
            else:
                body = dict(raw)
            body["timestamp"] = timestamp
            json_body = json.dumps(body, separators=(",", ":"))
            signature = self._sign(json_body)
            request.data = json_body
        else:
            # Query request (GET/DELETE): sign the query string with timestamp included.
            params: Dict = dict(request.params or {})
            params["timestamp"] = timestamp
            query_string = "&".join(f"{key}={value}" for key, value in params.items())
            signature = self._sign(query_string)
            request.params = params

        headers = request.headers or {}
        headers["x-auth-apikey"] = self.api_key
        headers["x-auth-signature"] = signature
        headers["Content-Type"] = "application/json"
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # Zebpay spot has no authenticated WebSocket stream.
        return request

    def _sign(self, message: str) -> str:
        return hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
