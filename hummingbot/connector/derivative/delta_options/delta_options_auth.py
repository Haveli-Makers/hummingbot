import hashlib
import hmac
import json
from typing import Any, Dict
from urllib.parse import urlparse

from yarl import URL

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class DeltaOptionsAuth(AuthBase):
    """
    REST requests are signed with HMAC-SHA256 of method + timestamp + path + query string + body, where the
    timestamp is in seconds and the signature is valid for 5 seconds. The private WebSocket is opened with one
    `key-auth` message signed over "GET" + timestamp + "/live".
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = str(int(self._time_provider.time()))
        query_string = ""
        if request.params:
            # Sign the exact query string aiohttp sends, which yarl builds.
            query_string = f"?{URL(request.url).extend_query(request.params).query_string}"
        body = ""
        if request.data:
            body = request.data if isinstance(request.data, str) else json.dumps(request.data, separators=(",", ":"))
            request.data = body

        signature = self._sign(f"{request.method.value}{timestamp}{urlparse(request.url).path}{query_string}{body}")
        headers = dict(request.headers or {})
        headers.update({
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp,
            "User-Agent": CONSTANTS.USER_AGENT,
            "Content-Type": "application/json",
        })
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def ws_auth_payload(self) -> Dict[str, Any]:
        timestamp = str(int(self._time_provider.time()))
        return {
            "type": CONSTANTS.WS_AUTH_TYPE,
            "payload": {
                "api-key": self.api_key,
                "signature": self._sign(f"GET{timestamp}{CONSTANTS.WS_AUTH_PATH}"),
                "timestamp": timestamp,
            },
        }

    def _sign(self, message: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
