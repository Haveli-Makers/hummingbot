import hashlib
import hmac
import json
from typing import Any, Dict

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoinDCXPerpetualAuth(AuthBase):
    """
    CoinDCX futures authentication.

    Every private request carries a JSON body containing a millisecond
    ``timestamp``; the body is serialised compactly and signed with
    HMAC-SHA256, and the hex digest is sent in ``X-AUTH-SIGNATURE`` alongside
    ``X-AUTH-APIKEY``.

    Unlike the spot connector this signs **every** method, not just POST: the
    futures wallet endpoint is a GET that still carries a signed body.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        body: Dict[str, Any]
        if request.data:
            body = json.loads(request.data) if isinstance(request.data, str) else dict(request.data)
        else:
            body = {}

        body.setdefault("timestamp", self._timestamp_ms())

        json_body = json.dumps(body, separators=(",", ":"))
        request.data = json_body

        headers = dict(request.headers) if request.headers else {}
        headers.update(self.header_for_authentication(self._generate_signature(json_body)))
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The Socket.IO private channel is authenticated with the join payload
        # produced by ``generate_ws_auth_payload``, not per-frame.
        return request

    def generate_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Payload for ``emit("join", ...)`` on the private ``coindcx`` channel.
        """
        json_body = json.dumps({"channel": CONSTANTS.PRIVATE_CHANNEL}, separators=(",", ":"))
        return {
            "channelName": CONSTANTS.PRIVATE_CHANNEL,
            "authSignature": self._generate_signature(json_body),
            "apiKey": self.api_key,
        }

    def header_for_authentication(self, signature: str) -> Dict[str, str]:
        return {
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

    def _timestamp_ms(self) -> int:
        return int(self.time_provider.time() * 1e3)

    def _generate_signature(self, payload: str) -> str:
        secret_bytes = bytes(self.secret_key, encoding="utf-8")
        return hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()
