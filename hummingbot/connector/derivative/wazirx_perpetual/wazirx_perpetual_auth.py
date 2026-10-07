import hashlib
import hmac
import json
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class WazirxPerpetualAuth(AuthBase):
    """
    WazirX futures (FAPI) authentication — the same scheme as the spot SAPI.

    Every signed request carries ``timestamp`` and ``signature``, where the
    signature is the hex HMAC-SHA256 of the parameter string exactly as it is
    sent: the query string for GET, the form-encoded body for POST and DELETE.
    The API key travels in the ``X-Api-Key`` header.

    The signed string is placed on the request verbatim (in the URL for GET, as
    the body otherwise) instead of being handed to aiohttp as a dict, so the
    bytes on the wire are the bytes that were signed.

    Timestamps are strictly increasing: WazirX rejects a non-GET request whose
    timestamp was already used within the recvWindow (error 2006, "The tonce has
    already been used"), which two orders sent in the same millisecond would
    otherwise trigger.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider
        self._last_timestamp_ms = 0

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        params: Dict[str, Any] = OrderedDict()
        params.update(request.params or {})
        if request.data:
            body = json.loads(request.data) if isinstance(request.data, str) else request.data
            params.update(body or {})

        signed_params, payload = self.add_auth_to_params(params)

        headers = dict(request.headers) if request.headers else {}
        headers.update(self.header_for_authentication())

        if request.method == RESTMethod.GET:
            request.url = f"{request.url}?{payload}"
            request.params = None
            request.data = None
            headers.pop("Content-Type", None)
        else:
            request.params = None
            request.data = payload
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # Private streams authenticate with the auth_key inside the subscribe
        # message, not per frame.
        return request

    def add_auth_to_params(self, params: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], str]:
        """
        Returns the parameters with ``timestamp`` and ``signature`` appended, and
        the exact signed string (signature included) to send.
        """
        request_params: Dict[str, Any] = OrderedDict()
        for key, value in (params or {}).items():
            if value is not None:
                request_params[key] = value
        request_params["timestamp"] = self._next_timestamp_ms()

        payload = urlencode(request_params)
        signature = self.generate_signature(payload)
        request_params["signature"] = signature
        return request_params, f"{payload}&signature={signature}"

    def generate_signature(self, payload: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def header_for_authentication(self) -> Dict[str, str]:
        return {"X-Api-Key": self.api_key}

    def _next_timestamp_ms(self) -> int:
        timestamp = int(self.time_provider.time() * 1e3)
        if timestamp <= self._last_timestamp_ms:
            timestamp = self._last_timestamp_ms + 1
        self._last_timestamp_ms = timestamp
        return timestamp
