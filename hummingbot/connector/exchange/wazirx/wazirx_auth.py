import hashlib
import hmac

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class WazirxAuth(AuthBase):
    """
    WazirX authentication.

    WazirX signed endpoints typically require an HMAC-SHA256 signature of the query string
    using the secret key and the API key sent in header `X-Api-Key`.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {} if request.headers is None else dict(request.headers)
        headers["X-Api-Key"] = self.api_key

        try:
            if request.params is None:
                request.params = {}
            timestamp = int(self.time_provider.time() * 1000)
            request.params["timestamp"] = timestamp

            items = []
            for k in sorted(request.params.keys()):
                items.append(f"{k}={request.params[k]}")
            query_string = "&".join(items)

            signature = self._generate_signature(query_string)
            request.params["signature"] = signature

            request.headers = headers
        except Exception:
            request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def _generate_signature(self, payload: str) -> str:
        secret_bytes = bytes(self.secret_key, encoding="utf-8")
        return hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()
