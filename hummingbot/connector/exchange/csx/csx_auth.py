import json
from urllib.parse import urlparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CsxAuth(AuthBase):
    """
    CoinSwitch Kuber (CSX) authentication using Ed25519 signatures.

    Headers added to every authenticated request:
      CSX-ACCESS-KEY       – public API key
      CSX-SIGNATURE        – hex-encoded Ed25519 signature
      CSX-ACCESS-TIMESTAMP – current epoch time in **seconds** (integer string)

    Signature message format:
      <timestamp_seconds><HTTP_METHOD><url_path><compact_sorted_json_body>

    Examples:
      GET  /api/v2/me/balance/      → "1725010288GET/api/v2/me/balance/"
      POST /api/v2/orders/          → "1725010288POST/api/v2/orders/{"instrument":"BTC/INR",...}"
      DELETE /api/v1/orders/{id}    → "1725010288DELETE/api/v1/orders/{id}"
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        if api_key or secret_key:
            try:
                secret_key_bytes = bytes.fromhex(secret_key)
            except ValueError:
                raise ValueError(
                    "CSX API secret must be a hex-encoded Ed25519 private key. "
                    "Ensure the key contains only hexadecimal characters."
                )
            if len(secret_key_bytes) != 32:
                raise ValueError(
                    f"CSX API secret must be a 32-byte (64 hex character) Ed25519 private key, "
                    f"got {len(secret_key_bytes)} bytes ({len(secret_key)} hex characters)."
                )
            self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key_bytes)
        else:
            self._private_key = None

        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.api_key or not self._private_key:
            return request

        timestamp = str(int(self._time_provider.time()))

        parsed = urlparse(request.url)
        path = parsed.path

        method_str = request.method.name if hasattr(request.method, "name") else str(request.method).upper()

        body_str = ""
        if method_str == "POST" and request.data:
            raw = request.data
            if isinstance(raw, str):
                try:
                    parsed_body = json.loads(raw)
                except ValueError:
                    parsed_body = {}
            else:
                parsed_body = raw
            body_str = json.dumps(parsed_body, separators=(",", ":"), sort_keys=True)
            request.data = body_str

        message = f"{timestamp}{method_str}{path}{body_str}"
        signature = self._sign(message)

        headers = request.headers or {}
        headers["CSX-ACCESS-KEY"] = self.api_key
        headers["CSX-SIGNATURE"] = signature
        headers["CSX-ACCESS-TIMESTAMP"] = timestamp
        headers["Content-Type"] = "application/json"
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def _sign(self, message: str) -> str:
        raw = self._private_key.sign(message.encode("utf-8"))
        return raw.hex()
