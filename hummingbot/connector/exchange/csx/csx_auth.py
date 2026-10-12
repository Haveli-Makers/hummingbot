import json
from urllib.parse import urlparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

# CSX rejects any request whose timestamp is in the future ("Request cannot be for
# a future time"). Since the connector signs with the local clock (CSX has no
# server-time endpoint), even ~1s of client-clock-ahead skew causes intermittent
# rejections. Back-date the signed timestamp by this many seconds so it is never
# in the future, while staying comfortably inside CSX's (generous) staleness window.
REQUEST_TIME_BUFFER_S = 2


class CsxAuth(AuthBase):
    """
    CoinSwitch Kuber (CSX) authentication using Ed25519 signatures.

    Headers added to every authenticated request:
      CSX-ACCESS-KEY       – public API key
      CSX-SIGNATURE        – hex-encoded Ed25519 signature
      CSX-ACCESS-TIMESTAMP – current epoch time in **seconds** (integer string)

    Signature message format (mirrors the official docs' gen_sign()):
      message = timestamp + METHOD + url_path + body

    where:
      - url_path includes the RAW (non-url-encoded) query string for GET requests.
        The CSX server percent-decodes the query before verifying, so the
        signature must use the raw values (e.g. "?status=in:OPEN,PARTIALLY_FILLED",
        NOT "?status=in%3AOPEN%2CPARTIALLY_FILLED").
      - body is "{}" when there is no request body (all GETs, body-less DELETEs);
        otherwise it is the request body as compact JSON with sorted keys.
        The same serialized string is written back to request.data so the bytes
        on the wire match exactly what was signed.

    Verified working examples (HTTP 200 against production):
      GET  /api/v2/me/balance/   → "1780312873GET/api/v2/me/balance/{}"
      GET  /api/v1/me/orders/    → "...GET/api/v1/me/orders/?onlyOpen=true{}"
      POST /api/v2/orders/       → "...POST/api/v2/orders/{\"instrument\":\"BTC/INR\",...}"
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

        timestamp = str(int(self._time_provider.time()) - REQUEST_TIME_BUFFER_S)

        method_str = request.method.name if hasattr(request.method, "name") else str(request.method).upper()

        # ── url_path (with raw query string for GET-style requests) ──────────────
        # request.params holds the query parameters as a dict; aiohttp appends
        # them (url-encoded) to the URL on the wire. The CSX server percent-decodes
        # the query before reconstructing the signature, so we sign the RAW values.
        path = urlparse(request.url).path
        if request.params:
            raw_query = "&".join(f"{key}={value}" for key, value in request.params.items())
            url_path = f"{path}?{raw_query}"
        else:
            url_path = path

        # ── body component ───────────────────────────────────────────────────────
        # Empty body  → "{}"  (the docs' gen_sign does: if not body: body = "{}")
        # Non-empty   → compact JSON with sorted keys, written back to request.data
        #               so the bytes sent match exactly what was signed.
        if request.data:
            raw = request.data
            if isinstance(raw, str):
                try:
                    body_obj = json.loads(raw)
                except ValueError:
                    body_obj = {}
            else:
                body_obj = raw
            input_body = json.dumps(body_obj, separators=(",", ":"), sort_keys=True)
            request.data = input_body
        else:
            input_body = "{}"

        message = f"{timestamp}{method_str}{url_path}{input_body}"
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
