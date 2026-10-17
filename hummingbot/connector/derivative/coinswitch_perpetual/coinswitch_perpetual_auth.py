import asyncio
import json
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus, urlencode, urlparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoinswitchPerpetualAuth(AuthBase):
    """
    Ed25519 request signing for the CoinSwitch PRO Futures v2 API (same scheme as spot).

    The signed message is ``METHOD + path_with_query + epoch``, where the query is
    URL-DECODED and the epoch (Unix ms) is also sent as ``X-AUTH-EPOCH``. A JSON
    body is never part of the message.

    Query parameters are sorted, and the request is sent with them in that same
    order, so the bytes signed are exactly the query the server receives whether
    it verifies against the raw query or a canonical (sorted) one.

    A connector built without keys (rate and volume oracles, the CLI's trading-pair
    fetcher) leaves requests unsigned.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""
        self._private_key: Optional[ed25519.Ed25519PrivateKey] = None
        if self.api_key or self.secret_key:
            self._private_key = self._load_private_key(self.secret_key)
        self._time_provider = time_provider
        self._epoch_lock = asyncio.Lock()
        self._last_epoch = 0

    @staticmethod
    def _load_private_key(secret_key: str) -> ed25519.Ed25519PrivateKey:
        try:
            secret_bytes = bytes.fromhex(secret_key)
        except ValueError:
            raise ValueError("The CoinSwitch API secret must be a hex-encoded Ed25519 private key.")
        if len(secret_bytes) != 32:
            raise ValueError(
                f"The CoinSwitch API secret must be a 32-byte (64 hex character) Ed25519 private key, "
                f"got {len(secret_bytes)} bytes.")
        return ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key) and self._private_key is not None

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if not self.has_credentials:
            return request

        params = self._sorted_params(request.params)
        if params is not None:
            request.params = params
        if isinstance(request.data, str):
            request.data = self.plain_number_json(request.data)

        epoch = await self._next_epoch()
        method = request.method.name if hasattr(request.method, "name") else str(request.method).upper()
        message = self.signature_message(method, urlparse(request.url).path, params, epoch)

        headers = dict(request.headers or {})
        headers.update({
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": self._sign(message),
            "X-AUTH-EPOCH": epoch,
        })
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The futures sockets are public; nothing to sign.
        return request

    @classmethod
    def plain_number_json(cls, body: str) -> str:
        """
        Re-renders a JSON body with every number in plain decimal notation. The
        REST assistant serialises floats with ``json.dumps``, which writes small
        values such as a 0.00001 price as ``1e-05``; that is valid JSON, but the
        exchange's number validation is not documented, so it is not risked.
        """
        try:
            parsed = json.loads(body, parse_float=Decimal)
        except ValueError:
            return body
        return cls._dump_plain(parsed)

    @classmethod
    def _dump_plain(cls, value: Any) -> str:
        if isinstance(value, dict):
            return "{" + ",".join(f"{json.dumps(str(key))}:{cls._dump_plain(item)}"
                                  for key, item in value.items()) + "}"
        if isinstance(value, list):
            return "[" + ",".join(cls._dump_plain(item) for item in value) + "]"
        if isinstance(value, Decimal):
            return format(value, "f")
        return json.dumps(value)

    @staticmethod
    def _sorted_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        if not params:
            return None
        return {str(key): str(value) for key, value in sorted(params.items()) if value is not None}

    @staticmethod
    def signature_message(method: str, path: str, params: Optional[Dict[str, Any]], epoch: str) -> str:
        """``METHOD + path[?decoded query] + epoch``, the bytes the server verifies."""
        path_with_query = path
        if params:
            path_with_query = f"{path}?{unquote_plus(urlencode(params))}"
        return f"{method.upper()}{path_with_query}{epoch}"

    def _sign(self, message: str) -> str:
        return self._private_key.sign(message.encode("utf-8")).hex()

    async def _next_epoch(self) -> str:
        """Server-synchronised Unix ms, strictly increasing across concurrent requests."""
        async with self._epoch_lock:
            epoch = int(self._time_provider.time() * 1e3)
            if epoch <= self._last_epoch:
                epoch = self._last_epoch + 1
            self._last_epoch = epoch
            return str(epoch)
