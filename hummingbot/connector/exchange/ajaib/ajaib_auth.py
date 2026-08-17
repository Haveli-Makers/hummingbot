import base64
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AjaibAuth(AuthBase):
    """
    Ajaib Open API authentication.

    Every signed request must include:
      - ``X-MBX-APIKEY`` header with the API key
      - a ``timestamp`` (UNIX ms) parameter
      - a ``signature`` parameter

    The signature is produced by:
      1. arranging every request parameter (incl. ``timestamp``/``recvWindow``)
         alphabetically by key,
      2. building a URL-encoded ``key=value&...`` query string,
      3. signing that string with the account's Ed25519 private key,
      4. base64-encoding the result.

    The Open API documents the key as Ed25519 (despite the "ECDSASHA256" label
    elsewhere); ``_sign`` detects the loaded key type so an EC or RSA PEM also
    works without code changes.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self._secret_key = secret_key
        self.time_provider = time_provider
        self._private_key: Optional[Any] = None
        self._load_private_key()

    def _load_private_key(self):
        if not self._secret_key:
            return

        try:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key

            pem_data = self._secret_key
            if not pem_data.lstrip().startswith("-----"):
                # Treat the secret as a path to a PEM file; fall back to raw bytes.
                try:
                    with open(pem_data, "rb") as f:
                        pem_data = f.read()
                except (FileNotFoundError, OSError):
                    pem_data = pem_data.encode("utf-8")
            else:
                pem_data = pem_data.encode("utf-8")

            self._private_key = load_pem_private_key(data=pem_data, password=None)
        except Exception:
            self._private_key = None

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers) if request.headers else {}
        headers["X-MBX-APIKEY"] = self.api_key

        timestamp = int(self.time_provider.time() * 1e3)

        if request.method in (RESTMethod.GET, RESTMethod.DELETE):
            headers.pop("Content-Type", None)
            params = dict(request.params) if request.params else {}
            params["timestamp"] = timestamp
            params.setdefault("recvWindow", CONSTANTS.RECV_WINDOW)
            params["signature"] = self._sign(params)
            request.params = params
        else:
            data = dict(request.data) if isinstance(request.data, dict) else {}
            data["timestamp"] = timestamp
            data.setdefault("recvWindow", CONSTANTS.RECV_WINDOW)
            data["signature"] = self._sign(data)
            request.data = data
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The user-data stream is authenticated through the listenKey in the URL,
        # not through the websocket frames themselves.
        return request

    def _sign(self, params: Dict[str, Any]) -> str:
        """
        Sign the parameters EXACTLY as they will be transmitted and return the
        base64-encoded signature.

        The server verifies against the query string it receives, so the signed
        bytes must match the wire order. Sorting here while sending the dict in
        insertion order produced a valid-looking signature over a different
        string and every request failed with ``-1022 Invalid signature``.
        Ordering itself is not significant to Ajaib -- verified live, both
        sorted and insertion order authenticate -- but signed and sent must
        agree, so this signs the caller's order and never reorders.
        """
        if self._private_key is None:
            return ""

        payload = self.signature_payload(params).encode("ascii")
        signature = self._raw_sign(payload)
        return base64.b64encode(signature).decode("ascii")

    @staticmethod
    def signature_payload(params: Dict[str, Any]) -> str:
        """The exact string that is signed, in transmission order."""
        return urlencode(list(params.items()))

    def _raw_sign(self, payload: bytes) -> bytes:
        key = self._private_key

        from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PrivateKey
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        if isinstance(key, (Ed25519PrivateKey, Ed448PrivateKey)):
            return key.sign(payload)

        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec, padding

        if isinstance(key, ec.EllipticCurvePrivateKey):
            return key.sign(payload, ec.ECDSA(hashes.SHA256()))

        return key.sign(payload, padding.PKCS1v15(), hashes.SHA256())

    def header_for_authentication(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self.api_key}
