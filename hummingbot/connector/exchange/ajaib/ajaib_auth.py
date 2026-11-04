import base64
import json
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest
from hummingbot.logger import HummingbotLogger


class AjaibAuth(AuthBase):
    """
    Ajaib Open API authentication.

    Every signed request must include:
      - ``X-MBX-APIKEY`` header with the API key
      - a ``timestamp`` (UNIX ms) parameter
      - a ``signature`` parameter

    The signature is produced by:
      1. taking every request parameter (incl. ``timestamp``/``recvWindow``) in
         the order it will be TRANSMITTED -- never re-sorted, because the server
         verifies against the query string it actually receives,
      2. building a URL-encoded ``key=value&...`` string,
      3. signing it with the account's Ed25519 private key,
      4. base64-encoding the result (hex is rejected with -1022).

    The Open API documents the key as Ed25519 (despite the "ECDSASHA256" label
    elsewhere); ``_sign`` detects the loaded key type so an EC or RSA PEM also
    works without code changes.
    """

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self._secret_key = secret_key
        self.time_provider = time_provider
        self._private_key: Optional[Any] = None
        self._load_private_key()

    def _load_private_key(self):
        """
        Load the Ed25519 key from PEM contents or a path to a .pem file.

        Failures are recorded and logged rather than swallowed. A key that fails
        to load used to leave ``_private_key`` as None, which made ``_sign``
        return an empty signature -- so a mistyped path surfaced as
        ``-1022 Invalid signature`` on every request, which points the reader at
        the signing algorithm instead of at the missing file.
        """
        self._load_error: Optional[str] = None
        if not self._secret_key:
            # Legitimate: the connector is built without credentials for
            # symbol-map/discovery use.
            return

        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        secret = self._secret_key
        looks_like_pem = secret.lstrip().startswith("-----")

        if looks_like_pem:
            pem_data = secret.encode("utf-8")
        else:
            path = secret.strip()
            if not os.path.isfile(path):
                self._load_error = (
                    f"Ajaib API secret does not look like PEM contents and is not a readable "
                    f"file: {path!r}. Pass the path to your Ed25519 .pem file, or its contents.")
                self.logger().error(self._load_error)
                return
            try:
                with open(path, "rb") as handle:
                    pem_data = handle.read()
            except OSError as exception:
                self._load_error = f"Could not read the Ajaib key file {path!r}: {exception}"
                self.logger().error(self._load_error)
                return

        try:
            self._private_key = load_pem_private_key(data=pem_data, password=None)
        except Exception as exception:
            source = "the supplied PEM contents" if looks_like_pem else f"{secret!r}"
            self._load_error = (
                f"Could not load an Ed25519 private key from {source}: "
                f"{type(exception).__name__}: {exception}")
            self.logger().error(self._load_error)
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
            # RESTAssistant json.dumps() the payload before auth runs, so the
            # body arrives here as a STRING. Treating only dicts as payloads
            # silently discarded it -- e.g. the listen-key keep-alive lost its
            # {"listenKey": ...} entirely.
            data = self._payload_as_dict(request.data)
            data["timestamp"] = timestamp
            data.setdefault("recvWindow", CONSTANTS.RECV_WINDOW)
            data["signature"] = self._sign(data)

            # Ajaib parses non-GET bodies as FORM data; a JSON body is rejected
            # with -1102 "Bad Request". RESTAssistant defaults the header to
            # application/json for non-GET, so this must OVERRIDE it rather than
            # setdefault -- otherwise the header says JSON while the body is
            # form-encoded and every signed POST/PUT/DELETE fails.
            request.data = urlencode(list(data.items()))
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # The user-data stream is authenticated through the listenKey in the URL,
        # not through the websocket frames themselves.
        return request

    @staticmethod
    def _payload_as_dict(payload: Any) -> Dict[str, Any]:
        """Accept a dict, a JSON string, or nothing and return a plain dict."""
        if isinstance(payload, dict):
            return dict(payload)
        if isinstance(payload, (str, bytes)):
            try:
                parsed = json.loads(payload)
            except (TypeError, ValueError):
                return {}
            return dict(parsed) if isinstance(parsed, dict) else {}
        return {}

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
            if self._secret_key:
                # A secret WAS supplied but could not be loaded. Returning ""
                # here sends an empty signature and the exchange replies
                # "-1022 Invalid signature", hiding the real cause.
                raise ValueError(
                    self._load_error
                    or "Ajaib API secret could not be loaded; cannot sign the request.")
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
