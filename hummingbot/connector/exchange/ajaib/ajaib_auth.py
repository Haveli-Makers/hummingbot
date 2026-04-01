import base64
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AjaibAuth(AuthBase):
    """
    Ajaib uses Ed25519 signatures for authentication.
    All requests are signed with the Ed25519 private key.
    The API key is sent via the X-MBX-APIKEY header.
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
            if not pem_data.startswith("-----"):
                try:
                    with open(pem_data, 'rb') as f:
                        pem_data = f.read()
                except (FileNotFoundError, OSError):
                    pem_data = pem_data.encode('utf-8')
            else:
                pem_data = pem_data.encode('utf-8')

            self._private_key = load_pem_private_key(data=pem_data, password=None)
        except Exception:
            self._private_key = None

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds Ed25519 signature and API key header to the request.
        """
        headers = dict(request.headers) if request.headers else {}
        headers["X-MBX-APIKEY"] = self.api_key

        timestamp = int(time.time() * 1000)

        if request.method in (RESTMethod.GET, RESTMethod.DELETE):
            headers.pop("Content-Type", None)
            params = dict(request.params) if request.params else {}
            params["timestamp"] = timestamp
            payload = urlencode(list(params.items()))
            signature = self._sign(payload)
            params["signature"] = signature
            request.params = params
        else:
            if request.data:
                if isinstance(request.data, dict):
                    data = request.data.copy()
                else:
                    data = {}
            else:
                data = {}
            data["timestamp"] = timestamp
            payload = urlencode(list(data.items()))
            signature = self._sign(payload)
            data["signature"] = signature
            request.data = data
            if "Content-Type" not in headers:
                headers["Content-Type"] = "application/x-www-form-urlencoded"

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def _sign(self, payload: str) -> str:
        """
        Sign the payload with Ed25519 private key and return base64-encoded signature.
        """
        if self._private_key is None:
            return ""
        signature = self._private_key.sign(payload.encode('ASCII'))
        return base64.b64encode(signature).decode('ASCII')

    def header_for_authentication(self) -> Dict[str, str]:
        return {
            "X-MBX-APIKEY": self.api_key,
        }
