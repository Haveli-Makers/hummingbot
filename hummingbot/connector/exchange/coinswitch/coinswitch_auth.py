import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoinswitchAuth(AuthBase):
    """
    CoinSwitch authentication using Ed25519 signature generation.
    Uses X-AUTH-SIGNATURE, X-AUTH-APIKEY, and X-AUTH-EPOCH headers.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        if api_key or secret_key:
            try:
                secret_key_bytes = bytes.fromhex(secret_key)
            except ValueError:
                raise ValueError(
                    "CoinSwitch API secret must be a hex-encoded Ed25519 private key. "
                    "Ensure the secret key contains only hexadecimal characters."
                )
            if len(secret_key_bytes) != 32:
                raise ValueError(
                    f"CoinSwitch API secret must be a 32-byte (64 hex character) Ed25519 private key, "
                    f"got {len(secret_key_bytes)} bytes ({len(secret_key)} hex characters)."
                )
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider
        self._nonce_lock = asyncio.Lock()
        self._last_timestamp = 0

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication headers to the request including signature.

        CoinSwitch API authentication (new format since July 2024):
        - All requests: X-AUTH-EPOCH header included
        - GET:    signature = method + endpoint_with_query_params + epoch_time
        - POST/DELETE: signature = method + endpoint + epoch_time  (body NOT in signature)
        """
        import json
        from urllib.parse import urlparse

        if not self.api_key or not self.secret_key:
            return request

        headers = request.headers or {}

        epoch_time = await self._get_timestamp()

        parsed_url = urlparse(request.url)
        endpoint = parsed_url.path

        method_str = request.method.name if hasattr(request.method, 'name') else str(request.method).upper()

        if method_str == "GET":
            params = request.params or {}
        else:
            if request.data:
                if isinstance(request.data, str):
                    params = json.loads(request.data)
                else:
                    params = request.data
            else:
                params = {}

            sorted_body = json.dumps(params, separators=(',', ':'), sort_keys=True)
            request.data = sorted_body

        signature = self._generate_signature(method_str, endpoint, params, epoch_time)

        headers["X-AUTH-APIKEY"] = self.api_key
        headers["X-AUTH-SIGNATURE"] = signature
        headers["X-AUTH-EPOCH"] = epoch_time
        headers["Content-Type"] = "application/json"

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Authenticate a WebSocket request.
        Args:
            request: The WebSocket request to authenticate

        Returns:
            The authenticated WebSocket request
        """
        epoch_time = await self._get_timestamp()
        auth_message = f"GET/ws/auth{epoch_time}"
        signature = self._generate_ws_signature(auth_message, epoch_time)

        if not hasattr(request, "headers") or request.headers is None:
            request.headers = {}

        request.headers["X-AUTH-APIKEY"] = self.api_key
        request.headers["X-AUTH-SIGNATURE"] = signature
        request.headers["X-AUTH-EPOCH"] = epoch_time

        return request

    def _generate_ws_signature(self, message: str, epoch_time: str) -> str:
        """
        Generate Ed25519 signature for WebSocket authentication.

        Args:
            message: The message to sign
            epoch_time: Epoch time in milliseconds

        Returns:
            Hex-encoded signature
        """
        request_string = bytes(message, 'utf-8')
        secret_key_bytes = bytes.fromhex(self.secret_key)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key_bytes)
        signature_bytes = private_key.sign(request_string)

        return signature_bytes.hex()

    async def _get_timestamp(self) -> str:
        """
        Get epoch time in milliseconds, ensuring uniqueness for rapid requests.
        """
        async with self._nonce_lock:
            epoch_time = int(self._time_provider.time() * 1000)

            if epoch_time <= self._last_timestamp:
                epoch_time = self._last_timestamp + 1

            self._last_timestamp = epoch_time
            return str(epoch_time)

    def _generate_signature(self, method: str, endpoint: str, params: dict, epoch_time: str) -> str:
        """
        Generate Ed25519 signature for the request.

        Signature message format (new CoinSwitch format since July 2024):
        - GET:    METHOD + endpoint_with_query_params + epoch_time
        - POST/DELETE: METHOD + endpoint + epoch_time  (body is NOT part of signature)

        Args:
            method: HTTP method (GET, POST, DELETE)
            endpoint: API endpoint path
            params: Query parameters (for GET); ignored in signature for POST/DELETE
            epoch_time: Epoch time in milliseconds

        Returns:
            Hex-encoded signature
        """

        if method == "GET":
            if params:
                sorted_params = sorted(params.items())
                query_parts = []
                for key, value in sorted_params:
                    query_parts.append(f"{key}={value}")
                query_string = "&".join(query_parts)
                endpoint_with_params = endpoint + "?" + query_string
            else:
                endpoint_with_params = endpoint

            signature_msg = method + endpoint_with_params + epoch_time
        else:
            signature_msg = method + endpoint + epoch_time

        request_string = bytes(signature_msg, 'utf-8')
        secret_key_bytes = bytes.fromhex(self.secret_key)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key_bytes)
        signature_bytes = private_key.sign(request_string)

        return signature_bytes.hex()
