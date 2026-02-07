import asyncio

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class CoinswitchAuth(AuthBase):
    """
    CoinSwitch authentication using Ed25519 signature generation.
    Uses X-AUTH-SIGNATURE, X-AUTH-APIKEY, and X-AUTH-EPOCH headers.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider):
        self.api_key = api_key
        self.secret_key = secret_key
        self._time_provider = time_provider
        self._nonce_lock = asyncio.Lock()
        self._last_timestamp = 0

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication headers to the request including signature.

        CoinSwitch API authentication:
        - GET requests: X-AUTH-EPOCH header included, signature = method + endpoint + epoch
        - POST/DELETE requests: NO X-AUTH-EPOCH header, signature = method + endpoint + json_body
        """
        import json
        from urllib.parse import urlparse

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
        headers["Content-Type"] = "application/json"

        if method_str == "GET":
            headers["X-AUTH-EPOCH"] = epoch_time

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

        if request.headers is None:
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

        Signature message format:
        - GET: METHOD + endpoint_with_query_params + epoch_time
        - POST/DELETE: METHOD + endpoint + json_body (NO epoch_time in signature)

        Args:
            method: HTTP method (GET, POST, DELETE)
            endpoint: API endpoint path
            params: Query parameters (for GET) or body (for POST/DELETE)
            epoch_time: Epoch time in milliseconds

        Returns:
            Hex-encoded signature
        """
        import json

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
            json_body = json.dumps(params, separators=(',', ':'), sort_keys=True) if params else ""
            signature_msg = method + endpoint + json_body

        request_string = bytes(signature_msg, 'utf-8')
        secret_key_bytes = bytes.fromhex(self.secret_key)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key_bytes)
        signature_bytes = private_key.sign(request_string)

        return signature_bytes.hex()

    async def add_auth_params(self, request: object, method: RESTMethod = None, params: dict = None, data: dict = None, headers: dict = None, url: str = None) -> None:
        """
        Add authentication signature to request headers.

        Args:
            request: The request object
            method: HTTP method
            params: Query parameters (GET) or request body (POST/DELETE)
            data: Alternative request body parameter
            headers: Request headers
            url: Request URL
        """
        if headers is None:
            headers = {}

        epoch_time = await self._get_timestamp()

        if url:
            from urllib.parse import urlparse
            parsed_url = urlparse(url)
            endpoint = parsed_url.path
            if parsed_url.query:
                endpoint += "?" + parsed_url.query
        else:
            endpoint = ""

        body_params = data if data is not None else (params if params is not None else {})

        method_str = method.name if hasattr(method, 'name') else str(method).upper()
        signature = self._generate_signature(method_str, endpoint, body_params, epoch_time)

        headers["X-AUTH-SIGNATURE"] = signature
        headers["X-AUTH-EPOCH"] = epoch_time
        headers["X-AUTH-APIKEY"] = self.api_key
        headers["Content-Type"] = "application/json"
