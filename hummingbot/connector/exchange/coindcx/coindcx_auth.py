import hashlib
import hmac
import json
import time
from typing import Any, Dict

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class CoinDCXAuth(AuthBase):
    """
    CoinDCX uses HMAC SHA256 signature for authentication.
    The signature is generated from the JSON body of the request.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the authentication headers to the request.
        CoinDCX requires:
        - X-AUTH-APIKEY: API key
        - X-AUTH-SIGNATURE: HMAC SHA256 signature of the JSON body
        
        :param request: the request to be configured for authenticated interaction
        """
        if request.method == RESTMethod.POST:
            if request.data:
                if isinstance(request.data, str):
                    body = json.loads(request.data)
                else:
                    body = request.data
            else:
                body = {}
            
            timestamp = int(time.time() * 1000)
            body["timestamp"] = timestamp
            
            json_body = json.dumps(body, separators=(',', ':'))
            
            signature = self._generate_signature(json_body)
            
            request.data = json_body
            
            headers = {}
            if request.headers is not None:
                headers.update(request.headers)
            headers.update(self.header_for_authentication(signature))
            headers["Content-Type"] = "application/json"
            request.headers = headers
        
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated.
        CoinDCX WebSocket authentication uses a channel-based approach.
        """
        return request  

    def generate_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Generates the authentication payload for WebSocket connection.
        Used when joining the private 'coindcx' channel.
        """
        body = {"channel": "coindcx"}
        json_body = json.dumps(body, separators=(',', ':'))
        signature = self._generate_signature(json_body)
        
        return {
            "channelName": "coindcx",
            "authSignature": signature,
            "apiKey": self.api_key
        }

    def header_for_authentication(self, signature: str) -> Dict[str, str]:
        """
        Returns the headers required for authentication.
        """
        return {
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature
        }

    def _generate_signature(self, payload: str) -> str:
        """
        Generates HMAC SHA256 signature for the given payload.
        
        :param payload: JSON string of the request body
        :return: Hexadecimal signature string
        """
        secret_bytes = bytes(self.secret_key, encoding='utf-8')
        signature = hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()
        return signature
