import asyncio
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class TestCoinDCXAuth(unittest.TestCase):
    """Test cases for CoinDCX authentication."""

    def setUp(self):
        self.api_key = "test_api_key"
        self.secret_key = "test_secret_key"
        self.time_provider = MagicMock()
        self.time_provider.time.return_value = 1640000000.0
        
        self.auth = CoinDCXAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self.time_provider
        )

    def test_auth_initialization(self):
        """Test that auth object initializes correctly."""
        self.assertEqual(self.auth.api_key, self.api_key)
        self.assertEqual(self.auth.secret_key, self.secret_key)

    def test_generate_signature(self):
        """Test that signature generation produces correct HMAC SHA256."""
        payload = '{"test":"data","timestamp":1640000000000}'
        
        # Calculate expected signature
        secret_bytes = bytes(self.secret_key, encoding='utf-8')
        expected_signature = hmac.new(
            secret_bytes, 
            payload.encode(), 
            hashlib.sha256
        ).hexdigest()
        
        # Get actual signature
        actual_signature = self.auth._generate_signature(payload)
        
        self.assertEqual(actual_signature, expected_signature)

    def test_header_for_authentication(self):
        """Test that authentication headers are correctly formatted."""
        test_signature = "test_signature_123"
        
        headers = self.auth.header_for_authentication(test_signature)
        
        self.assertIn("X-AUTH-APIKEY", headers)
        self.assertIn("X-AUTH-SIGNATURE", headers)
        self.assertEqual(headers["X-AUTH-APIKEY"], self.api_key)
        self.assertEqual(headers["X-AUTH-SIGNATURE"], test_signature)

    def test_rest_authenticate_post_request(self):
        """Test REST authentication for POST requests."""
        async def run_test():
            request = RESTRequest(
                method=RESTMethod.POST,
                url="https://api.coindcx.com/exchange/v1/orders/create",
                data={"market": "BTCUSDT", "side": "buy"}
            )
            
            with patch('time.time', return_value=1640000000.0):
                authenticated_request = await self.auth.rest_authenticate(request)
            
            # Check headers are present
            self.assertIn("X-AUTH-APIKEY", authenticated_request.headers)
            self.assertIn("X-AUTH-SIGNATURE", authenticated_request.headers)
            self.assertIn("Content-Type", authenticated_request.headers)
            
            # Check API key is correct
            self.assertEqual(authenticated_request.headers["X-AUTH-APIKEY"], self.api_key)
            
            # Check data has timestamp
            body_data = json.loads(authenticated_request.data)
            self.assertIn("timestamp", body_data)
            
        asyncio.get_event_loop().run_until_complete(run_test())

    def test_rest_authenticate_with_string_data(self):
        """Test REST authentication when data is already a JSON string."""
        async def run_test():
            json_data = json.dumps({"market": "BTCUSDT", "side": "buy"})
            request = RESTRequest(
                method=RESTMethod.POST,
                url="https://api.coindcx.com/exchange/v1/orders/create",
                data=json_data
            )
            
            with patch('time.time', return_value=1640000000.0):
                authenticated_request = await self.auth.rest_authenticate(request)
            
            # Should not raise an error
            body_data = json.loads(authenticated_request.data)
            self.assertIn("timestamp", body_data)
            self.assertIn("market", body_data)
            
        asyncio.get_event_loop().run_until_complete(run_test())

    def test_rest_authenticate_with_empty_data(self):
        """Test REST authentication when data is empty."""
        async def run_test():
            request = RESTRequest(
                method=RESTMethod.POST,
                url="https://api.coindcx.com/exchange/v1/users/balances",
                data=None
            )
            
            with patch('time.time', return_value=1640000000.0):
                authenticated_request = await self.auth.rest_authenticate(request)
            
            # Should still add timestamp
            body_data = json.loads(authenticated_request.data)
            self.assertIn("timestamp", body_data)
            
        asyncio.get_event_loop().run_until_complete(run_test())

    def test_ws_authenticate(self):
        """Test WebSocket authentication returns request unchanged."""
        async def run_test():
            # Use a mock for WSRequest since it's abstract
            from unittest.mock import MagicMock
            
            request = MagicMock()
            request.payload = {"channel": "test"}
            authenticated_request = await self.auth.ws_authenticate(request)
            
            # WebSocket auth should return the request as-is
            self.assertEqual(request, authenticated_request)
            
        asyncio.get_event_loop().run_until_complete(run_test())

    def test_generate_ws_auth_payload(self):
        """Test WebSocket authentication payload generation."""
        payload = self.auth.generate_ws_auth_payload()
        
        self.assertIn("channelName", payload)
        self.assertIn("authSignature", payload)
        self.assertIn("apiKey", payload)
        
        self.assertEqual(payload["channelName"], "coindcx")
        self.assertEqual(payload["apiKey"], self.api_key)
        self.assertIsInstance(payload["authSignature"], str)

    def test_signature_consistency(self):
        """Test that same payload produces same signature."""
        payload = '{"test":"data"}'
        
        sig1 = self.auth._generate_signature(payload)
        sig2 = self.auth._generate_signature(payload)
        
        self.assertEqual(sig1, sig2)

    def test_different_payloads_different_signatures(self):
        """Test that different payloads produce different signatures."""
        payload1 = '{"test":"data1"}'
        payload2 = '{"test":"data2"}'
        
        sig1 = self.auth._generate_signature(payload1)
        sig2 = self.auth._generate_signature(payload2)
        
        self.assertNotEqual(sig1, sig2)


if __name__ == "__main__":
    unittest.main()
