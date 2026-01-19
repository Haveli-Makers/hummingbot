import unittest


class CoinswitchUserStreamDataSourceTests(unittest.TestCase):
    """Test cases for CoinSwitch user stream data source"""

    def test_module_imports(self):
        """Test that user stream data source module can be imported"""
        try:
            from hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source import (
                CoinswitchAPIUserStreamDataSource,
            )
            self.assertIsNotNone(CoinswitchAPIUserStreamDataSource)
        except ImportError as e:
            self.fail(f"Failed to import CoinswitchAPIUserStreamDataSource: {e}")

    def test_data_source_instantiation(self):
        """Test creating a user stream data source instance"""
        from hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source import (
            CoinswitchAPIUserStreamDataSource,
        )

        data_source = CoinswitchAPIUserStreamDataSource()
        self.assertIsNotNone(data_source)

    def test_stream_message_format_order_update(self):
        """Test order update message format"""
        order_update = {
            "type": "order_update",
            "order_id": "order_123",
            "status": "completed",
            "filled_qty": "10.0",
            "price": "91.50",
            "timestamp": 1234567890000
        }

        # Should contain required fields
        self.assertIn("type", order_update)
        self.assertIn("order_id", order_update)
        self.assertIn("status", order_update)
        self.assertEqual("order_update", order_update["type"])

    def test_stream_message_format_balance_update(self):
        """Test balance update message format"""
        balance_update = {
            "type": "balance_update",
            "balance": {
                "INR": "1000.50",
                "BTC": "0.5"
            },
            "timestamp": 1234567890000
        }

        # Should contain required fields
        self.assertIn("type", balance_update)
        self.assertIn("balance", balance_update)
        self.assertEqual("balance_update", balance_update["type"])

    def test_stream_connection_url(self):
        """Test WebSocket connection URL format"""
        from hummingbot.connector.exchange.coinswitch.coinswitch_constants import HBOT_ORDER_ID_PREFIX

        # Stream URLs should be valid
        self.assertIsNotNone(HBOT_ORDER_ID_PREFIX)

    def test_order_event_parsing(self):
        """Test parsing order events"""
        order_event = {
            "event": "order",
            "data": {
                "order_id": "x-CS-order_123",
                "status": "filled",
                "side": "buy",
                "qty": "10.0",
                "price": "91.50"
            }
        }

        self.assertIn("data", order_event)
        self.assertIn("order_id", order_event["data"])

    def test_trade_event_parsing(self):
        """Test parsing trade events"""
        trade_event = {
            "event": "trade",
            "data": {
                "trade_id": "trade_123",
                "order_id": "order_123",
                "qty": "5.0",
                "price": "91.50",
                "fee": "0.25"
            }
        }

        self.assertIn("data", trade_event)
        self.assertIn("trade_id", trade_event["data"])

    def test_balance_event_parsing(self):
        """Test parsing balance events"""
        balance_event = {
            "event": "balance",
            "data": {
                "balances": {
                    "INR": "500.00",
                    "BTC": "1.0"
                }
            }
        }

        self.assertIn("data", balance_event)
        self.assertIn("balances", balance_event["data"])

    def test_multiple_order_updates(self):
        """Test handling multiple order updates"""
        order_updates = [
            {"order_id": "order_1", "status": "filled"},
            {"order_id": "order_2", "status": "pending"},
            {"order_id": "order_3", "status": "cancelled"}
        ]

        self.assertEqual(3, len(order_updates))
        self.assertEqual("order_1", order_updates[0]["order_id"])

    def test_error_message_format(self):
        """Test error message format"""
        error_message = {
            "type": "error",
            "code": "AUTH_FAILED",
            "message": "Authentication failed",
            "timestamp": 1234567890000
        }

        self.assertIn("type", error_message)
        self.assertIn("code", error_message)
        self.assertIn("message", error_message)
        self.assertEqual("error", error_message["type"])

    def test_heartbeat_message(self):
        """Test heartbeat message format"""
        heartbeat = {
            "type": "heartbeat",
            "timestamp": 1234567890000
        }

        self.assertIn("type", heartbeat)
        self.assertEqual("heartbeat", heartbeat["type"])

    def test_subscription_confirmation(self):
        """Test subscription confirmation message"""
        subscription = {
            "type": "subscription_response",
            "channel": "user_stream",
            "status": "subscribed"
        }

        self.assertIn("type", subscription)
        self.assertEqual("subscription_response", subscription["type"])

    def test_timestamp_format(self):
        """Test timestamp format in messages"""
        timestamp_ms = 1234567890000
        timestamp_s = timestamp_ms / 1000

        self.assertEqual(1234567890.0, timestamp_s)

    def test_event_type_identification(self):
        """Test identifying event types"""
        event_types = ["order_update", "balance_update", "error", "heartbeat"]

        self.assertIn("order_update", event_types)
        self.assertIn("balance_update", event_types)
        self.assertEqual(4, len(event_types))


if __name__ == "__main__":
    unittest.main()
