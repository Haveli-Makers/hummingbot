import json
from unittest import TestCase

from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_user_stream_data_source import unwrap_frame

# A real payload captured from wss://stream.coindcx.com.
PAYLOAD = {
    "ts": 1785133949849,
    "vs": 210599929,
    "asks": {"65480": "0.07", "65479.6": "0.414"},
    "bids": {"65479": "0.997", "65479.5": "19.792"},
    "type": "depth-snapshot",
    "pts": 1785133949849,
    "pr": "futures",
    "s": "BTCUSDT",
}


class UnwrapFrameTests(TestCase):
    def test_unwraps_json_string_data(self):
        # Frames arrive as {"event": ..., "data": "<json string>"}.
        frame = {"event": "depth-snapshot", "data": json.dumps(PAYLOAD)}
        self.assertEqual(PAYLOAD, unwrap_frame(frame))

    def test_accepts_plain_dict_data(self):
        self.assertEqual(PAYLOAD, unwrap_frame({"event": "x", "data": PAYLOAD}))

    def test_accepts_bare_dict(self):
        self.assertEqual(PAYLOAD, unwrap_frame(PAYLOAD))

    def test_returns_none_on_garbage(self):
        self.assertIsNone(unwrap_frame("not json"))
        self.assertIsNone(unwrap_frame(None))
        self.assertIsNone(unwrap_frame({"event": "x", "data": "[1,2]"}))
