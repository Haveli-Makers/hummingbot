import asyncio
import gzip
import json
from unittest import TestCase

import hummingbot.connector.exchange.coinex.coinex_constants as CONSTANTS
import hummingbot.connector.exchange.coinex.coinex_web_utils as web_utils
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class CoinexWebUtilsTests(TestCase):
    def test_public_and_private_rest_url(self):
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.MARKETS_PATH_URL}",
                         web_utils.public_rest_url(CONSTANTS.MARKETS_PATH_URL))
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}",
                         web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL))

    def test_build_api_factory_has_processors(self):
        factory = web_utils.build_api_factory()
        self.assertEqual(1, len(factory._rest_pre_processors))
        self.assertEqual(1, len(factory._ws_post_processors))
        self.assertIsInstance(factory._ws_post_processors[0], web_utils.CoinexWSPostProcessor)

    def test_get_current_server_time_returns_ms(self):
        ms = asyncio.get_event_loop().run_until_complete(web_utils.get_current_server_time())
        self.assertGreater(ms, 1e12)

    def test_ws_post_processor_decompresses_gzip_binary(self):
        processor = web_utils.CoinexWSPostProcessor()
        payload = {"method": "depth.update", "data": {"market": "BTCUSDT"}}
        compressed = gzip.compress(json.dumps(payload).encode("utf-8"))
        out = asyncio.get_event_loop().run_until_complete(processor.post_process(WSResponse(compressed)))
        self.assertEqual(payload, out.data)

    def test_ws_post_processor_passes_through_dicts(self):
        processor = web_utils.CoinexWSPostProcessor()
        payload = {"id": 1, "code": 0, "message": "OK"}
        out = asyncio.get_event_loop().run_until_complete(processor.post_process(WSResponse(payload)))
        self.assertEqual(payload, out.data)
