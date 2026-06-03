from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Dict, List, Optional

from hummingbot.core.volume_oracle.sources.binance_volume_source import BinanceVolumeSource
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase
from hummingbot.core.volume_oracle.volume_oracle import VolumeOracle


def _make_volume_dict(exchange: str, symbol: str, base_vol: str, last: str) -> Dict[str, Decimal]:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "base_volume": Decimal(base_vol),
        "last_price": Decimal(last),
    }


class DummyVolumeSource(VolumeSourceBase):
    def __init__(self, volume_data: Dict[str, Dict[str, Decimal]]):
        super().__init__()
        self._data = volume_data

    @property
    def name(self) -> str:
        return "dummy"

    async def get_all_24h_volumes(
        self, trading_pairs: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Decimal]]:
        if trading_pairs:
            return {k: v for k, v in self._data.items() if k in {tp.upper() for tp in trading_pairs}}
        return dict(self._data)


class VolumeOracleTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        VolumeOracle._shared_instance = None

    def tearDown(self):
        VolumeOracle._shared_instance = None

    def _make_dummy_data(self) -> Dict[str, Dict[str, Decimal]]:
        return {
            "BTC-USDT": _make_volume_dict("dummy", "BTC-USDT", "1000", "50000"),
            "ETH-USDT": _make_volume_dict("dummy", "ETH-USDT", "5000", "3000"),
            "SOL-USDT": _make_volume_dict("dummy", "SOL-USDT", "200", "150"),
        }

    async def test_get_all_volumes_no_filter(self):
        data = self._make_dummy_data()
        oracle = VolumeOracle(source=DummyVolumeSource(data))

        result = await oracle.get_all_24h_volumes()

        self.assertEqual(len(data), len(result))
        self.assertIn("BTC-USDT", result)
        self.assertEqual(Decimal("1000"), result["BTC-USDT"]["base_volume"])
        self.assertEqual(Decimal("50000"), result["BTC-USDT"]["last_price"])

    async def test_get_all_volumes_with_filter(self):
        data = self._make_dummy_data()
        oracle = VolumeOracle(source=DummyVolumeSource(data))

        result = await oracle.get_all_24h_volumes(trading_pairs=["BTC-USDT", "ETH-USDT"])

        self.assertIn("BTC-USDT", result)
        self.assertIn("ETH-USDT", result)
        self.assertNotIn("SOL-USDT", result)

    async def test_get_all_volumes_empty_list_returns_all(self):
        data = self._make_dummy_data()
        oracle = VolumeOracle(source=DummyVolumeSource(data))

        result = await oracle.get_all_24h_volumes(trading_pairs=[])

        self.assertEqual(len(data), len(result))

    async def test_source_property_returns_and_sets_source(self):
        source1 = DummyVolumeSource({})
        source2 = DummyVolumeSource({"X-Y": _make_volume_dict("d", "X-Y", "1", "1")})
        oracle = VolumeOracle(source=source1)

        self.assertIs(source1, oracle.source)
        oracle.source = source2
        self.assertIs(source2, oracle.source)

    def test_default_source_is_binance(self):
        oracle = VolumeOracle()
        self.assertIsInstance(oracle.source, BinanceVolumeSource)

    def test_get_instance_returns_shared_instance(self):
        instance1 = VolumeOracle.get_instance()
        instance2 = VolumeOracle.get_instance()
        self.assertIs(instance1, instance2)

    def test_str_includes_source_name(self):
        oracle = VolumeOracle(source=DummyVolumeSource({}))
        self.assertIn("dummy", str(oracle))

    async def test_source_can_be_replaced(self):
        data_a = {"A-B": _make_volume_dict("dummy", "A-B", "100", "10")}
        data_b = {"C-D": _make_volume_dict("dummy", "C-D", "200", "20")}
        oracle = VolumeOracle(source=DummyVolumeSource(data_a))

        result_a = await oracle.get_all_24h_volumes()
        self.assertIn("A-B", result_a)

        oracle.source = DummyVolumeSource(data_b)
        result_b = await oracle.get_all_24h_volumes()
        self.assertIn("C-D", result_b)
        self.assertNotIn("A-B", result_b)

    async def test_volume_data_fields_present(self):
        data = {
            "BTC-USDT": {
                "exchange": "dummy",
                "symbol": "BTC-USDT",
                "base_volume": Decimal("999"),
                "last_price": Decimal("50000"),
                "quote_volume": Decimal("49950000"),
            }
        }
        oracle = VolumeOracle(source=DummyVolumeSource(data))
        result = await oracle.get_all_24h_volumes()

        entry = result["BTC-USDT"]
        self.assertIn("exchange", entry)
        self.assertIn("symbol", entry)
        self.assertIn("base_volume", entry)
        self.assertIn("last_price", entry)
        self.assertIn("quote_volume", entry)
        self.assertEqual(Decimal("49950000"), entry["quote_volume"])
