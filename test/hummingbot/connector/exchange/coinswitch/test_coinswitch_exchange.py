import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.coinswitch import (
    coinswitch_constants as CONSTANTS,
    coinswitch_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee

_VALID_SECRET = "aa" * 32


def _make_exchange(**kwargs) -> CoinswitchExchange:
    defaults = dict(
        coinswitch_api_key="test_api_key",
        coinswitch_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    defaults.update(kwargs)
    return CoinswitchExchange(**defaults)


class CoinswitchExchangePropertiesTests(IsolatedAsyncioWrapperTestCase):
    """Tests for exchange connector properties and static helpers."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    def test_name(self):
        self.assertEqual("coinswitch", self.exchange.name)

    def test_client_order_id_prefix(self):
        self.assertEqual("x-CS", self.exchange.client_order_id_prefix)

    def test_client_order_id_max_length(self):
        self.assertEqual(CONSTANTS.MAX_ORDER_ID_LEN, self.exchange.client_order_id_max_length)

    def test_supported_order_types_include_limit(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)

    def test_is_cancel_request_synchronous(self):
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)

    def test_trading_rules_request_path(self):
        self.assertEqual(CONSTANTS.TRADE_INFO_PATH_URL, self.exchange.trading_rules_request_path)

    def test_trading_pairs_request_path(self):
        self.assertEqual(CONSTANTS.ACTIVE_COINS_PATH_URL, self.exchange.trading_pairs_request_path)

    def test_check_network_request_path(self):
        self.assertEqual(CONSTANTS.PING_PATH_URL, self.exchange.check_network_request_path)

    def test_coinswitch_order_type_always_limit(self):
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, CoinswitchExchange.coinswitch_order_type(OrderType.LIMIT))
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, CoinswitchExchange.coinswitch_order_type(OrderType.LIMIT_MAKER))

    def test_to_hb_order_type_is_limit(self):
        self.assertEqual(OrderType.LIMIT, CoinswitchExchange.to_hb_order_type("limit"))

    def test_domain_stored(self):
        ex = _make_exchange(domain="com")
        self.assertEqual("com", ex.domain)

    def test_exchange_attribute_stored(self):
        ex = _make_exchange(exchange="coinswitchx")
        self.assertEqual("coinswitchx", ex._exchange)


class CoinswitchExchangeTradingRulesTests(IsolatedAsyncioWrapperTestCase):
    """Tests for _format_trading_rules and _initialize_trading_pair_symbols_from_exchange_info."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    def _trade_info_response(self, **overrides):
        base = {
            "data": {
                "coinswitchx": {
                    "BTC/INR": {
                        "precision": {"base": 6, "quote": 2, "limit": 0},
                        "quote": {"min": "150", "max": "2500000"},
                    },
                    "ETH/INR": {
                        "precision": {"base": 4, "quote": 2, "limit": 0},
                        "quote": {"min": "100", "max": "1000000"},
                    },
                }
            }
        }
        base.update(overrides)
        return base

    async def test_format_trading_rules_returns_correct_count(self):
        rules = await self.exchange._format_trading_rules(self._trade_info_response())
        self.assertEqual(2, len(rules))

    async def test_format_trading_rules_btc_inr_step_size(self):
        rules = await self.exchange._format_trading_rules(self._trade_info_response())
        btc = next(r for r in rules if r.trading_pair == "BTC-INR")
        self.assertEqual(Decimal("1E-6"), btc.min_order_size)
        self.assertEqual(Decimal("1E-6"), btc.min_base_amount_increment)

    async def test_format_trading_rules_btc_inr_tick_size(self):
        rules = await self.exchange._format_trading_rules(self._trade_info_response())
        btc = next(r for r in rules if r.trading_pair == "BTC-INR")
        self.assertEqual(Decimal("0.01"), btc.min_price_increment)

    async def test_format_trading_rules_btc_inr_min_notional(self):
        rules = await self.exchange._format_trading_rules(self._trade_info_response())
        btc = next(r for r in rules if r.trading_pair == "BTC-INR")
        self.assertEqual(Decimal("150"), btc.min_notional_size)

    async def test_format_trading_rules_skips_symbol_without_separator(self):
        data = {
            "data": {
                "coinswitchx": {
                    "BTCINR": {
                        "precision": {"base": 6, "quote": 2},
                        "quote": {"min": "10"},
                    }
                }
            }
        }
        rules = await self.exchange._format_trading_rules(data)
        self.assertEqual(0, len(rules))

    async def test_format_trading_rules_empty_exchange_data(self):
        rules = await self.exchange._format_trading_rules({"data": {}})
        self.assertEqual(0, len(rules))

    def test_initialize_trading_pair_symbols_from_ticker_data(self):
        ticker_response = {
            "data": {
                "BTC/INR": {"lastPrice": "5000000"},
                "ETH/INR": {"lastPrice": "200000"},
                "USDT-INR": {"lastPrice": "85"},    # hyphen-separated variant
            }
        }
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(ticker_response)
        self.assertTrue(self.exchange.trading_pair_symbol_map_ready())
        # Access via _set_trading_pair_symbol_map stored value through the public async method
        loop = asyncio.get_event_loop()
        symbol_map = loop.run_until_complete(self.exchange.trading_pair_symbol_map())
        self.assertIn("BTC/INR", symbol_map)
        self.assertEqual("BTC-INR", symbol_map["BTC/INR"])
        self.assertIn("ETH/INR", symbol_map)
        self.assertIn("USDT-INR", symbol_map)

    def test_initialize_trading_pair_symbols_skips_no_separator(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info({"data": {"BTCINR": {}}})
        self.assertFalse(self.exchange.trading_pair_symbol_map_ready())


class CoinswitchExchangeAPITests(IsolatedAsyncioWrapperTestCase):
    """Tests for HTTP-backed exchange methods using aioresponses."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "INR"
        cls.trading_pair = "BTC-INR"
        cls.exchange_symbol = "BTC/INR"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.exchange = _make_exchange(trading_pairs=[self.trading_pair])
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0.0)
        self.exchange._set_trading_pair_symbol_map(bidict({self.exchange_symbol: self.trading_pair}))

    def _url(self, path):
        return web_utils.public_rest_url(path)

    def _regex(self, path):
        return re.compile(rf"^{re.escape(self._url(path))}")

    @aioresponses()
    async def test_make_trading_pairs_request(self, mock_api):
        resp = {"data": {self.exchange_symbol: {"lastPrice": "5000000"}}}
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps(resp))

        result = await self.exchange._make_trading_pairs_request()

        self.assertIn("data", result)
        self.assertIn(self.exchange_symbol, result["data"])

    @aioresponses()
    async def test_make_trading_rules_request(self, mock_api):
        resp = {
            "data": {
                "coinswitchx": {
                    self.exchange_symbol: {
                        "precision": {"base": 6, "quote": 2},
                        "quote": {"min": "150"},
                    }
                }
            }
        }
        mock_api.get(self._regex(CONSTANTS.TRADE_INFO_PATH_URL), body=json.dumps(resp))

        result = await self.exchange._make_trading_rules_request()

        self.assertIn("data", result)

    @aioresponses()
    async def test_get_all_pairs_prices_returns_data(self, mock_api):
        resp = {"data": {self.exchange_symbol: {"lastPrice": "5000000"}}}
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps(resp))

        result = await self.exchange.get_all_pairs_prices()

        self.assertIn("data", result)

    @aioresponses()
    async def test_get_all_24h_volume_tickers_no_filter_returns_all(self, mock_api):
        resp = {
            "data": {
                "BTC/INR": {"lastPrice": "5000000", "volume": "10"},
                "ETH/INR": {"lastPrice": "200000", "volume": "50"},
            }
        }
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps(resp))

        result = await self.exchange.get_all_24h_volume_tickers()

        self.assertEqual(2, len(result))

    @aioresponses()
    async def test_get_all_24h_volume_tickers_with_filter(self, mock_api):
        resp = {
            "data": {
                "BTC/INR": {"lastPrice": "5000000", "volume": "10"},
                "ETH/INR": {"lastPrice": "200000", "volume": "50"},
            }
        }
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps(resp))

        result = await self.exchange.get_all_24h_volume_tickers(trading_pairs=["BTC-INR"])

        self.assertEqual(1, len(result))
        self.assertEqual("BTC/INR", result[0]["symbol"])

    @aioresponses()
    async def test_get_all_24h_volume_tickers_filter_case_insensitive(self, mock_api):
        resp = {"data": {"BTC/INR": {"lastPrice": "5000000"}}}
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps(resp))

        result = await self.exchange.get_all_24h_volume_tickers(trading_pairs=["btc-inr"])

        self.assertEqual(1, len(result))

    @aioresponses()
    async def test_get_all_24h_volume_tickers_empty_response(self, mock_api):
        mock_api.get(self._regex(CONSTANTS.TICKER_ALL_PATH_URL), body=json.dumps({}))

        result = await self.exchange.get_all_24h_volume_tickers()

        self.assertEqual([], result)

    @aioresponses()
    async def test_update_balances_list_format(self, mock_api):
        resp = {
            "data": [
                {"currency": "BTC", "main_balance": "1.5", "blocked_balance_order": "0.5"},
                {"currency": "INR", "main_balance": "100000", "blocked_balance_order": "0"},
            ]
        }
        mock_api.get(self._regex(CONSTANTS.GET_PORTFOLIO_PATH_URL), body=json.dumps(resp))

        await self.exchange._update_balances()

        self.assertEqual(Decimal("2.0"), self.exchange._account_balances["BTC"])
        self.assertEqual(Decimal("1.5"), self.exchange._account_available_balances["BTC"])
        self.assertEqual(Decimal("100000"), self.exchange._account_balances["INR"])

    @aioresponses()
    async def test_update_balances_dict_format(self, mock_api):
        """Portfolio endpoint may return a dict instead of a list."""
        resp = {
            "data": {
                "BTC": {"main_balance": "2.0", "blocked_balance_order": "0.5"},
            }
        }
        mock_api.get(self._regex(CONSTANTS.GET_PORTFOLIO_PATH_URL), body=json.dumps(resp))

        await self.exchange._update_balances()

        self.assertIn("BTC", self.exchange._account_balances)

    @aioresponses()
    async def test_update_balances_removes_stale_assets(self, mock_api):
        self.exchange._account_balances["XRP"] = Decimal("100")
        self.exchange._account_available_balances["XRP"] = Decimal("100")

        resp = {"data": [{"currency": "BTC", "main_balance": "1.0", "blocked_balance_order": "0"}]}
        mock_api.get(self._regex(CONSTANTS.GET_PORTFOLIO_PATH_URL), body=json.dumps(resp))

        await self.exchange._update_balances()

        self.assertNotIn("XRP", self.exchange._account_balances)

    @aioresponses()
    async def test_place_order_buy(self, mock_api):
        resp = {"data": {"order_id": "ex_order_456", "created_time": 1640000000000}}
        mock_api.post(self._regex(CONSTANTS.CREATE_ORDER_PATH_URL), body=json.dumps(resp))

        exchange_id, timestamp = await self.exchange._place_order(
            order_id="x-CS-test001",
            trading_pair=self.trading_pair,
            amount=Decimal("0.001"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("5000000"),
        )

        self.assertEqual("ex_order_456", exchange_id)
        self.assertEqual(1640000000.0, timestamp)

    @aioresponses()
    async def test_place_order_sell(self, mock_api):
        resp = {"data": {"order_id": "ex_order_789", "created_time": 1640000001000}}
        mock_api.post(self._regex(CONSTANTS.CREATE_ORDER_PATH_URL), body=json.dumps(resp))

        exchange_id, timestamp = await self.exchange._place_order(
            order_id="x-CS-test002",
            trading_pair=self.trading_pair,
            amount=Decimal("0.001"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("5000000"),
        )

        self.assertEqual("ex_order_789", exchange_id)

    @aioresponses()
    async def test_place_order_zero_created_time_uses_sync_time(self, mock_api):
        resp = {"data": {"order_id": "ex_order_000", "created_time": 0}}
        mock_api.post(self._regex(CONSTANTS.CREATE_ORDER_PATH_URL), body=json.dumps(resp))

        _, timestamp = await self.exchange._place_order(
            order_id="x-CS-test003",
            trading_pair=self.trading_pair,
            amount=Decimal("0.001"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("5000000"),
        )

        self.assertGreater(timestamp, 0)

    @aioresponses()
    async def test_place_cancel_success_flag(self, mock_api):
        mock_api.delete(self._regex(CONSTANTS.CANCEL_ORDER_PATH_URL), body=json.dumps({"success": True}))
        order = self._make_in_flight_order("x-CS-c001", "ex_c001")

        result = await self.exchange._place_cancel("x-CS-c001", order)

        self.assertTrue(result)

    @aioresponses()
    async def test_place_cancel_data_status_cancelled(self, mock_api):
        mock_api.delete(
            self._regex(CONSTANTS.CANCEL_ORDER_PATH_URL),
            body=json.dumps({"data": {"status": "CANCELLED"}}),
        )
        order = self._make_in_flight_order("x-CS-c002", "ex_c002")

        result = await self.exchange._place_cancel("x-CS-c002", order)

        self.assertTrue(result)

    @aioresponses()
    async def test_place_cancel_returns_false_on_failure(self, mock_api):
        mock_api.delete(
            self._regex(CONSTANTS.CANCEL_ORDER_PATH_URL),
            body=json.dumps({"error": "Order not found"}),
        )
        order = self._make_in_flight_order("x-CS-c003", "ex_c003")

        result = await self.exchange._place_cancel("x-CS-c003", order)

        self.assertFalse(result)

    @aioresponses()
    async def test_request_order_status_open(self, mock_api):
        resp = {"data": {"order_id": "ex_001", "status": "OPEN", "updated_time": 1640000000000}}
        mock_api.get(self._regex(CONSTANTS.GET_ORDER_PATH_URL), body=json.dumps(resp))
        order = self._make_in_flight_order("x-CS-s001", "ex_001")

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual("ex_001", update.exchange_order_id)

    @aioresponses()
    async def test_request_order_status_filled(self, mock_api):
        resp = {"data": {"order_id": "ex_002", "status": "EXECUTED", "updated_time": 1640000001000}}
        mock_api.get(self._regex(CONSTANTS.GET_ORDER_PATH_URL), body=json.dumps(resp))
        order = self._make_in_flight_order("x-CS-s002", "ex_002")

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.FILLED, update.new_state)

    @aioresponses()
    async def test_request_order_status_cancelled(self, mock_api):
        resp = {"data": {"order_id": "ex_003", "status": "CANCELLED", "updated_time": 1640000002000}}
        mock_api.get(self._regex(CONSTANTS.GET_ORDER_PATH_URL), body=json.dumps(resp))
        order = self._make_in_flight_order("x-CS-s003", "ex_003")

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.CANCELED, update.new_state)

    @aioresponses()
    async def test_request_order_status_partially_filled(self, mock_api):
        resp = {
            "data": {
                "order_id": "ex_004",
                "status": "PARTIALLY_EXECUTED",
                "updated_time": 1640000003000,
            }
        }
        mock_api.get(self._regex(CONSTANTS.GET_ORDER_PATH_URL), body=json.dumps(resp))
        order = self._make_in_flight_order("x-CS-s004", "ex_004")

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)

    @aioresponses()
    async def test_request_order_status_raises_on_missing_data(self, mock_api):
        mock_api.get(self._regex(CONSTANTS.GET_ORDER_PATH_URL), body=json.dumps({}))
        order = self._make_in_flight_order("x-CS-s005", "ex_005")

        with self.assertRaises(Exception):
            await self.exchange._request_order_status(order)

    @aioresponses()
    async def test_get_trading_fees_parses_taker_fee(self, mock_api):
        resp = {
            "data": {
                "coinswitchx": {
                    "BTC": {
                        "maker_fee": 0.0009,
                        "taker_fee": 0.0009,
                        "maker_fee_after_discount": 0.0,
                        "taker_fee_after_discount": 0.0009,
                    }
                }
            }
        }
        mock_api.get(self._regex(CONSTANTS.TRADING_FEE_PATH_URL), body=json.dumps(resp))

        fees = await self.exchange._get_trading_fees()

        self.assertIn("BTC", fees)
        self.assertIsInstance(fees["BTC"], DeductedFromReturnsTradeFee)
        self.assertEqual(Decimal("0.0009"), fees["BTC"].percent)

    @aioresponses()
    async def test_update_trading_fees_populates_dict(self, mock_api):
        resp = {
            "data": {
                "coinswitchx": {
                    "BTC": {"taker_fee_after_discount": 0.0005},
                    "ETH": {"taker_fee_after_discount": 0.0005},
                }
            }
        }
        mock_api.get(self._regex(CONSTANTS.TRADING_FEE_PATH_URL), body=json.dumps(resp))

        await self.exchange._update_trading_fees()

        self.assertIn("BTC", self.exchange._trading_fees)
        self.assertIn("ETH", self.exchange._trading_fees)

    @aioresponses()
    async def test_get_trading_fees_empty_response_returns_empty_dict(self, mock_api):
        mock_api.get(self._regex(CONSTANTS.TRADING_FEE_PATH_URL), body=json.dumps({}))

        fees = await self.exchange._get_trading_fees()

        self.assertEqual({}, fees)

    def test_get_fee_returns_deducted_from_returns(self):
        fee = self.exchange._get_fee(
            base_currency="BTC",
            quote_currency="INR",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("5000000"),
        )
        self.assertIsInstance(fee, DeductedFromReturnsTradeFee)

    def test_get_fee_limit_maker_uses_maker_rate(self):
        fee_maker = self.exchange._get_fee(
            base_currency="BTC",
            quote_currency="INR",
            order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("5000000"),
            is_maker=True,
        )
        fee_taker = self.exchange._get_fee(
            base_currency="BTC",
            quote_currency="INR",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("5000000"),
            is_maker=False,
        )
        self.assertLessEqual(fee_maker.percent, fee_taker.percent)

    def test_is_request_exception_related_to_time_synchronizer(self):
        ex = Exception("timestamp mismatch")
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(ex))

    def test_is_request_exception_unrelated_to_time_synchronizer(self):
        ex = Exception("order not found")
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(ex))

    def test_is_order_not_found_during_status_update(self):
        ex = Exception("order not found")
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(ex))

    def test_is_order_not_found_during_cancelation(self):
        ex = Exception("does not exist")
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(ex))

    async def test_user_stream_listener_processes_balance_update(self):
        event = {
            "event": CONSTANTS.BALANCE_UPDATE_EVENT_TYPE,
            "data": [
                {"currency": "BTC", "main_balance": "3.0", "blocked_balance_order": "0.5"}
            ],
        }
        with patch.object(
            self.exchange,
            "_iter_user_event_queue",
            side_effect=lambda: self._async_gen([event]),
        ):
            task = asyncio.create_task(self.exchange._user_stream_event_listener())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(Decimal("3.5"), self.exchange._account_balances.get("BTC", Decimal("0")))
        self.assertEqual(Decimal("3.0"), self.exchange._account_available_balances.get("BTC", Decimal("0")))

    async def test_user_stream_listener_processes_order_update(self):
        order = self._make_in_flight_order("x-CS-ws001", "ex_ws001")
        self.exchange._order_tracker._in_flight_orders["x-CS-ws001"] = order

        event = {
            "event": CONSTANTS.ORDER_UPDATE_EVENT_TYPE,
            "data": [
                {
                    "client_order_id": "x-CS-ws001",
                    "order_id": "ex_ws001",
                    "status": "EXECUTED",
                    "updated_time": 1640000001000,
                }
            ],
        }
        with patch.object(
            self.exchange,
            "_iter_user_event_queue",
            side_effect=lambda: self._async_gen([event]),
        ):
            task = asyncio.create_task(self.exchange._user_stream_event_listener())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _make_in_flight_order(self, client_id: str, exchange_id: str) -> InFlightOrder:
        return InFlightOrder(
            client_order_id=client_id,
            exchange_order_id=exchange_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("5000000"),
            amount=Decimal("0.001"),
            creation_timestamp=1640000000.0,
        )

    @staticmethod
    async def _async_gen(items):
        for item in items:
            yield item


if __name__ == "__main__":
    import unittest
    unittest.main()
