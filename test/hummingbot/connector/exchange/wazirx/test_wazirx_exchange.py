
import asyncio
import json
import re
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses
from aioresponses.core import RequestCall
from yarl import URL

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxAPIError, WazirxExchange
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import (
    AddedToCostTradeFee,
    DeductedFromReturnsTradeFee,
    TokenAmount,
    TradeFeeBase,
)


class DummyRA:
    def __init__(self, response=None, resp=None, exc=None, raise_exc=False):
        self._response = response if response is not None else resp
        self._exc = exc
        self._raise = raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute_request(self, *args, **kwargs):
        if self._exc is not None:
            raise self._exc
        if self._raise:
            raise Exception("dummy error")
        return self._response


class DummyFactory:
    def __init__(self, response=None, resp=None, exc=None, raise_exc=False):
        self._response = response if response is not None else resp
        self._exc = exc
        self._raise = raise_exc

    def build_rest_assistant(self):
        return DummyRA(response=self._response, resp=self._response, exc=self._exc, raise_exc=self._raise)

    async def get_rest_assistant(self):
        """Async version for compatibility with wazirx_exchange."""
        return DummyRA(response=self._response, resp=self._response, exc=self._exc, raise_exc=self._raise)


class WazirxExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.TICKERS_PATH_URL, domain=self.exchange._domain)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKERS_PATH_URL, domain=self.exchange._domain)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.private_rest_url(CONSTANTS.USER_BALANCES_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.private_rest_url(CONSTANTS.TICKERS_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.CREATE_ORDER_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.USER_BALANCES_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseAsset": self.base_asset,
                "quoteAsset": self.quote_asset,
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [],
            }
        ]

    @property
    def latest_prices_request_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "lastPrice": "0.00000500",
                "bidPrice": "0.00000500",
                "askPrice": "0.00000500",
                "volume": "1000.00000000",
                "highPrice": "0.00000500",
                "lowPrice": "0.00000500",
                "at": 1639598493658
            }
        ]

    @property
    def network_status_request_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def order_creation_request_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "orderId": self.expected_exchange_order_id,
            "orderListId": -1,
            "clientOrderId": "hbot-123456789",
            "transactTime": 1639598493658,
            "price": "0.00000500",
            "origQty": "100.00000000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "fills": []
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "makerCommission": 10,
            "takerCommission": 10,
            "buyerCommission": 0,
            "sellerCommission": 0,
            "canTrade": True,
            "canWithdraw": True,
            "canDeposit": True,
            "updateTime": 1639598493658,
            "accountType": "SPOT",
            "balances": [
                {
                    "asset": self.base_asset,
                    "free": "100.00000000",
                    "locked": "0.00000000"
                },
                {
                    "asset": self.quote_asset,
                    "free": "1000.00000000",
                    "locked": "0.00000000"
                }
            ],
            "permissions": [
                "SPOT"
            ]
        }

    @property
    def balance_request_mock_response_only_base(self):
        return self.balance_request_mock_response_for_base_and_quote

    @property
    def balance_event_websocket_update(self):
        return {}

    @property
    def expected_latest_price(self):
        return 5e-06

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.000001"),
            max_order_size=Decimal("100000.0"),
            min_price_increment=Decimal("0.00000001"),
            min_base_amount_increment=Decimal("0.000001"),
            min_quote_amount_increment=Decimal("0.000001"),
            min_notional_size=Decimal("0.0001"),
            max_notional_size=Decimal("100000.0"),
        )

    @property
    def expected_logged_error_for_rejected_order(self):
        return "Order rejected"

    @property
    def expected_exchange_order_id(self):
        return "123456789"

    @property
    def is_order_fill_http_update_included_in_status_update(self):
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self):
        return False

    @property
    def expected_partial_fill_price(self):
        return Decimal("0.000005")

    @property
    def expected_partial_fill_amount(self):
        return Decimal("50")

    @property
    def expected_fill_fee(self):
        return TradeFeeBase.new_spot_fee(
            fee_schema=DeductedFromReturnsTradeFee,
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(amount=Decimal("0.00005"), token=self.quote_asset)]
        )

    @property
    def expected_fill_trade_id(self):
        return "987654321"

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        return WazirxExchange(
            wazirx_api_key="test_api_key",
            wazirx_api_secret="test_api_secret",
            trading_pairs=[self.trading_pair],
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        self.assertIn("X-API-KEY", request_call.kwargs["headers"])

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual(order.trade_type.name.upper(), request_data["side"])
        self.assertEqual(order.order_type.name.upper(), request_data["type"])
        self.assertEqual(Decimal("100"), Decimal(request_data["quantity"]))
        self.assertEqual(Decimal("0.000005"), Decimal(request_data["price"]))

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
        self.assertEqual(order.exchange_order_id, request_params["orderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
        self.assertEqual(order.exchange_order_id, request_params["orderId"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        super().test_update_trading_rules(mock_api)

    @aioresponses()
    def test_update_trading_rules_ignores_unsupported_pairs(self, mock_api):
        super().test_update_trading_rules_ignores_unsupported_pairs(mock_api)

    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        super().test_update_trading_rules_ignores_rule_with_error(mock_api)

    @aioresponses()
    def test_create_order(self, mock_api):
        super().test_create_order(mock_api)

    @aioresponses()
    def test_create_order_fails_when_trading_rule_error_happens(self, mock_api):
        super().test_create_order_fails_when_trading_rule_error_happens(mock_api)

    @aioresponses()
    def test_create_order_fails_with_insufficient_balance(self, mock_api):
        super().test_create_order_fails_with_insufficient_balance(mock_api)

    @aioresponses()
    def test_execute_buy_order_with_filled_event(self, mock_api):
        super().test_execute_buy_order_with_filled_event(mock_api)

    @aioresponses()
    def test_execute_sell_order_with_filled_event(self, mock_api):
        super().test_execute_sell_order_with_filled_event(mock_api)

    @aioresponses()
    def test_execute_buy_order_with_partial_fill(self, mock_api):
        super().test_execute_buy_order_with_partial_fill(mock_api)

    @aioresponses()
    def test_execute_sell_order_with_partial_fill(self, mock_api):
        super().test_execute_sell_order_with_partial_fill(mock_api)

    @aioresponses()
    def test_cancel_order(self, mock_api):
        super().test_cancel_order(mock_api)

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        super().test_cancel_order_not_found_in_the_exchange(mock_api)

    @aioresponses()
    def test_get_last_traded_price(self, mock_api):
        super().test_get_last_traded_price(mock_api)

    @aioresponses()
    def test_get_last_traded_price_returns_none_when_no_data(self, mock_api):
        super().test_get_last_traded_price_returns_none_when_no_data(mock_api)

    @aioresponses()
    def test_get_last_traded_price_returns_none_when_exchange_returns_empty_data(self, mock_api):
        super().test_get_last_traded_price_returns_none_when_exchange_returns_empty_data(mock_api)

    @aioresponses()
    def test_update_balances(self, mock_api):
        super().test_update_balances(mock_api)

    @aioresponses()
    def test_update_order_status(self, mock_api):
        super().test_update_order_status(mock_api)

    @aioresponses()
    def test_update_order_status_marks_order_as_failure_when_request_fails(self, mock_api):
        super().test_update_order_status_marks_order_as_failure_when_request_fails(mock_api)

    @aioresponses()
    def test_update_order_status_marks_order_as_failure_when_exchange_returns_invalid_data(self, mock_api):
        super().test_update_order_status_marks_order_as_failure_when_exchange_returns_invalid_data(mock_api)

    @aioresponses()
    def test_update_order_status_marks_order_as_failure_when_exchange_returns_error_status(self, mock_api):
        super().test_update_order_status_marks_order_as_failure_when_exchange_returns_error_status(mock_api)

    @aioresponses()
    def test_user_stream_update_for_order_failure(self, mock_api):
        super().test_user_stream_update_for_order_failure(mock_api)

    @aioresponses()
    def test_user_stream_update_for_order_full_fill(self, mock_api):
        super().test_user_stream_update_for_order_full_fill(mock_api)

    @aioresponses()
    def test_user_stream_update_for_order_partial_fill(self, mock_api):
        super().test_user_stream_update_for_order_partial_fill(mock_api)

    @aioresponses()
    def test_user_stream_update_for_order_cancel(self, mock_api):
        super().test_user_stream_update_for_order_cancel(mock_api)

    @aioresponses()
    def test_user_stream_balance_update(self, mock_api):
        super().test_user_stream_balance_update(mock_api)

    @aioresponses()
    def test_user_stream_update_for_new_order(self, mock_api):
        super().test_user_stream_update_for_new_order(mock_api)

    @aioresponses()
    def test_user_stream_update_for_unknown_order(self, mock_api):
        super().test_user_stream_update_for_unknown_order(mock_api)

    def test_format_trading_rules(self):
        raw_trading_rules = [
            {
                "symbol": "btcusdt",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.000001",
                        "maxQty": "100000.0",
                        "stepSize": "0.000001"
                    },
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.00000001",
                        "maxPrice": "100000.0",
                        "tickSize": "0.00000001"
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": "0.0001"
                    }
                ]
            }
        ]
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(1, len(trading_rules))
        rule = trading_rules[0]
        self.assertEqual("BTC-USDT", rule.trading_pair)
        self.assertEqual(Decimal("0.000001"), rule.min_order_size)
        self.assertEqual(Decimal("100000.0"), rule.max_order_size)
        self.assertEqual(Decimal("0.00000001"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.0001"), rule.min_notional_size)

    def test_format_trading_rules_with_missing_filters(self):
        raw_trading_rules = [
            {
                "symbol": "btcusdt",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": []
            }
        ]
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(1, len(trading_rules))
        rule = trading_rules[0]
        self.assertEqual("BTC-USDT", rule.trading_pair)
        self.assertEqual(Decimal("1e-8"), rule.min_order_size)
        self.assertEqual(Decimal("1e8"), rule.max_order_size)

    def test_format_trading_rules_with_invalid_data(self):
        raw_trading_rules = [
            {
                "symbol": "btcusdt",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "invalid",
                        "maxQty": "100000.0",
                        "stepSize": "0.000001"
                    }
                ]
            }
        ]
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(1, len(trading_rules))
        rule = trading_rules[0]
        self.assertEqual("BTC-USDT", rule.trading_pair)
        self.assertEqual(Decimal("1e-8"), rule.min_order_size)

    @aioresponses()
    def test_create_order_buy_limit(self, mock_api):
        mock_api.post(
            self.order_creation_url,
            body=json.dumps(self.order_creation_request_mock_response)
        )

        order_id = self.exchange.create_order(
            trade_type=self.buy_trade_type,
            order_id="test_order",
            trading_pair=self.trading_pair,
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            price=Decimal("0.000005")
        )

        self.assertEqual("test_order", order_id)

    @aioresponses()
    def test_create_order_sell_market(self, mock_api):
        mock_response = self.order_creation_request_mock_response.copy()
        mock_response["type"] = "MARKET"
        mock_response["side"] = "SELL"

        mock_api.post(
            self.order_creation_url,
            body=json.dumps(mock_response)
        )

        order_id = self.exchange.create_order(
            trade_type=self.sell_trade_type,
            order_id="test_order_sell",
            trading_pair=self.trading_pair,
            amount=Decimal("100"),
            order_type=OrderType.MARKET,
            price=None
        )

        self.assertEqual("test_order_sell", order_id)

    @aioresponses()
    def test_cancel_order_by_id(self, mock_api):
        cancel_url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL, domain=self.exchange._domain)
        mock_api.delete(
            cancel_url,
            body=json.dumps({"symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)})
        )

        result = self.exchange.cancel_order("test_client_order_id", "BTC-USDT", "12345")
        self.assertTrue(result)

    @aioresponses()
    def test_get_order_status(self, mock_api):
        status_url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL, domain=self.exchange._domain)
        mock_api.get(
            status_url,
            payload={
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "orderId": "12345",
                "status": "FILLED",
                "executedQty": "100.00000000",
                "cummulativeQuoteQty": "0.00050000"
            }
        )

        status = self.exchange.get_order_status("BTC-USDT", "12345")
        self.assertIsNotNone(status)

    @aioresponses()
    def test_update_balances_direct(self, mock_api):
        mock_api.get(
            self.balance_url,
            body=json.dumps(self.balance_request_mock_response_for_base_and_quote)
        )

        self.exchange._update_balances()
        self.assertIn(self.base_asset, self.exchange._account_balances)
        self.assertIn(self.quote_asset, self.exchange._account_balances)

    def test_process_user_stream_event_order_update(self):
        event_data = {
            "eventType": "executionReport",
            "symbol": "btcusdt",
            "orderId": 12345,
            "clientOrderId": "test_client_order",
            "side": "BUY",
            "orderType": "LIMIT",
            "orderStatus": "FILLED",
            "quantity": "100.00000000",
            "price": "0.00000500",
            "executedQty": "100.00000000",
            "cummulativeQuoteQty": "0.00050000"
        }

        self.exchange._process_user_stream_event(event_data)

    def test_process_user_stream_event_balance_update(self):
        event_data = {
            "eventType": "balanceUpdate",
            "asset": "BTC",
            "free": "1.00000000",
            "locked": "0.50000000"
        }

        self.exchange._process_user_stream_event(event_data)

    def test_process_user_stream_event_invalid(self):
        event_data = {
            "eventType": "invalid",
            "data": "test"
        }

        self.exchange._process_user_stream_event(event_data)

    def test_get_fee_returns_zero_fee(self):
        fee = self.exchange.get_fee("BTC", "USDT", OrderType.LIMIT, self.buy_trade_type, Decimal("100"), Decimal("0.000005"))
        self.assertIsNotNone(fee)
        self.assertEqual(Decimal("0"), fee.percent)

    def test_get_order_price_quantum(self):
        quantum = self.exchange.get_order_price_quantum("BTC-USDT", Decimal("0.000005"))
        self.assertIsNotNone(quantum)

    def test_get_order_size_quantum(self):
        quantum = self.exchange.get_order_size_quantum("BTC-USDT", Decimal("100"))
        self.assertIsNotNone(quantum)

    def test_quantize_order_amount(self):
        quantized = self.exchange.quantize_order_amount("BTC-USDT", Decimal("100.12345678"))
        self.assertIsNotNone(quantized)

    def test_quantize_order_price(self):
        quantized = self.exchange.quantize_order_price("BTC-USDT", Decimal("0.000005123456"))
        self.assertIsNotNone(quantized)

    def test_properties_and_helpers(self):
        self.assertEqual(self.exchange.name, "wazirx")
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)
        self.assertEqual(self.exchange.supported_order_types(), [OrderType.LIMIT, OrderType.MARKET])

        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(Exception()))
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(Exception("Order does not exist")))
        self.assertFalse(self.exchange._is_order_not_found_during_status_update_error(Exception("Other error")))

        # Additional properties to increase coverage
        self.assertEqual(self.exchange.domain, CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual(self.exchange.client_order_id_max_length, CONSTANTS.MAX_ORDER_ID_LEN)
        self.assertEqual(self.exchange.client_order_id_prefix, CONSTANTS.HBOT_ORDER_ID_PREFIX)
        self.assertEqual(self.exchange.trading_rules_request_path, CONSTANTS.TICKERS_PATH_URL)
        self.assertEqual(self.exchange.trading_pairs_request_path, CONSTANTS.TICKERS_PATH_URL)
        self.assertEqual(self.exchange.check_network_request_path, CONSTANTS.TICKERS_PATH_URL)
        self.assertEqual(self.exchange.trading_pairs, [self.trading_pair])
        self.assertTrue(self.exchange.is_trading_required)
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(Exception("Order does not exist")))
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(Exception("Other error")))

    @pytest.mark.asyncio
    async def test_get_last_traded_prices_list_and_dict_and_exception(self):
        class DummyRA:
            def __init__(self, response=None, exc=None):
                self._response = response
                self._exc = exc

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def execute_request(self, *args, **kwargs):
                if self._exc:
                    raise self._exc
                return self._response

        list_resp = [{"symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), "lastPrice": "0.00000500"}]
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=list_resp)
        prices = await self.exchange.get_last_traded_prices([self.trading_pair])
        assert prices[self.trading_pair] == 5e-06

        dict_resp = {"symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), "lastPrice": "0.00001000"}
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=dict_resp)
        prices = await self.exchange.get_last_traded_prices([self.trading_pair])
        assert prices[self.trading_pair] == 1e-05

        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(exc=Exception("boom"))
        prices = await self.exchange.get_last_traded_prices([self.trading_pair])
        assert prices[self.trading_pair] == 0.0

    @pytest.mark.asyncio
    async def test_user_stream_event_listener_processes_order_and_balance_and_unknown(self):
        from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

        order = InFlightOrder("test_client_order", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"))
        self.exchange._order_tracker.all_updatable_orders["test_client_order"] = order

        async def mock_iter_queue():
            yield {"event": "orderUpdate", "order": {"clientOrderId": "test_client_order", "status": "FILLED", "orderId": 123}, "timestamp": 1000}
            yield {"event": "balanceUpdate", "balance": {"asset": self.base_asset, "free": "1.00000000", "locked": "0.00000000"}}
            yield {"event": "unknownEvent", "data": "x"}
            raise asyncio.CancelledError

        from hummingbot.connector.exchange_py_base import ExchangePyBase

        original_iter = ExchangePyBase._iter_user_event_queue
        try:
            ExchangePyBase._iter_user_event_queue = mock_iter_queue
            with pytest.raises(asyncio.CancelledError):
                await self.exchange._user_stream_event_listener()

            assert self.exchange._account_available_balances.get(self.base_asset) == Decimal("1.00000000")
            assert self.exchange._account_balances.get(self.base_asset) == Decimal("1.00000000")
            assert order.current_state == OrderState.FILLED
        finally:
            ExchangePyBase._iter_user_event_queue = original_iter

    @pytest.mark.asyncio
    async def test_all_trade_updates_and_request_order_status_and_place_and_cancel(self):

        class DummyRA:
            def __init__(self, response=None):
                self._response = response

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def execute_request(self, *args, **kwargs):
                return self._response

        # Test _all_trade_updates_for_order
        order = InFlightOrder("c1", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"), exchange_order_id="123")
        trade_resp = [{"id": "987654321", "commission": "0.00005", "commissionAsset": self.quote_asset, "qty": "50", "quoteQty": "0.00025", "price": "0.000005", "orderId": "123", "time": 1639598493658}]
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=trade_resp)
        updates = await self.exchange._all_trade_updates_for_order(order)
        assert len(updates) == 1
        assert updates[0].trade_id == "987654321"

        # Test _request_order_status
        status_resp = {"symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), "orderId": "123", "status": "FILLED", "updateTime": 1639598493658}
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=status_resp)
        ou = await self.exchange._request_order_status(order)
        assert ou.new_state.name == "FILLED"

        # Test _place_order
        place_resp = {"orderId": "4444", "executedQty": "10"}
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=place_resp)
        order_id, executed_amount = await self.exchange._place_order("c2", self.trading_pair, Decimal("100"), self.buy_trade_type, OrderType.LIMIT, Decimal("0.000005"))
        assert order_id == "4444"
        assert executed_amount == 10.0

        # Test _place_cancel
        cancel_resp = {"status": "CANCELED"}
        tracked_order = InFlightOrder("c3", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"), exchange_order_id="555")
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response=cancel_resp)
        cancelled = await self.exchange._place_cancel("555", tracked_order)
        assert cancelled is True

    def test_format_trading_rules_with_dict_input(self):
        raw_trading_rules = {
            "symbols": [
                {
                    "symbol": "btcusdt",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "baseAssetPrecision": 8,
                    "quoteAssetPrecision": 8,
                    "filters": [
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.000001",
                            "maxQty": "100000.0",
                            "stepSize": "0.000001"
                        },
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.00000001",
                            "maxPrice": "100000.0",
                            "tickSize": "0.00000001"
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "0.0001"
                        }
                    ]
                }
            ]
        }
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(1, len(trading_rules))
        rule = trading_rules[0]
        self.assertEqual("BTC-USDT", rule.trading_pair)
        self.assertEqual(Decimal("0.000001"), rule.min_order_size)
        self.assertEqual(Decimal("100000.0"), rule.max_order_size)
        self.assertEqual(Decimal("0.00000001"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.0001"), rule.min_notional_size)

    def test_format_trading_rules_with_missing_symbol(self):
        raw_trading_rules = [
            {
                "symbol": "",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": []
            }
        ]
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(0, len(trading_rules))

    def test_format_trading_rules_with_missing_base_asset(self):
        raw_trading_rules = [
            {
                "symbol": "btcusdt",
                "baseAsset": "",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": []
            }
        ]
        trading_rules = self.exchange._format_trading_rules(raw_trading_rules)
        self.assertEqual(0, len(trading_rules))

    @pytest.mark.asyncio
    async def test_all_trade_updates_for_order_no_exchange_id(self):
        order = InFlightOrder("c1", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"), exchange_order_id=None)
        updates = await self.exchange._all_trade_updates_for_order(order)
        assert updates == []

    @pytest.mark.asyncio
    async def test_all_trade_updates_for_order_exception(self):
        order = InFlightOrder("c1", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"), exchange_order_id="123")
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(exc=Exception("boom"))
        updates = await self.exchange._all_trade_updates_for_order(order)
        assert updates == []

    @pytest.mark.asyncio
    async def test_update_balances_exception(self):
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(exc=Exception("boom"))
        await self.exchange._update_balances()
        # Logger error is called, balances not updated

    @pytest.mark.asyncio
    async def test_place_order_exception(self):
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(exc=Exception("boom"))
        with pytest.raises(Exception):
            await self.exchange._place_order("c2", self.trading_pair, Decimal("100"), self.buy_trade_type, OrderType.LIMIT, Decimal("0.000005"))

    @pytest.mark.asyncio
    async def test_place_cancel_exception(self):
        tracked_order = InFlightOrder("c3", self.trading_pair, OrderType.LIMIT, self.buy_trade_type, Decimal("100"), 0, price=Decimal("0.000005"), exchange_order_id="555")
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(exc=Exception("boom"))
        result = await self.exchange._place_cancel("555", tracked_order)
        assert result is False

    @pytest.mark.asyncio
    async def test_user_stream_event_listener_exception(self):
        async def mock_iter_queue():
            yield {"event": "orderUpdate", "order": None}  # Causes AttributeError
            raise asyncio.CancelledError

        from hummingbot.connector.exchange_py_base import ExchangePyBase
        original_iter = ExchangePyBase._iter_user_event_queue
        try:
            ExchangePyBase._iter_user_event_queue = mock_iter_queue
            with pytest.raises(asyncio.CancelledError):
                await self.exchange._user_stream_event_listener()
        finally:
            ExchangePyBase._iter_user_event_queue = original_iter

    @pytest.mark.asyncio
    async def test_get_last_traded_prices_invalid_resp(self):
        self.exchange._web_assistants_factory.build_rest_assistant = lambda: DummyRA(response="invalid")
        prices = await self.exchange.get_last_traded_prices([self.trading_pair])
        assert self.trading_pair not in prices

    def test_authenticator_and_factory_and_data_sources(self):
        # Authenticator
        from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
        from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import (
            WazirxAPIUserStreamDataSource,
        )
        from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
        from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

        auth = self.exchange.authenticator
        self.assertIsInstance(auth, WazirxAuth)

        factory = self.exchange._create_web_assistants_factory()
        self.assertIsInstance(factory, WebAssistantsFactory)

        ob_ds = self.exchange._create_order_book_data_source()
        self.assertIsInstance(ob_ds, WazirxAPIOrderBookDataSource)

        us_ds = self.exchange._create_user_stream_data_source()
        self.assertIsInstance(us_ds, WazirxAPIUserStreamDataSource)

    def test_initialize_trading_pair_symbols_from_exchange_info(self):
        exchange_info = {
            "symbols": [
                {"symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                 "baseAsset": self.base_asset,
                 "quoteAsset": self.quote_asset}
            ]
        }
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(exchange_info)
        mapping = self.async_run_with_timeout(self.exchange.trading_pair_symbol_map())
        ex_symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset).lower()
        self.assertIn(ex_symbol, mapping)
        self.assertEqual(mapping[ex_symbol], self.trading_pair)

    def test_get_order_book_raises_when_missing_and_tick_sets_notifier(self):
        with self.assertRaises(ValueError):
            self.exchange.get_order_book("NOT-EXIST")

        self.exchange._last_timestamp = 0
        timestamp = self.exchange.TICK_INTERVAL_LIMIT + 1
        self.exchange.tick(timestamp)
        self.assertTrue(self.exchange._poll_notifier.is_set())


@pytest.mark.asyncio
async def test_format_trading_rules_with_list_input():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    rule = {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
        ],
    }

    rules = await exchange._format_trading_rules([rule])
    assert isinstance(rules, list)


@pytest.mark.asyncio
async def test_format_trading_rules_with_dict_input():
    exchange = WazirxExchange("k", "s", trading_pairs=["ETH-USDT"])
    payload = {"symbols": [
        {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "filters": []}
    ]}

    rules = await exchange._format_trading_rules(payload)
    assert isinstance(rules, list)


@pytest.mark.asyncio
async def test_get_last_traded_prices_list_response():
    """Test that list responses are not processed (only dict responses are supported)."""
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    resp = [{"lastPrice": "123.45"}]
    exchange._web_assistants_factory = DummyFactory(resp=resp)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices.get("BTC-USDT", 0.0) == 0.0


@pytest.mark.asyncio
async def test_get_last_traded_prices_dict_response():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    resp = {"lastPrice": "200.0"}
    exchange._web_assistants_factory = DummyFactory(resp=resp)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices["BTC-USDT"] == 200.0


@pytest.mark.asyncio
async def test_get_last_traded_prices_handles_exceptions():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    exchange._web_assistants_factory = DummyFactory(resp=None, raise_exc=True)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices["BTC-USDT"] == 0.0


@pytest.mark.asyncio
async def test_all_trade_updates_for_order_and_request_order_status():
    """Test trade updates parsing for an open order.

    Note: We mock _wazirx_request since that's what the exchange actually uses,
    not the web_assistants_factory directly.
    """
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])

    order = InFlightOrder(
        client_order_id="c1",
        exchange_order_id="42",
        trading_pair="BTC-USDT",
        order_type=OrderType.LIMIT,
        trade_type=TradeType.BUY,
        price=Decimal("123.0"),
        amount=Decimal("0.5"),
        creation_timestamp=1650000000.0,
        initial_state=OrderState.OPEN
    )

    trades_resp = [
        {
            "id": "1",
            "orderId": "42",
            "commissionAsset": "USDT",
            "commission": "0.1",
            "qty": "0.5",
            "quoteQty": "61.5",
            "price": "123.0",
            "time": 1650000000000,
        }
    ]

    async def mock_wazirx_request(method, path, params=None, is_auth_required=False):
        return trades_resp

    exchange._wazirx_request = mock_wazirx_request
    exchange._trade_fee_schema = {}

    orig_new_spot_fee = TradeFeeBase.new_spot_fee
    TradeFeeBase.new_spot_fee = staticmethod(
        lambda fee_schema, trade_type, percent=Decimal(0), percent_token=None, flat_fees=None:
        AddedToCostTradeFee(percent=percent, percent_token=percent_token, flat_fees=flat_fees or [])
    )
    try:
        updates = await exchange._all_trade_updates_for_order(order)
    finally:
        TradeFeeBase.new_spot_fee = orig_new_spot_fee

    assert len(updates) == 1
    tu = updates[0]
    assert tu.trade_id == "1"
    assert float(tu.fill_price) == 123.0


class WazirxExchangeApiKeyRotationTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.responses = []
        self.sent = []
        self.exchange = self.create_exchange("k1,k2,k3", "s1,s2,s3")
        self.exchange._send_signed_request = AsyncMock(side_effect=self.fake_send)

    def create_exchange(self, keys: str, secrets: str, trading_required: bool = True) -> WazirxExchange:
        exchange = WazirxExchange(keys, secrets, trading_pairs=["BTC-INR"], trading_required=trading_required)
        exchange.logger().setLevel(1)
        exchange.logger().addHandler(self)
        self.addCleanup(exchange.logger().removeHandler, self)
        return exchange

    def handle(self, record):
        self.log_records.append(record)

    def is_logged(self, log_level: str, fragment: str) -> bool:
        return any(record.levelname == log_level and fragment in record.getMessage() for record in self.log_records)

    async def fake_send(self, auth, method, url, params):
        self.sent.append((auth.api_key, method, url))
        return self.responses.pop(0)

    def fake_send_by_key(self, outcomes):
        async def send(auth, method, url, params):
            self.sent.append((auth.api_key, method, url))
            outcome = outcomes.get(auth.api_key, (200, None, []))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return send

    async def signed_get(self, path: str = CONSTANTS.MY_TRADES_PATH_URL):
        return await self.exchange._wazirx_request("GET", path, {"symbol": "btcinr"}, is_auth_required=True)

    def keys_used(self):
        return [api_key for api_key, _, _ in self.sent]

    def test_constructor_rejects_mismatched_credentials(self):
        with self.assertRaises(ValueError):
            WazirxExchange("k1,k2", "s1", trading_pairs=[])

    def test_constructor_creates_one_auth_per_key(self):
        pool = self.exchange._key_pool
        self.assertEqual(["k1", "k2", "k3"], [pool.auth(i).api_key for i in range(pool.size)])
        self.assertEqual(["s1", "s2", "s3"], [pool.auth(i).secret_key for i in range(pool.size)])
        self.assertIs(self.exchange._auth, pool.auth(0))
        self.assertEqual("k1", self.exchange.api_key)

    async def test_signed_requests_rotate_across_keys(self):
        self.responses = [(200, None, []) for _ in range(4)]
        for _ in range(4):
            await self.signed_get()
        self.assertEqual(["k1", "k2", "k3", "k1"], self.keys_used())

    async def test_each_signed_request_logs_the_key_it_used(self):
        self.responses = [(429, None, "Too many api request"), (200, None, [])]

        await self.signed_get()

        self.assertTrue(self.is_logged("DEBUG", "WazirX GET /v1/myTrades used API key 1 of 3 (k1...): HTTP 429"))
        self.assertTrue(self.is_logged("DEBUG", "WazirX GET /v1/myTrades used API key 2 of 3 (k2...): HTTP 200"))

    async def test_order_requests_log_the_order_id_with_the_key(self):
        self.responses = [(200, None, {"id": 42}), (400, None, "Order does not exist")]

        await self.exchange._wazirx_request("POST", CONSTANTS.CREATE_ORDER_PATH_URL, {"symbol": "btcinr"}, True)
        with self.assertRaises(WazirxAPIError):
            await self.exchange._wazirx_request("DELETE", CONSTANTS.CANCEL_ORDER_PATH_URL, {"orderId": "7"}, True)

        self.assertTrue(self.is_logged("DEBUG", "WazirX POST /v1/order order 42 used API key 1 of 3 (k1...): HTTP 200"))
        self.assertTrue(self.is_logged("DEBUG", "WazirX DELETE /v1/order order 7 used API key 1 of 3 (k1...): HTTP 400"))

    async def test_order_endpoint_counts_each_method_against_its_own_limit(self):
        self.responses = [(200, None, {}) for _ in range(3)]
        pool = self.exchange._key_pool
        with patch.object(pool, "acquire", wraps=pool.acquire) as acquire:
            for method in ("POST", "get", "DELETE"):
                await self.exchange._wazirx_request(method, CONSTANTS.CREATE_ORDER_PATH_URL, {}, is_auth_required=True)

        self.assertEqual(
            [CONSTANTS.CREATE_ORDER_LIMIT_ID, CONSTANTS.ORDER_STATUS_LIMIT_ID, CONSTANTS.CANCEL_ORDER_LIMIT_ID],
            [call.args[0] for call in acquire.call_args_list])
        self.assertEqual(["POST", "GET", "DELETE"], [method for _, method, _ in self.sent])

    async def test_rate_limited_request_retries_once_on_another_key(self):
        self.responses = [(429, "3", '{"code":2136,"message":"Too many api request"}'), (200, None, {"id": 1})]

        result = await self.exchange._wazirx_request(
            "POST", CONSTANTS.CREATE_ORDER_PATH_URL, {"symbol": "btcinr"}, is_auth_required=True)

        self.assertEqual({"id": 1}, result)
        self.assertEqual(["k1", "k2"], self.keys_used())
        rest_until = self.exchange._key_pool._rest_until[(0, CONSTANTS.CREATE_ORDER_LIMIT_ID)]
        self.assertAlmostEqual(time.monotonic() + 3, rest_until, delta=0.5)
        self.assertTrue(self.is_logged("WARNING", "rate-limited API key 1 of 3"))

    async def test_second_rate_limit_raises(self):
        self.responses = [(429, None, "Too many api request"), (429, None, "Too many api request")]

        with self.assertRaises(WazirxAPIError) as context:
            await self.signed_get()

        self.assertEqual(429, context.exception.status)
        self.assertIn("key 2 of 3", str(context.exception))
        self.assertEqual(["k1", "k2"], self.keys_used())

    async def test_single_key_rate_limit_raises_and_rests_the_key(self):
        exchange = self.create_exchange("k1", "s1")
        exchange._send_signed_request = AsyncMock(return_value=(429, None, "Too many api request"))

        with self.assertRaises(WazirxAPIError):
            await exchange._wazirx_request("GET", CONSTANTS.MY_TRADES_PATH_URL, {}, is_auth_required=True)

        self.assertEqual(1, exchange._send_signed_request.await_count)
        rest_until = exchange._key_pool._rest_until[(0, CONSTANTS.MY_TRADES_PATH_URL)]
        self.assertAlmostEqual(time.monotonic() + CONSTANTS.DEFAULT_RATE_LIMIT_COOLDOWN, rest_until, delta=0.5)

    async def test_pinned_key_is_not_retried_on_another_key(self):
        self.responses = [(429, None, "Too many api request")]

        with self.assertRaises(WazirxAPIError):
            await self.exchange._signed_request("GET", CONSTANTS.USER_BALANCES_PATH_URL, {}, key_index=1)

        self.assertEqual(["k2"], self.keys_used())

    async def test_ip_ban_pauses_all_signed_requests(self):
        self.responses = [(418, "60", "IP banned")]

        with self.assertRaises(WazirxAPIError) as context:
            await self.signed_get()
        self.assertEqual(418, context.exception.status)
        self.assertTrue(self.is_logged("ERROR", "Signed requests are paused for 60s"))

        with self.assertRaisesRegex(IOError, "paused"):
            await self.signed_get()
        self.assertEqual(1, len(self.sent))

    async def test_recv_window_error_resyncs_the_clock_and_retries_once(self):
        self.responses = [(400, None, '{"code":2098,"message":"Request out of receiving window."}'), (200, None, [])]
        self.exchange._time_synchronizer.add_time_offset_ms_sample(5000)
        self.exchange._update_time_synchronizer = AsyncMock()

        result = await self.signed_get(CONSTANTS.USER_BALANCES_PATH_URL)

        self.assertEqual([], result)
        self.assertEqual(0, len(self.exchange._time_synchronizer._time_offset_ms))
        self.exchange._update_time_synchronizer.assert_awaited_once_with(pass_on_non_cancelled_error=True)
        self.assertEqual(2, len(self.sent))

    async def test_second_recv_window_error_raises(self):
        self.responses = [(400, None, '{"code":2098}'), (400, None, '{"code":2098}')]
        self.exchange._update_time_synchronizer = AsyncMock()

        with self.assertRaises(WazirxAPIError):
            await self.signed_get()

        self.assertEqual(2, len(self.sent))
        self.exchange._update_time_synchronizer.assert_awaited_once()

    async def test_other_errors_raise_with_the_key_named(self):
        self.responses = [(400, None, '{"code":2004,"message":"Order does not exist"}')]

        with self.assertRaises(WazirxAPIError) as context:
            await self.exchange._wazirx_request(
                "DELETE", CONSTANTS.CANCEL_ORDER_PATH_URL, {"orderId": "1"}, is_auth_required=True)

        message = str(context.exception)
        self.assertIn("with WazirX API key 1 of 3", message)
        self.assertIn("HTTP status is 400", message)
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(context.exception))
        self.assertEqual(1, len(self.sent))

    async def test_unsigned_requests_skip_the_key_pool(self):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_24HR_PATH_URL}"
        with aioresponses() as mock_api:
            mock_api.get(re.compile(rf"^{re.escape(url)}"), body=json.dumps({"lastPrice": "1"}))
            mock_api.post(url, body=json.dumps({"ok": True}))
            fetched = await self.exchange._wazirx_request("GET", CONSTANTS.TICKER_24HR_PATH_URL, {"symbol": "btcinr"})
            posted = await self.exchange._wazirx_request("POST", CONSTANTS.TICKER_24HR_PATH_URL, {"symbol": "btcinr"})
        with self.assertRaises(ValueError):
            await self.exchange._wazirx_request("DELETE", CONSTANTS.TICKER_24HR_PATH_URL)

        self.assertEqual({"lastPrice": "1"}, fetched)
        self.assertEqual({"ok": True}, posted)
        self.assertEqual([], self.sent)

    def test_retry_after_seconds(self):
        self.assertEqual(5.0, WazirxExchange._retry_after_seconds("5", 2.0))
        self.assertEqual(2.0, WazirxExchange._retry_after_seconds(None, 2.0))
        self.assertEqual(2.0, WazirxExchange._retry_after_seconds("soon", 2.0))
        self.assertEqual(2.0, WazirxExchange._retry_after_seconds("0", 2.0))

    async def test_send_signed_request_get_signs_the_query_string(self):
        exchange = self.create_exchange("k1", "s1")
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.USER_BALANCES_PATH_URL}"
        with aioresponses() as mock_api:
            mock_api.get(re.compile(rf"^{re.escape(url)}\?"), body=json.dumps([{"asset": "inr"}]))
            response = await exchange._send_signed_request(
                auth=exchange._key_pool.auth(0), method="GET", url=url, params={})
            (_, request_url), calls = next(iter(mock_api.requests.items()))

        self.assertEqual((200, None, [{"asset": "inr"}]), response)
        self.assertIn("recvWindow=60000", str(request_url))
        self.assertIn("signature=", str(request_url))
        self.assertEqual("k1", calls[0].kwargs["headers"]["X-Api-Key"])

    async def test_send_signed_request_post_and_delete_send_a_signed_body(self):
        exchange = self.create_exchange("k1", "s1")
        auth = exchange._key_pool.auth(0)
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.CREATE_ORDER_PATH_URL}"
        with aioresponses() as mock_api:
            mock_api.post(url, body=json.dumps({"id": 7}))
            mock_api.delete(url, status=400, body="Order does not exist", headers={"Retry-After": "1"})
            created = await exchange._send_signed_request(auth=auth, method="POST", url=url, params={"symbol": "btcinr"})
            canceled = await exchange._send_signed_request(auth=auth, method="DELETE", url=url, params={"orderId": "7"})
            post_body = mock_api.requests[("POST", URL(url))][0].kwargs["data"]
            delete_body = mock_api.requests[("DELETE", URL(url))][0].kwargs["data"]

        self.assertEqual((200, None, {"id": 7}), created)
        self.assertEqual((400, "1", "Order does not exist"), canceled)
        self.assertTrue(post_body.startswith("symbol=btcinr&recvWindow=60000&timestamp="))
        self.assertIn("&signature=", post_body)
        self.assertTrue(delete_body.startswith("orderId=7&recvWindow=60000&timestamp="))

    async def test_send_signed_request_rejects_unsupported_method(self):
        exchange = self.create_exchange("k1", "s1")
        with self.assertRaises(ValueError):
            await exchange._send_signed_request(
                auth=exchange._key_pool.auth(0), method="PUT", url=CONSTANTS.REST_URL, params={})

    async def test_key_check_leaves_rejected_key_out_of_rotation(self):
        self.exchange._send_signed_request = AsyncMock(side_effect=self.fake_send_by_key({
            "k2": (400, None, '{"code":2128,"message":"IP has not been whitelisted"}'),
        }))

        await self.exchange._check_api_keys()

        self.assertEqual([0, 2], self.exchange._key_pool.active_indexes)
        self.assertEqual({"k1", "k2", "k3"}, set(self.keys_used()))
        self.assertTrue(self.is_logged("ERROR", "rejected API key 2 of 3 at start-up"))
        self.assertTrue(self.is_logged("INFO", "across 2 of 3 API keys"))

        self.sent.clear()
        for _ in range(3):
            await self.signed_get()
        self.assertEqual(["k1", "k3", "k1"], self.keys_used())

    async def test_key_check_keeps_keys_whose_check_was_inconclusive(self):
        self.exchange._send_signed_request = AsyncMock(side_effect=self.fake_send_by_key({
            "k1": (500, None, "Internal Server Error"),
            "k2": (429, None, "Too many api request"),
            "k3": aiohttp.ClientConnectionError("Connection reset"),
        }))

        await self.exchange._check_api_keys()

        self.assertEqual([0, 1, 2], self.exchange._key_pool.active_indexes)
        for index in (1, 2, 3):
            self.assertTrue(self.is_logged("WARNING", f"Could not check WazirX API key {index} of 3"))

    async def test_key_check_keeps_every_key_when_all_are_rejected(self):
        self.exchange._send_signed_request = AsyncMock(side_effect=self.fake_send_by_key(
            {api_key: (401, None, "Unauthorized") for api_key in ("k1", "k2", "k3")}))

        await self.exchange._check_api_keys()

        self.assertEqual([0, 1, 2], self.exchange._key_pool.active_indexes)
        self.assertTrue(self.is_logged("ERROR", "rejected every API key"))

    async def test_key_check_timeout_keeps_every_key(self):
        async def hang(**kwargs):
            await asyncio.sleep(10)

        self.exchange._send_signed_request = AsyncMock(side_effect=hang)
        self.exchange._key_pool.set_active([0])

        with patch.object(CONSTANTS, "API_KEY_CHECK_TIMEOUT", 0.05):
            await self.exchange._check_api_keys()

        self.assertEqual([0, 1, 2], self.exchange._key_pool.active_indexes)
        self.assertTrue(self.is_logged("WARNING", "API key check timed out"))

    async def test_key_check_brings_back_a_key_that_passes_again(self):
        self.exchange._key_pool.set_active([0, 2])
        self.exchange._send_signed_request = AsyncMock(side_effect=self.fake_send_by_key({}))

        await self.exchange._check_api_keys()

        self.assertEqual([0, 1, 2], self.exchange._key_pool.active_indexes)

    async def test_start_network_checks_keys_only_when_trading_with_several_keys(self):
        cases = [
            (self.exchange, 1),
            (self.create_exchange("k1", "s1"), 0),
            (self.create_exchange("k1,k2", "s1,s2", trading_required=False), 0),
        ]
        with patch.object(ExchangePyBase, "start_network", new_callable=AsyncMock) as base_start_network:
            for exchange, expected_checks in cases:
                exchange._check_api_keys = AsyncMock()
                await exchange.start_network()
                self.assertEqual(expected_checks, exchange._check_api_keys.await_count)

        self.assertEqual(3, base_start_network.await_count)
