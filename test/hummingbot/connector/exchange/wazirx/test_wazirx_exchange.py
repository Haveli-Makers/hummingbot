
import json
from decimal import Decimal

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase


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
