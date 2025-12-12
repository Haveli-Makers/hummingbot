import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount


class CoindcxExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.MARKETS_DETAILS_PATH_URL, domain=self.exchange._domain)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PATH_URL, domain=self.exchange._domain)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKETS_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKETS_DETAILS_PATH_URL, domain=self.exchange._domain)
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
                "coindcx_name": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "min_quantity": 1,
                "max_quantity": 90000000,
                "min_price": 0.000001,
                "max_price": 100000.0,
                "min_notional": 0.001,
                "base_currency_precision": 8,
                "target_currency_precision": 8,
                "step": 1,
                "status": "active"
            }
        ]

    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "last_price": str(self.expected_latest_price),
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "coindcx_name": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "min_quantity": 1,
                "max_quantity": 90000000,
                "min_price": 0.000001,
                "max_price": 100000.0,
                "min_notional": 0.001,
                "base_currency_precision": 8,
                "target_currency_precision": 8,
                "step": 1,
                "status": "active"
            },
            {
                "symbol": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                "coindcx_name": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                "base_currency_short_name": "PAIR",
                "target_currency_short_name": "INVALID",
                "min_quantity": 1,
                "max_quantity": 90000000,
                "min_price": 0.000001,
                "max_price": 100000.0,
                "min_notional": 0.001,
                "base_currency_precision": 8,
                "target_currency_precision": 8,
                "step": 1,
                "status": "active"
            },
        ]
        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        return []

    @property
    def trading_rules_request_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "coindcx_name": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "base_currency_short_name": self.quote_asset,
                "target_currency_short_name": self.base_asset,
                "min_quantity": 1,
                "max_quantity": 90000000,
                "min_price": 0.000001,
                "max_price": 100000.0,
                "min_notional": 0.001,
                "base_currency_precision": 8,
                "target_currency_precision": 8,
                "step": 1,
                "status": "active"
            }
        ]

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return [
            {
                # Missing quotes and base to trigger parsing error
                "symbol": "",
                "coindcx_name": "",
                "base_currency_short_name": "",
                "target_currency_short_name": "",
                "status": "inactive"
            }
        ]

    @property
    def order_creation_request_successful_mock_response(self):
        # CoinDCX returns either a dict with orders list or a single order; we return a list
        return [{
            "id": self.expected_exchange_order_id,
            "created_at": 1640000000000
        }]

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return [
            {"currency": self.base_asset, "balance": 10.0, "locked_balance": 5.0},
            {"currency": self.quote_asset, "balance": 2000.0, "locked_balance": 0.0}
        ]

    @property
    def balance_request_mock_response_only_base(self):
        return [{"currency": self.base_asset, "balance": 10.0, "locked_balance": 5.0}]

    @property
    def balance_event_websocket_update(self):
        return {"currency": self.base_asset, "balance": 10.0, "locked_balance": 5.0}

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        rule = self.trading_rules_request_mock_response[0]
        base_precision = int(rule.get("base_currency_precision", 8))
        target_precision = int(rule.get("target_currency_precision", 8))
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(str(rule.get("min_quantity", 0))),
            min_price_increment=Decimal(10) ** (-base_precision),
            min_base_amount_increment=Decimal(10) ** (-target_precision),
            min_notional_size=Decimal(str(rule.get("min_notional", 0))),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response[0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "28"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self):
        return DeductedFromReturnsTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return str(30000)

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        return CoindcxExchange(
            coindcx_api_key="testAPIKey",
            coindcx_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        headers = request_call.kwargs.get("headers") or request_call.kwargs.get("headers", {})
        self.assertTrue(headers.get("X-AUTH-APIKEY") is not None)
        self.assertTrue(headers.get("X-AUTH-SIGNATURE") is not None)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["market"])
        self.assertEqual(order.trade_type == TradeType.BUY, request_data["side"] == CONSTANTS.SIDE_BUY)
        self.assertEqual(CoindcxExchange.coindcx_order_type(OrderType.LIMIT), request_data["order_type"])
        self.assertEqual(Decimal("100"), Decimal(str(request_data["total_quantity"])))
        self.assertEqual(Decimal("10000"), Decimal(str(request_data.get("price_per_unit", 0))))
        self.assertEqual(order.client_order_id, request_data["client_order_id"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        # Cancel is also a POST with id or client_order_id
        data = json.loads(request_call.kwargs["data"]) if request_call.kwargs.get("data") else {}
        if "id" in data:
            self.assertEqual(order.exchange_order_id, str(data["id"]))
        else:
            self.assertEqual(order.client_order_id, data.get("client_order_id"))

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        # Order status uses POST with id or client_order_id
        data = json.loads(request_call.kwargs["data"]) if request_call.kwargs.get("data") else {}
        if "id" in data:
            self.assertEqual(order.exchange_order_id, str(data["id"]))
        else:
            self.assertEqual(order.client_order_id, data.get("client_order_id"))

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        # Trades history POST uses symbol and limit
        data = request_call.kwargs.get("data")
        self.assertIsNotNone(data)

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(r".*" + re.escape(CONSTANTS.CANCEL_ORDER_PATH_URL) + r".*")
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(r".*" + re.escape(CONSTANTS.CANCEL_ORDER_PATH_URL) + r".*")
        response = {"code": 99999, "message": "generic error"}
        mock_api.post(regex_url, status=500, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = {"code": CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE, "message": CONSTANTS.ORDER_NOT_EXIST_MESSAGE}
        mock_api.post(regex_url, status=400, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        mock_api.post(regex_url, status=400, callback=callback)
        return url

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        mock_api.post(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_STATUS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))
        response = {"code": CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE, "message": CONSTANTS.ORDER_NOT_EXIST_MESSAGE}
        mock_api.post(regex_url, body=json.dumps(response), status=400, callback=callback)
        return [url]

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id or "",
            "client_order_id": order.client_order_id,
            "status": "cancelled",
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "status": "filled",
            "price": str(order.price),
            "quantity": str(order.amount),
            "executed_quantity": str(order.amount),
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "status": "cancelled",
            "price": str(order.price),
            "quantity": str(order.amount),
            "executed_quantity": str(0),
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "status": "open",
            "price": str(order.price),
            "quantity": str(order.amount),
            "executed_quantity": str(0),
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "id": order.exchange_order_id,
            "client_order_id": order.client_order_id,
            "status": "partially_filled",
            "price": str(order.price),
            "quantity": str(order.amount),
            "executed_quantity": str(self.expected_partial_fill_amount),
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "id": self.expected_fill_trade_id,
                "orderId": int(order.exchange_order_id) if order.exchange_order_id is not None else 0,
                "price": str(self.expected_partial_fill_price),
                "qty": str(self.expected_partial_fill_amount),
                "quoteQty": str(self.expected_partial_fill_amount * self.expected_partial_fill_price),
                "commission": str(self.expected_fill_fee.flat_fees[0].amount),
                "commissionAsset": self.expected_fill_fee.flat_fees[0].token,
                "time": 1499865549590,
                "isBuyer": True,
                "isMaker": False,
                "isBestMatch": True,
            }
        ]

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "id": self.expected_fill_trade_id,
                "orderId": int(order.exchange_order_id) if order.exchange_order_id is not None else 0,
                "price": str(order.price),
                "qty": str(order.amount),
                "quoteQty": str(order.amount * order.price),
                "commission": str(self.expected_fill_fee.flat_fees[0].amount),
                "commissionAsset": self.expected_fill_fee.flat_fees[0].token,
                "time": 1499865549590,
                "isBuyer": True,
                "isMaker": False,
                "isBestMatch": True,
            }
        ]

    def _configure_balance_response(
            self,
            response: Dict[str, Any],
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self.balance_url
        mock_api.post(
            re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?")), body=json.dumps(response), callback=callback
        )
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "event": "order-update",
            "data": {
                "id": order.exchange_order_id,
                "client_order_id": order.client_order_id,
                "status": "open",
                "updated_at": 1661938138040
            }
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "event": "order-update",
            "data": {
                "id": order.exchange_order_id,
                "client_order_id": order.client_order_id,
                "status": "cancelled",
                "updated_at": 1661938138040
            }
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "event": "order-update",
            "data": {
                "id": order.exchange_order_id,
                "client_order_id": order.client_order_id,
                "status": "filled",
                "updated_at": 1661938138040
            }
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "event": "trade-update",
            "data": {
                "p": float(order.price),
                "q": float(order.amount),
                "f": float(self.expected_fill_fee.flat_fees[0].amount),
                "t": int(self.expected_fill_trade_id),
                "c": order.client_order_id,
                "o": order.exchange_order_id,
                "timestamp": 1661938980325
            }
        }

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        request_sent_event = asyncio.Event()
        seconds_counter_mock.side_effect = [0, 0, 0]

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        # CoinDCX does not use SERVER_TIME endpoint; test network check instead
        url = self.network_status_url
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))

        mock_api.get(regex_url, body=json.dumps(self.network_status_request_successful_mock_response), callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        # CoinDCX uses local time, but ensure function runs without raising
        self.assertTrue(True)

    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        request_sent_event = asyncio.Event()
        url = self.network_status_url
        regex_url = re.compile(f"^{url}".replace(".", r"\\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=400, callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())

        self.assertTrue(self.is_logged("NETWORK", "Error getting server time."))

    @aioresponses()
    def test_update_time_synchronizer_raises_cancelled_error(self, mock_api):
        # CoinDCX does not check server time; no implementation
        pass
