import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _make_exchange(**kwargs) -> ZebpayExchange:
    defaults = dict(
        zebpay_api_key="test_key",
        zebpay_api_secret="test_secret",
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    defaults.update(kwargs)
    return ZebpayExchange(**defaults)


async def _async_gen(items):
    for item in items:
        yield item


class ZebpayExchangePropertiesTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # IsolatedAsyncioTestCase provides a running event loop during setUp,
        # which ExchangePyBase needs when constructing the connector.
        self.exchange = _make_exchange()

    def test_name(self):
        self.assertEqual("zebpay", self.exchange.name)

    def test_prefix_alphanumeric(self):
        self.assertTrue(self.exchange.client_order_id_prefix.isalnum())

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)

    def test_request_paths(self):
        self.assertEqual(CONSTANTS.EXCHANGE_INFO_PATH_URL, self.exchange.trading_rules_request_path)
        self.assertEqual(CONSTANTS.EXCHANGE_INFO_PATH_URL, self.exchange.trading_pairs_request_path)
        self.assertEqual(CONSTANTS.PING_PATH_URL, self.exchange.check_network_request_path)

    def test_cancel_synchronous(self):
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)

    def test_is_request_exception_related_to_time_synchronizer(self):
        f = self.exchange._is_request_exception_related_to_time_synchronizer
        # A bare "invalid signature" (clock skew, no "time" word) must trigger a
        # resync — the previous `or ... and ...` grouping returned False here.
        self.assertTrue(f(IOError("invalid signature")))
        self.assertTrue(f(IOError("timestamp expired")))
        self.assertTrue(f(IOError("request time too old")))
        self.assertFalse(f(IOError("insufficient balance")))


class ZebpayExchangeTradingPairTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange()

    async def test_symbol_map_from_exchange_info_dict_list(self):
        info = {"data": [
            {"symbol": "BTC-INR", "baseAsset": "BTC", "quoteAsset": "INR"},
            {"symbol": "ETH-INR", "baseAsset": "ETH", "quoteAsset": "INR"},
        ]}
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(info)
        self.assertEqual("BTC-INR", await self.exchange.trading_pair_associated_to_exchange_symbol("BTC-INR"))
        self.assertEqual("BTC-INR", await self.exchange.exchange_symbol_associated_to_pair("BTC-INR"))

    async def test_format_trading_rules(self):
        info = {"data": [{
            "symbol": "BTC-INR", "baseAsset": "BTC", "quoteAsset": "INR",
            "pricePrecision": 0, "quantityPrecision": 6, "tickSz": "1", "lotSz": "0.000001",
        }]}
        rules = await self.exchange._format_trading_rules(info)
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BTC-INR", rule.trading_pair)
        self.assertEqual(Decimal("1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000001"), rule.min_base_amount_increment)
        # exchangeInfo has no minNotional → INR pairs fall back to the known 99 INR floor.
        self.assertEqual(Decimal("99"), rule.min_notional_size)

    async def test_format_trading_rules_uses_api_min_notional_when_present(self):
        info = {"data": [{
            "symbol": "BTC-INR", "baseAsset": "BTC", "quoteAsset": "INR",
            "tickSz": "1", "lotSz": "0.000001", "minNotional": "150",
        }]}
        rules = await self.exchange._format_trading_rules(info)
        self.assertEqual(Decimal("150"), rules[0].min_notional_size)

    async def test_format_trading_rules_warns_on_default_increments(self):
        # No tickSz/lotSz and no precision → hardcoded fallback increments + a warning.
        info = {"data": [{"symbol": "FOO-INR", "baseAsset": "FOO", "quoteAsset": "INR"}]}
        with self.assertLogs(level="WARNING") as cm:
            rules = await self.exchange._format_trading_rules(info)
        self.assertEqual(CONSTANTS.DEFAULT_PRICE_INCREMENT, rules[0].min_price_increment)
        self.assertEqual(CONSTANTS.DEFAULT_BASE_INCREMENT, rules[0].min_base_amount_increment)
        self.assertTrue(any("fallback" in line.lower() for line in cm.output))

    async def test_volume_tickers_resolve_symbol_before_filtering(self):
        # A non-dashed exchange symbol must still match the requested HB pair via the
        # symbol map; filtering on the raw symbol would drop it.
        self.exchange._set_trading_pair_symbol_map(bidict({"BTCINR": "BTC-INR"}))
        self.exchange.get_all_pairs_prices = AsyncMock(return_value=[
            {"symbol": "BTCINR", "volume": "10"},
            {"symbol": "ETHINR", "volume": "5"},
        ])
        out = await self.exchange.get_all_24h_volume_tickers(["BTC-INR"])
        self.assertEqual(1, len(out))
        self.assertEqual("BTCINR", out[0]["symbol"])


class ZebpayExchangeBalanceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange()

    async def test_update_balances(self):
        resp = {"data": [
            {"currency": "BTC", "total": "1.2", "free": "1.0", "used": "0.2"},
            {"currency": "INR", "total": "80000", "free": "80000", "used": "0"},
        ]}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = resp
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1.2"), self.exchange._account_balances["BTC"])
        self.assertEqual(Decimal("1.0"), self.exchange._account_available_balances["BTC"])
        self.assertEqual(Decimal("80000"), self.exchange._account_balances["INR"])

    async def test_update_balances_removes_stale(self):
        self.exchange._account_balances["STALE"] = Decimal("9")
        self.exchange._account_available_balances["STALE"] = Decimal("9")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": [{"currency": "BTC", "total": "1", "free": "1", "used": "0"}]}
            await self.exchange._update_balances()
        self.assertNotIn("STALE", self.exchange._account_balances)

    async def test_update_balances_empty_list_reflects_empty_account(self):
        # A genuine empty account (HTTP 200 + {"data": []}) must wipe stale balances;
        # keeping them would let the strategy size orders against funds it no longer has.
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": [], "statusCode": 200}
            await self.exchange._update_balances()
        self.assertNotIn("BTC", self.exchange._account_balances)

    async def test_update_balances_degenerate_payload_keeps_balances(self):
        # A degenerate {"data": null} (not an empty list) is a transient hiccup, not a
        # real empty account — keep the last known balances.
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": None, "statusCode": 200}
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1"), self.exchange._account_balances.get("BTC"))

    async def test_update_balances_unparseable_non_empty_list_keeps_balances(self):
        # REGRESSION: the guard used to ask "is the payload NOT a list?", so a NON-EMPTY
        # list whose items all fail to parse (renamed field, partial-outage body) slipped
        # through — it IS a list — and the stale-removal loop wiped every tracked balance.
        # The strategy then sees zero funds and stops sizing orders. The guard now asks
        # "is the payload POSITIVELY empty?" instead, so this keeps the last known state.
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            # "symbol"/"amount" instead of the expected "currency"/"total" keys.
            mock_get.return_value = {"data": [{"symbol": "BTC", "amount": "1.0"}], "statusCode": 200}
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1"), self.exchange._account_balances.get("BTC"))
        self.assertEqual(Decimal("1"), self.exchange._account_available_balances.get("BTC"))

    async def test_update_balances_empty_dict_payload_keeps_balances(self):
        # {"data": {}} carries no balance list at all — degenerate, not an empty account.
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {}, "statusCode": 200}
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1"), self.exchange._account_balances.get("BTC"))

    async def test_update_balances_empty_nested_list_reflects_empty_account(self):
        # The dict-shaped empty account ({"balances": []}) is still POSITIVELY empty and
        # must wipe, just like the bare {"data": []} form.
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"balances": []}, "statusCode": 200}
            await self.exchange._update_balances()
        self.assertNotIn("BTC", self.exchange._account_balances)

    async def test_update_balances_business_error_does_not_wipe(self):
        self.exchange._account_balances["BTC"] = Decimal("1")
        self.exchange._account_available_balances["BTC"] = Decimal("1")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": None, "statusCode": 500, "statusDescription": "blip"}
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1"), self.exchange._account_balances.get("BTC"))


class ZebpayExchangeOrderTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange(trading_pairs=["BTC-INR"])
        self.exchange._set_trading_pair_symbol_map(bidict({"BTC-INR": "BTC-INR"}))

    async def test_place_order_builds_payload(self):
        resp = {"data": {"orderId": "ord-1", "status": "OPEN", "timestamp": 1_700_000_000_000}}
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = resp
            oid, ts = await self.exchange._place_order(
                order_id="ZEBtest", trading_pair="BTC-INR", amount=Decimal("0.001"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("3000000"),
            )
        self.assertEqual("ord-1", oid)
        body = mock_post.call_args.kwargs.get("data")
        self.assertEqual("BTC-INR", body["symbol"])
        self.assertEqual("BUY", body["side"])
        self.assertEqual("LIMIT", body["type"])
        self.assertEqual("3000000", body["price"])
        self.assertEqual("0.001", body["amount"])

    async def test_place_order_rejection_raises(self):
        # Zebpay rejects with HTTP 200 + statusCode 77 (price out of band) → must raise.
        rejected = {"data": None, "statusCode": 77,
                    "statusDescription": "Rate should be in the range of 5694116 - 7703804"}
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = rejected
            with self.assertRaises(IOError):
                await self.exchange._place_order(
                    order_id="ZEBtest", trading_pair="BTC-INR", amount=Decimal("0.001"),
                    trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("3000000"),
                )

    async def test_place_order_missing_orderid_raises(self):
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"data": {}, "statusCode": 200}
            with self.assertRaises(IOError):
                await self.exchange._place_order(
                    order_id="ZEBtest", trading_pair="BTC-INR", amount=Decimal("0.001"),
                    trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("3000000"),
                )

    async def test_place_cancel_success(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_delete", new_callable=AsyncMock) as mock_del:
            mock_del.return_value = {"data": {"orderId": "ord-1", "symbol": "BTC-INR", "status": "CANCELLED"}}
            self.assertTrue(await self.exchange._place_cancel("ZEBtest", tracked))

    async def test_request_order_status_filled(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "FILLED",
                                              "filled": "0.001", "updatedAt": 1_700_000_001_000}}
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.FILLED, upd.new_state)

    async def test_request_order_status_partial(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "OPEN", "filled": "0.0005"}}
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.PARTIALLY_FILLED, upd.new_state)

    async def test_request_order_status_unknown_raises(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "WAT"}}
            with self.assertRaises(ValueError):
                await self.exchange._request_order_status(tracked)

    def test_is_order_not_found_during_cancelation_error(self):
        # Terminal-state cancel rejections must classify as not-found so the base
        # cancel flow settles the order instead of leaving it in-flight.
        f = self.exchange._is_order_not_found_during_cancelation_error
        self.assertTrue(f(IOError("Zebpay API error (statusCode 88): Order already cancelled")))
        self.assertTrue(f(IOError("Order not in active state")))
        self.assertTrue(f(IOError("order already filled")))
        self.assertTrue(f(IOError("Order not found")))
        self.assertFalse(f(IOError("insufficient balance")))

    async def test_all_trade_updates_business_error_raises(self):
        # A 200-business-error on the fills path must propagate, not silently
        # record zero fills.
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": None, "statusCode": 500, "statusDescription": "blip"}
            with self.assertRaises(IOError):
                await self.exchange._all_trade_updates_for_order(tracked)

    async def test_all_trade_updates_parses_fills(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"fills": [{
                "id": "f1", "price": "3000000", "amount": "0.001",
                "fees": "3", "feeCurrency": "INR", "createdAt": 1_700_000_001_000,
            }]}}
            updates = await self.exchange._all_trade_updates_for_order(tracked)
        self.assertEqual(1, len(updates))
        self.assertEqual(Decimal("3000000"), updates[0].fill_price)
        self.assertEqual(Decimal("0.001"), updates[0].fill_base_amount)

    async def test_trade_update_event_records_fills(self):
        # The realtime account-trades user-stream event records fills onto the tracked
        # order (process_trade_update), surfacing them faster than the status loop.
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        event = {"event": "trade_update", "data": [{
            "orderId": "ord-1",
            "fills": [{"id": "f1", "price": "100", "amount": "0.5", "fees": "0.1",
                       "feeCurrency": "INR", "createdAt": 1_700_000_000_000}],
        }]}
        with patch.object(self.exchange, "_iter_user_event_queue", return_value=_async_gen([event])):
            await self.exchange._user_stream_event_listener()
        self.assertEqual(Decimal("0.5"), order.executed_amount_base)

    async def test_idless_fill_trade_ids_are_stable_across_reordered_polls(self):
        # REGRESSION: the fallback trade_id for a fill with no native id used to be
        # f"{order_id}-{len(trade_updates)}" — its INDEX in the response. If /fills came
        # back in a different order on a later poll, that index moved to a different
        # fill: the reused id was deduped away and the other fill looked new and was
        # applied twice, double-counting executed_amount_base. Ids are now derived from
        # the fill's own content, so reordering the same fills yields the same ids.
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        f1 = {"price": "100", "amount": "0.3", "fees": "0.1",
              "feeCurrency": "INR", "createdAt": 1_700_000_001_000}
        f2 = {"price": "101", "amount": "0.4", "fees": "0.1",
              "feeCurrency": "INR", "createdAt": 1_700_000_002_000}

        ids_first = [u.trade_id for u in self.exchange._build_trade_updates_from_fills(order, [f1])]
        ids_reordered = [u.trade_id for u in self.exchange._build_trade_updates_from_fills(order, [f2, f1])]

        # f1 keeps the same id no matter where it sits in the list.
        self.assertEqual(ids_first[0], ids_reordered[1])
        # ...and f2 gets its own distinct id rather than inheriting f1's slot.
        self.assertNotEqual(ids_reordered[0], ids_reordered[1])
        self.assertNotIn("ord-1-0", ids_reordered)

    async def test_idless_identical_fills_get_distinct_ids(self):
        # Two fills identical in every field must still be counted separately, so the
        # content-derived id carries an occurrence suffix.
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        fill = {"price": "100", "amount": "0.3", "fees": "0.1",
                "feeCurrency": "INR", "createdAt": 1_700_000_001_000}
        ids = [u.trade_id for u in self.exchange._build_trade_updates_from_fills(order, [dict(fill), dict(fill)])]
        self.assertEqual(2, len(set(ids)))

    async def test_native_fill_id_still_preferred(self):
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        updates = self.exchange._build_trade_updates_from_fills(
            order, [{"id": "f1", "price": "100", "amount": "0.3", "createdAt": 1}]
        )
        self.assertEqual("f1", updates[0].trade_id)

    async def test_terminal_order_update_records_final_fill_before_untracking(self):
        # REGRESSION: a terminal order_update untracks the order, and an untracked order
        # is invisible to the independent account-trades poll (it iterates
        # in_flight_orders). A fill landing in the same ~2s window the order settled was
        # therefore never recorded and executed_amount_base stayed under-reported for
        # good. The listener now pulls the outstanding fills BEFORE processing the
        # terminal update. (CSX already had this fix; Zebpay did not.)
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        # The settled-order fast path reports FILLED with cumulative filled=1.0, but
        # nothing has been applied to the order yet.
        event = {"event": "order_update", "data": [{
            "orderId": "ord-1", "status": "FILLED", "filled": "1.0",
            "updatedAt": 1_700_000_002_000,
        }]}
        fills_resp = {"data": {"fills": [{
            "id": "f-final", "price": "100", "amount": "1.0", "fees": "0.1",
            "feeCurrency": "INR", "createdAt": 1_700_000_001_000,
        }]}}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = fills_resp
            with patch.object(self.exchange, "_iter_user_event_queue", return_value=_async_gen([event])):
                await self.exchange._user_stream_event_listener()
        # process_order_update defers via safe_ensure_future; let it run.
        await asyncio.sleep(0.01)

        self.assertEqual(Decimal("1.0"), order.executed_amount_base)
        self.assertEqual(OrderState.FILLED, order.current_state)

    async def test_terminal_filled_without_filled_field_still_fetches_fills(self):
        # A FILLED payload that omits the cumulative `filled` field must still trigger
        # the fills fetch — the order says it is done while we have applied less than
        # its full amount, so something is outstanding.
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        event = {"event": "order_update", "data": [{
            "orderId": "ord-1", "status": "FILLED", "updatedAt": 1_700_000_002_000,
        }]}  # note: no "filled" key
        fills_resp = {"data": {"fills": [{
            "id": "f-final", "price": "100", "amount": "1.0", "fees": "0.1",
            "feeCurrency": "INR", "createdAt": 1_700_000_001_000,
        }]}}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = fills_resp
            with patch.object(self.exchange, "_iter_user_event_queue", return_value=_async_gen([event])):
                await self.exchange._user_stream_event_listener()
        self.assertEqual(Decimal("1.0"), order.executed_amount_base)

    async def test_terminal_order_update_skips_fills_fetch_when_nothing_outstanding(self):
        # A clean cancel with no unapplied fill must not cost an extra /fills request.
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        event = {"event": "order_update", "data": [{
            "orderId": "ord-1", "status": "CANCELLED", "filled": "0",
            "updatedAt": 1_700_000_002_000,
        }]}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            with patch.object(self.exchange, "_iter_user_event_queue", return_value=_async_gen([event])):
                await self.exchange._user_stream_event_listener()
        mock_get.assert_not_called()

    async def test_terminal_order_update_still_settles_when_fills_fetch_fails(self):
        # A failing fills fetch must never block the terminal transition — an order
        # tracked forever is worse than one missed fill (the status poll retries).
        order = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1.0"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        event = {"event": "order_update", "data": [{
            "orderId": "ord-1", "status": "FILLED", "filled": "1.0",
            "updatedAt": 1_700_000_002_000,
        }]}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = IOError("fills endpoint down")
            with patch.object(self.exchange._order_tracker, "process_order_update") as mock_update:
                with patch.object(self.exchange, "_iter_user_event_queue", return_value=_async_gen([event])):
                    with self.assertLogs(level="WARNING") as cm:
                        await self.exchange._user_stream_event_listener()

        # The failure is reported, but the terminal OrderUpdate is still submitted.
        self.assertTrue(any("Could not record final fills" in line for line in cm.output))
        mock_update.assert_called_once()
        self.assertEqual(OrderState.FILLED, mock_update.call_args.kwargs["order_update"].new_state)

    async def test_request_order_status_timestamp_in_seconds_not_divided(self):
        # A seconds-scale timestamp (< 1e12) must not be divided by 1000.
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "OPEN",
                                              "updatedAt": 1_700_000_000}}  # seconds
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(1_700_000_000.0, upd.update_timestamp)

    async def test_request_order_status_timestamp_in_millis_divided(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "OPEN",
                                              "updatedAt": 1_700_000_000_000}}  # millis
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(1_700_000_000.0, upd.update_timestamp)


if __name__ == "__main__":
    unittest.main()
