import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from test.hummingbot.connector.derivative.delta_options.test_delta_options_utils import option_product
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_derivative import (
    DeltaOptionsDerivative,
    _SettlementDue,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import PositionSide, TradeType
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

CALL = "C_BTC_100000_251226-USD"
PUT = "P_BTC_90000_251226-USD"
EXPIRY = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()
PRODUCTS = [option_product("C-BTC-100000-251226", id=11), option_product("P-BTC-90000-251226", "put_options", id=12)]


def settlement_fill(product_id=11, symbol="C-BTC-100000-251226", price="1142.66222222224", commission="0.00878319",
                    side="sell", fill_id="1f667448e9be44b48b9660ba1e7f4b1a"):
    """Shaped as Delta wrote it for a call held through the 10 Nov 2026 expiry."""
    return {
        "id": fill_id, "fill_type": "settlement", "side": side, "size": "1", "price": price, "commission": commission,
        "product_id": product_id, "product_symbol": symbol, "product": symbol, "role": "taker",
        "order_id": "07390bdf02924ff98bcb2009b774dcf2", "notional": "74.4338", "settling_asset_symbol": "USD",
        "settling_asset_id": 14, "created_at": "2026-12-25T12:00:00.459282Z", "credits_used": "0",
        "meta_data": {"is_commission_on_notional": True, "new_position": {"commission": "0", "size": 0},
                      "order_price": price, "order_type": "limit_order", "order_unfilled_size": "0"},
    }


def buy_fill():
    return {"id": "d70041d567924bdf83c23e096b5b2e15", "fill_type": "normal", "side": "buy", "size": "1",
            "price": "1131", "commission": "0.00877014", "product_id": 11, "product_symbol": "C-BTC-100000-251226",
            "order_id": "1668729403", "settling_asset_symbol": "USD", "created_at": "2026-12-25T11:10:58.659647Z"}


def fills_response(*fills):
    return {"success": True, "result": list(fills), "meta": {"after": None}}


class DeltaOptionsSettlementTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = self._connector([CALL, PUT], PRODUCTS)
        # Held here: connectors keep their listeners weakly.
        self.settlements = EventLogger()
        self.connector.add_listener(MarketEvent.OptionSettled, self.settlements)

    @staticmethod
    def _connector(trading_pairs, products):
        connector = DeltaOptionsDerivative("key", "secret", trading_pairs=trading_pairs, trading_required=False)
        connector._initialize_trading_pair_symbols_from_exchange_info(products)
        connector._api_get = AsyncMock(return_value=fills_response())
        # Each test drives the watch by hand.
        connector._start_settlement_watch = MagicMock()
        return connector

    def _at(self, timestamp):
        return patch.object(self.connector, "_time", return_value=timestamp)

    def _hold(self, trading_pair, amount):
        amount = Decimal(amount)
        position = Position(trading_pair=trading_pair, position_side=PositionSide.LONG if amount > 0 else PositionSide.SHORT,
                            unrealized_pnl=Decimal("0"), entry_price=Decimal("1131"), amount=amount,
                            leverage=Decimal("1"))
        self.connector._options_trading.set_position(trading_pair, position)

    def _expire(self):
        with self._at(EXPIRY):
            self.connector._check_expiries()

    # ── What expiry hands over ────────────────────────────────────────────────

    def test_expiry_marks_each_option_for_settlement_with_whether_it_was_held(self):
        self._hold(CALL, "0.001")

        self._expire()

        due = self.connector._settlements_due
        self.assertEqual({CALL: True, PUT: False}, {pair: entry.held for pair, entry in due.items()})
        self.assertEqual(EXPIRY + CONSTANTS.SETTLEMENT_WAIT, due[CALL].until)
        self.connector._start_settlement_watch.assert_called()

    def test_a_position_delta_closed_at_the_expiry_still_counts_as_held(self):
        self._hold(CALL, "0.001")
        with self._at(EXPIRY - 3):
            self.connector._check_expiries()
        # Delta's position stream drops it half a second after the expiry, before the next check.
        self.connector._options_trading.remove_position(CALL)

        with self._at(EXPIRY + 2):
            self.connector._check_expiries()

        self.assertTrue(self.connector._settlements_due[CALL].held)

    # ── Reading the settlement ────────────────────────────────────────────────

    async def test_a_long_is_closed_by_delta_settlement_fill(self):
        self._hold(CALL, "0.001")
        self._expire()
        no_product = dict(settlement_fill(fill_id="no-product"), product_id=None)
        self.connector._api_get.return_value = fills_response(buy_fill(), no_product, settlement_fill())

        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()

        self.assertEqual(1, len(self.settlements.event_log))
        event = self.settlements.event_log[0]
        self.assertEqual((CALL, TradeType.SELL, Decimal("0.001"), Decimal("1142.66222222224")),
                         (event.trading_pair, event.trade_type, event.amount, event.price))
        self.assertEqual([TokenAmount(token="USD", amount=Decimal("0.00878319"))], event.trade_fee.flat_fees)
        self.assertEqual(("1f667448e9be44b48b9660ba1e7f4b1a", "delta_options"), (event.exchange_trade_id, event.market))
        self.assertEqual(datetime(2026, 12, 25, 12, 0, 0, 459282, tzinfo=timezone.utc).timestamp(), event.timestamp)
        self.assertNotIn(CALL, self.connector._settlements_due)
        self.assertTrue(self.connector._poll_notifier.is_set())
        # Both options are read in one request, from a minute before the expiry.
        self.connector._api_get.assert_awaited_once_with(
            path_url=CONSTANTS.FILLS_PATH_URL, is_auth_required=True,
            params={"product_ids": "11,12", "page_size": CONSTANTS.FILLS_PAGE_SIZE,
                    "start_time": int((EXPIRY - 60) * 1e6)})

    async def test_a_worthless_option_is_settled_at_zero(self):
        self._hold(CALL, "0.001")
        self._expire()
        self.connector._api_get.return_value = fills_response(settlement_fill(price="0", commission="0"))

        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()

        event = self.settlements.event_log[0]
        self.assertEqual((Decimal("0"), Decimal("0")), (event.price, event.trade_fee.flat_fees[0].amount))

    async def test_a_short_is_bought_back_at_settlement(self):
        self._hold(CALL, "-0.001")
        self._expire()
        self.connector._api_get.return_value = fills_response(settlement_fill(side="buy"))

        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()

        self.assertEqual((TradeType.BUY, Decimal("0.001")),
                         (self.settlements.event_log[0].trade_type, self.settlements.event_log[0].amount))

    async def test_reads_start_a_few_seconds_after_the_expiry(self):
        self._expire()

        with self._at(EXPIRY + 1):
            await self.connector._look_for_settlements()

        self.connector._api_get.assert_not_awaited()

    async def test_options_are_read_ten_at_a_time(self):
        pairs = [f"C_BTC_{100000 + 100 * n}_251226-USD" for n in range(12)]
        connector = self._connector(pairs, [option_product(f"C-BTC-{100000 + 100 * n}-251226", id=100 + n)
                                            for n in range(12)])
        with patch.object(connector, "_time", return_value=EXPIRY):
            connector._check_expiries()

        with patch.object(connector, "_time", return_value=EXPIRY + 10):
            await connector._look_for_settlements()

        self.assertEqual([",".join(str(100 + n) for n in range(10)), "110,111"],
                         [c.kwargs["params"]["product_ids"] for c in connector._api_get.await_args_list])

    async def test_an_option_known_only_by_its_name_is_looked_up_by_symbol(self):
        expired = "C_BTC_70000_011126-USD"
        connector = self._connector([expired], PRODUCTS)  # not listed any more: known from its name only
        settlements = EventLogger()
        connector.add_listener(MarketEvent.OptionSettled, settlements)
        product = option_product("C-BTC-70000-011126", id=77, state="expired", settlement_price="0")
        connector._api_get.side_effect = [
            {"success": True, "result": product},
            fills_response(settlement_fill(product_id=77, symbol="C-BTC-70000-011126", price="0", commission="0")),
        ]
        expiry = connector.get_instrument(expired).expiry_timestamp

        with patch.object(connector, "_time", return_value=expiry + 86400):  # the bot started a day later
            connector._check_expiries()
            await connector._look_for_settlements()

        self.assertEqual(f"{CONSTANTS.PRODUCTS_PATH_URL}/C-BTC-70000-011126",
                         connector._api_get.await_args_list[0].kwargs["path_url"])
        self.assertEqual((77, Decimal("0.001")), (connector._product_ids[expired], connector.contract_size(expired)))
        self.assertEqual([expired], [event.trading_pair for event in settlements.event_log])

    def test_a_settlement_fill_is_raised_once(self):
        self.connector._record_settlement(CALL, settlement_fill())
        self.connector._record_settlement(CALL, settlement_fill())

        self.assertEqual(1, len(self.settlements.event_log))

    # ── How long it is looked for ─────────────────────────────────────────────

    async def test_one_read_is_enough_where_nothing_was_held(self):
        self._hold(CALL, "0.001")
        self._expire()

        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()
            self.connector._drop_finished_settlement_watches()

        self.assertEqual([CALL], list(self.connector._settlements_due))

    async def test_a_held_position_without_settlement_is_reported_after_the_wait(self):
        self._hold(CALL, "0.001")
        self._expire()
        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()

        with self._at(EXPIRY + CONSTANTS.SETTLEMENT_WAIT), patch.object(DeltaOptionsDerivative, "logger") as logger:
            self.connector._drop_finished_settlement_watches()

        self.assertEqual({}, self.connector._settlements_due)
        self.assertIn(f"Delta lists no settlement for {CALL} 15 minutes after it expired",
                      logger.return_value.warning.call_args[0][0])

    async def test_an_option_delta_does_not_return_is_reported_after_the_wait(self):
        expired = "C_BTC_70000_011126-USD"
        connector = self._connector([expired], PRODUCTS)
        connector._api_get.return_value = {"success": True, "result": None}
        expiry = connector.get_instrument(expired).expiry_timestamp
        with patch.object(connector, "_time", return_value=expiry + 10):
            connector._check_expiries()
            await connector._look_for_settlements()
            connector._drop_finished_settlement_watches()
        self.assertIn(expired, connector._settlements_due)

        with patch.object(connector, "_time", return_value=expiry + 10 + CONSTANTS.SETTLEMENT_WAIT), \
                patch.object(DeltaOptionsDerivative, "logger") as logger:
            connector._drop_finished_settlement_watches()

        self.assertEqual({}, connector._settlements_due)
        self.assertIn(f"Could not read Delta's records for {expired}", logger.return_value.warning.call_args[0][0])

    async def test_a_failed_read_is_tried_again(self):
        self._expire()
        self.connector._api_get.side_effect = IOError("proxy down")
        with self._at(EXPIRY + 10):
            await self.connector._look_for_settlements()
            self.connector._drop_finished_settlement_watches()
        # Nothing was read, so neither option is given up yet.
        self.assertEqual({CALL, PUT}, set(self.connector._settlements_due))

        self.connector._api_get.side_effect = None
        self.connector._api_get.return_value = fills_response(settlement_fill())
        with self._at(EXPIRY + 20):
            await self.connector._look_for_settlements()
            self.connector._drop_finished_settlement_watches()

        self.assertEqual({}, self.connector._settlements_due)
        self.assertEqual([CALL], [event.trading_pair for event in self.settlements.event_log])

    async def test_the_watch_runs_until_the_settlement_is_found(self):
        self._hold(CALL, "0.001")
        self._expire()
        # The first read comes before Delta has written the fill.
        self.connector._api_get.side_effect = [fills_response(), fills_response(settlement_fill())]
        self.connector._sleep = AsyncMock()

        with self._at(EXPIRY + 10):
            await asyncio.wait_for(self.connector._settlement_watch_loop(), timeout=1)

        self.assertEqual([CALL], [event.trading_pair for event in self.settlements.event_log])
        self.assertEqual({}, self.connector._settlements_due)
        self.connector._sleep.assert_awaited_once_with(CONSTANTS.SETTLEMENT_POLL_INTERVAL)

    async def test_the_watch_runs_once_and_resumes_after_a_network_stop(self):
        connector = DeltaOptionsDerivative("key", "secret", trading_pairs=[CALL], trading_required=False)
        connector._settlement_watch_loop = MagicMock(side_effect=lambda: asyncio.sleep(3600))
        connector._settlements_due[CALL] = _SettlementDue(expiry=EXPIRY, held=True, until=EXPIRY + 900)

        connector._start_settlement_watch()
        first = connector._settlement_task
        await asyncio.sleep(0)
        connector._start_settlement_watch()
        self.assertIs(first, connector._settlement_task)

        await connector.stop_network()
        await asyncio.sleep(0)
        self.assertTrue(first.cancelled())
        self.assertIn(CALL, connector._settlements_due)

        connector._start_settlement_watch()
        resumed = connector._settlement_task
        self.assertIsNot(first, resumed)
        await asyncio.sleep(0)
        resumed.cancel()
        await asyncio.sleep(0)
