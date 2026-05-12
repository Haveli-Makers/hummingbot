"""
Tests for order-edit functionality in ExchangePyBase.

Groups:
  TestOrderUpdatePriceAmountFields   – OrderUpdate NamedTuple carries new_price/new_amount;
                                       update_with_order_update() applies them.
  TestPendingEditContextNestedClass  – PendingEditContext is a nested class of ExchangePyBase.
  TestIsOrderEditable                – _is_order_editable() classifies every OrderState correctly.
  TestExecuteNativeEdit              – _execute_native_edit() happy path, failure rollback, events.
  TestExecuteCancelReplace           – _execute_edit_via_cancel_replace() happy path.
  TestEditOrderRouting               – edit_order() / _execute_edit() routing and edge cases.
"""

import asyncio
import unittest
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent, OrderEditedEvent, OrderEditFailedEvent
from hummingbot.exceptions import OrderEditError


class _MockExchange(ExchangePyBase):
    """Implements every abstract method with the minimum needed for tests."""

    def __init__(self):
        with (
            patch("hummingbot.core.data_type.order_book_tracker.OrderBookTracker.__init__",
                  return_value=None),
            patch("hummingbot.core.data_type.order_book_tracker.OrderBookTracker.start",
                  return_value=None),
            patch("hummingbot.core.data_type.user_stream_tracker.UserStreamTracker.__init__",
                  return_value=None),
        ):
            super().__init__()

        mock_obt = MagicMock()
        mock_obt.order_books = {}
        mock_obt.ready = True
        self._set_order_book_tracker(mock_obt)
        self._user_stream_tracker = MagicMock()

        self._current_timestamp = 1_700_000_000.0

        self._place_edit_calls: list = []
        self._place_cancel_result: bool = True
        self._place_edit_result: Tuple[str, str, float] = ("same_id", "EX_NEW", 1_700_000_001.0)
        self._place_edit_raises: Optional[Exception] = None

        self.edit_logger = EventLogger()
        self.edit_failed_logger = EventLogger()

        rule = TradingRule(
            trading_pair="BTC-USDT",
            min_order_size=Decimal("0.001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.001"),
            min_notional_size=Decimal("10"),
        )
        self._trading_rules = {"BTC-USDT": rule}

    @property
    def name(self) -> str:
        return "mock_exchange"

    @property
    def authenticator(self):
        return MagicMock()

    @property
    def rate_limits_rules(self) -> list:
        return []

    @property
    def domain(self) -> str:
        return "com"

    @property
    def client_order_id_max_length(self) -> int:
        return 30

    @property
    def client_order_id_prefix(self) -> str:
        return "test"

    @property
    def trading_rules_request_path(self) -> str:
        return "/rules"

    @property
    def trading_pairs_request_path(self) -> str:
        return "/pairs"

    @property
    def check_network_request_path(self) -> str:
        return "/ping"

    @property
    def trading_pairs(self) -> List[str]:
        return ["BTC-USDT"]

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return False

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        return self._place_cancel_result

    async def _place_order(self, order_id, trading_pair, amount, trade_type, order_type, price, **kwargs):
        return "EX_OID_NEW", self.current_timestamp

    def _get_fee(self, base_currency, quote_currency, order_type, order_side, amount, price=None, is_maker=None):
        return AddedToCostTradeFee(percent=Decimal("0.001"))

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        pass

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.OPEN,
            client_order_id=tracked_order.client_order_id,
        )

    async def _format_trading_rules(self, exchange_info_dict) -> list:
        return []

    async def _update_balances(self):
        pass

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> list:
        return []

    def _create_web_assistants_factory(self):
        return MagicMock()

    def _create_order_book_data_source(self):
        return MagicMock()

    def _create_user_stream_data_source(self):
        return MagicMock()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info):
        pass

    @property
    def current_timestamp(self) -> float:
        return self._current_timestamp

    async def _place_edit(
        self,
        client_order_id: str,
        exchange_order_id: str,
        trading_pair: str,
        new_price: Decimal,
        new_amount: Decimal,
        **kwargs,
    ) -> Tuple[str, str, float]:
        self._place_edit_calls.append({
            "client_order_id": client_order_id,
            "exchange_order_id": exchange_order_id,
            "new_price": new_price,
            "new_amount": new_amount,
        })
        if self._place_edit_raises is not None:
            raise self._place_edit_raises
        return self._place_edit_result

    async def _sleep(self, delay: float):
        return

    def _add_event_listener(self, event_type: MarketEvent):
        """Register the appropriate EventLogger for a market event type."""
        if event_type == MarketEvent.OrderEdited:
            self.add_listener(event_type, self.edit_logger)
        elif event_type == MarketEvent.OrderEditFailed:
            self.add_listener(event_type, self.edit_failed_logger)

    def _make_tracked_order(
        self,
        *,
        client_order_id: str = "OID-1",
        exchange_order_id: str = "EX-1",
        price: Decimal = Decimal("100"),
        amount: Decimal = Decimal("1"),
        state: OrderState = OrderState.OPEN,
    ) -> InFlightOrder:
        order = InFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=price,
            amount=amount,
            creation_timestamp=self.current_timestamp,
        )
        order.current_state = state
        self._order_tracker.start_tracking_order(order)
        return order

class TestOrderUpdatePriceAmountFields(unittest.TestCase):

    def test_order_update_has_new_price_field(self):
        u = OrderUpdate(trading_pair="BTC-USDT", update_timestamp=0.0, new_state=OrderState.OPEN)
        self.assertIsNone(u.new_price)

    def test_order_update_has_new_amount_field(self):
        u = OrderUpdate(trading_pair="BTC-USDT", update_timestamp=0.0, new_state=OrderState.OPEN)
        self.assertIsNone(u.new_amount)

    def test_order_update_can_carry_new_price(self):
        u = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=0.0,
            new_state=OrderState.OPEN,
            new_price=Decimal("150"),
        )
        self.assertEqual(Decimal("150"), u.new_price)

    def test_order_update_can_carry_new_amount(self):
        u = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=0.0,
            new_state=OrderState.OPEN,
            new_amount=Decimal("2.5"),
        )
        self.assertEqual(Decimal("2.5"), u.new_amount)

    def test_update_with_order_update_applies_new_price(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="EX-1",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            creation_timestamp=0.0,
        )
        update = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=1.0,
            new_state=OrderState.OPEN,
            client_order_id="OID-1",
            new_price=Decimal("150"),
        )
        order.update_with_order_update(update)
        self.assertEqual(Decimal("150"), order.price)

    def test_update_with_order_update_applies_new_amount(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="EX-1",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            creation_timestamp=0.0,
        )
        update = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=1.0,
            new_state=OrderState.OPEN,
            client_order_id="OID-1",
            new_amount=Decimal("3"),
        )
        order.update_with_order_update(update)
        self.assertEqual(Decimal("3"), order.amount)

    def test_update_with_order_update_leaves_price_unchanged_when_none(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="EX-1",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            creation_timestamp=0.0,
        )
        update = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=1.0,
            new_state=OrderState.OPEN,
            client_order_id="OID-1",
        )
        order.update_with_order_update(update)
        self.assertEqual(Decimal("100"), order.price)

    def test_update_with_order_update_leaves_amount_unchanged_when_none(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="EX-1",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            creation_timestamp=0.0,
        )
        update = OrderUpdate(
            trading_pair="BTC-USDT",
            update_timestamp=1.0,
            new_state=OrderState.OPEN,
            client_order_id="OID-1",
        )
        order.update_with_order_update(update)
        self.assertEqual(Decimal("1"), order.amount)


class TestPendingEditContextNestedClass(unittest.TestCase):

    def test_pending_edit_context_accessible_as_nested_class(self):
        self.assertTrue(hasattr(ExchangePyBase, "PendingEditContext"))

    def test_pending_edit_context_not_at_module_level(self):
        import hummingbot.connector.exchange_py_base as mod
        self.assertFalse(hasattr(mod, "PendingEditContext"))

    def test_pending_edit_context_is_dataclass(self):
        import dataclasses
        self.assertTrue(dataclasses.is_dataclass(ExchangePyBase.PendingEditContext))

    def test_pending_edit_context_has_required_fields(self):
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(ExchangePyBase.PendingEditContext)}
        for required in ("original_order", "new_price", "new_amount", "cancel_initiated_at",
                         "cancel_confirmed", "max_balance_wait_seconds", "max_retries"):
            self.assertIn(required, field_names)

    def test_pending_edit_context_can_be_instantiated(self):
        order = MagicMock()
        ctx = ExchangePyBase.PendingEditContext(
            original_order=order,
            new_price=Decimal("101"),
            new_amount=Decimal("2"),
            cancel_initiated_at=1_700_000_000.0,
        )
        self.assertEqual(Decimal("101"), ctx.new_price)
        self.assertEqual(Decimal("2"), ctx.new_amount)
        self.assertFalse(ctx.cancel_confirmed)

class TestIsOrderEditable(unittest.IsolatedAsyncioTestCase):

    def _make_exchange(self) -> _MockExchange:
        return _MockExchange()

    def _order_in_state(self, exchange: _MockExchange, state: OrderState) -> InFlightOrder:
        o = InFlightOrder(
            client_order_id="OID",
            exchange_order_id="EX",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
            creation_timestamp=0.0,
        )
        o.current_state = state
        return o

    def test_open_is_editable(self):
        ex = self._make_exchange()
        self.assertTrue(ex._is_order_editable(self._order_in_state(ex, OrderState.OPEN)))

    def test_partially_filled_is_editable(self):
        ex = self._make_exchange()
        self.assertTrue(ex._is_order_editable(self._order_in_state(ex, OrderState.PARTIALLY_FILLED)))

    def test_pending_create_is_editable(self):
        ex = self._make_exchange()
        self.assertTrue(ex._is_order_editable(self._order_in_state(ex, OrderState.PENDING_CREATE)))

    def test_filled_is_not_editable(self):
        ex = self._make_exchange()
        self.assertFalse(ex._is_order_editable(self._order_in_state(ex, OrderState.FILLED)))

    def test_canceled_is_not_editable(self):
        ex = self._make_exchange()
        self.assertFalse(ex._is_order_editable(self._order_in_state(ex, OrderState.CANCELED)))

    def test_pending_cancel_is_not_editable(self):
        ex = self._make_exchange()
        self.assertFalse(ex._is_order_editable(self._order_in_state(ex, OrderState.PENDING_CANCEL)))

    def test_failed_is_not_editable(self):
        ex = self._make_exchange()
        self.assertFalse(ex._is_order_editable(self._order_in_state(ex, OrderState.FAILED)))

    def test_pending_edit_is_not_editable(self):
        ex = self._make_exchange()
        self.assertFalse(ex._is_order_editable(self._order_in_state(ex, OrderState.PENDING_EDIT)))


class TestExecuteNativeEdit(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _MockExchange()
        self.exchange._add_event_listener(MarketEvent.OrderEdited)
        self.exchange._add_event_listener(MarketEvent.OrderEditFailed)

    async def test_native_edit_happy_path_updates_price_and_amount(self):
        order = self.exchange._make_tracked_order(price=Decimal("100"), amount=Decimal("1"))
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)

        await self.exchange._execute_native_edit(
            tracked_order=order,
            new_price=Decimal("110"),
            new_amount=Decimal("2"),
        )
        await asyncio.sleep(0)  # let process_order_update fire-and-forget tasks apply

        self.assertEqual(Decimal("110"), order.price)
        self.assertEqual(Decimal("2"), order.amount)

    async def test_native_edit_happy_path_emits_order_edited_event(self):
        order = self.exchange._make_tracked_order()
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)

        await self.exchange._execute_native_edit(
            tracked_order=order,
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )

        self.assertEqual(1, len(self.exchange.edit_logger.event_log))
        evt: OrderEditedEvent = self.exchange.edit_logger.event_log[0]
        self.assertEqual("OID-1", evt.order_id)
        self.assertEqual(Decimal("110"), evt.new_price)

    async def test_native_edit_passes_correct_args_to_place_edit(self):
        order = self.exchange._make_tracked_order(
            client_order_id="OID-1",
            exchange_order_id="EX-1",
            price=Decimal("100"),
            amount=Decimal("1"),
        )
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)

        await self.exchange._execute_native_edit(
            tracked_order=order,
            new_price=Decimal("99"),
            new_amount=Decimal("0.5"),
        )

        self.assertEqual(1, len(self.exchange._place_edit_calls))
        call = self.exchange._place_edit_calls[0]
        self.assertEqual("OID-1", call["client_order_id"])
        self.assertEqual("EX-1", call["exchange_order_id"])
        self.assertEqual(Decimal("99"), call["new_price"])
        self.assertEqual(Decimal("0.5"), call["new_amount"])

    async def test_native_edit_failure_rolls_back_state_to_open(self):
        order = self.exchange._make_tracked_order()
        self.exchange._place_edit_raises = RuntimeError("exchange error")

        with self.assertRaises(OrderEditError):
            await self.exchange._execute_native_edit(
                tracked_order=order,
                new_price=Decimal("110"),
                new_amount=Decimal("1"),
            )

        await asyncio.sleep(0)
        self.assertEqual(OrderState.OPEN, order.current_state)

    async def test_native_edit_sets_pending_edit_state_before_calling_place_edit(self):
        """Verify that a PENDING_EDIT OrderUpdate is queued before _place_edit() is called."""
        updates_before_place_edit: list = []
        all_updates: list = []
        original_process_update = self.exchange._order_tracker.process_order_update

        def capturing_process_update(update):
            all_updates.append(update)
            return original_process_update(update)

        original_place_edit = self.exchange._place_edit

        async def _capturing_place_edit(*args, **kwargs):
            updates_before_place_edit.extend(all_updates[:])
            return await original_place_edit(*args, **kwargs)

        order = self.exchange._make_tracked_order()
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)
        self.exchange._order_tracker.process_order_update = capturing_process_update
        self.exchange._place_edit = _capturing_place_edit

        await self.exchange._execute_native_edit(
            tracked_order=order,
            new_price=Decimal("105"),
            new_amount=Decimal("1"),
        )

        pending_states = [u.new_state for u in updates_before_place_edit]
        self.assertIn(OrderState.PENDING_EDIT, pending_states)

    async def test_native_edit_restores_open_after_failure_not_edit_failed_event(self):
        """Failure inside _execute_native_edit raises OrderEditError — the caller handles the event."""
        order = self.exchange._make_tracked_order()
        self.exchange._place_edit_raises = RuntimeError("boom")

        with self.assertRaises(OrderEditError):
            await self.exchange._execute_native_edit(
                tracked_order=order,
                new_price=Decimal("110"),
                new_amount=Decimal("1"),
            )

        # No OrderEditFailed event here — that is emitted by _execute_edit(), not _execute_native_edit()
        self.assertEqual(0, len(self.exchange.edit_failed_logger.event_log))


# ---------------------------------------------------------------------------
# 5. _execute_edit_via_cancel_replace
# ---------------------------------------------------------------------------

class TestExecuteCancelReplace(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _MockExchange()
        self.exchange._add_event_listener(MarketEvent.OrderEdited)
        self.exchange._add_event_listener(MarketEvent.OrderEditFailed)
        self.exchange._place_cancel_result = True

    async def test_cancel_replace_happy_path_emits_order_edited_event(self):
        order = self.exchange._make_tracked_order(price=Decimal("100"), amount=Decimal("1"))
        self.exchange._wait_for_balance_update = AsyncMock(return_value=None)
        self.exchange._place_replacement_order = AsyncMock(return_value="OID-REPLACE")

        await self.exchange._execute_edit_via_cancel_replace(
            tracked_order=order,
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )

        self.assertEqual(1, len(self.exchange.edit_logger.event_log))
        evt: OrderEditedEvent = self.exchange.edit_logger.event_log[0]
        self.assertEqual(Decimal("110"), evt.new_price)

    async def test_cancel_replace_sets_pending_edit_before_cancel(self):
        """A PENDING_EDIT OrderUpdate should be queued before the cancel is attempted."""
        updates_before_cancel: list = []
        all_updates: list = []
        original_process_update = self.exchange._order_tracker.process_order_update

        def capturing_process_update(update):
            all_updates.append(update)
            return original_process_update(update)

        original_cancel = self.exchange._cancel_and_verify_for_edit

        async def _capturing_cancel(context, **kwargs):
            updates_before_cancel.extend(all_updates[:])
            return await original_cancel(context, **kwargs)

        order = self.exchange._make_tracked_order()
        self.exchange._order_tracker.process_order_update = capturing_process_update
        self.exchange._cancel_and_verify_for_edit = _capturing_cancel
        self.exchange._wait_for_balance_update = AsyncMock(return_value=None)
        self.exchange._place_replacement_order = AsyncMock(return_value="OID-REPLACE")

        await self.exchange._execute_edit_via_cancel_replace(
            tracked_order=order,
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )

        pending_states = [u.new_state for u in updates_before_cancel]
        self.assertIn(OrderState.PENDING_EDIT, pending_states)

    async def test_cancel_replace_rollback_when_cancel_fails(self):
        from hummingbot.exceptions import OrderEditCancelFailed

        async def _failing_cancel(context, **kwargs):
            raise OrderEditCancelFailed("cancel rejected")

        order = self.exchange._make_tracked_order()
        self.exchange._cancel_and_verify_for_edit = _failing_cancel

        with self.assertRaises(OrderEditCancelFailed):
            await self.exchange._execute_edit_via_cancel_replace(
                tracked_order=order,
                new_price=Decimal("110"),
                new_amount=Decimal("1"),
            )

        await asyncio.sleep(0)
        self.assertEqual(OrderState.OPEN, order.current_state)

class TestEditOrderRouting(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _MockExchange()
        self.exchange._add_event_listener(MarketEvent.OrderEdited)
        self.exchange._add_event_listener(MarketEvent.OrderEditFailed)

    async def test_edit_order_routes_to_native_when_supported(self):
        """When is_edit_order_supported_by_exchange=True, _execute_native_edit is called."""
        order = self.exchange._make_tracked_order()
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)

        with patch.object(type(self.exchange), "is_edit_order_supported_by_exchange",
                          new_callable=lambda: property(lambda self: True)):
            native_mock = AsyncMock(return_value="OID-1")
            self.exchange._execute_native_edit = native_mock

            await self.exchange._execute_edit(
                client_order_id="OID-1",
                trading_pair="BTC-USDT",
                new_price=Decimal("110"),
                new_amount=Decimal("1"),
            )

        native_mock.assert_awaited_once()

    async def test_edit_order_routes_to_cancel_replace_by_default(self):
        """When is_edit_order_supported_by_exchange=False, _execute_edit_via_cancel_replace is called."""
        order = self.exchange._make_tracked_order()

        cancel_replace_mock = AsyncMock(return_value="OID-REPLACE")
        self.exchange._execute_edit_via_cancel_replace = cancel_replace_mock

        await self.exchange._execute_edit(
            client_order_id="OID-1",
            trading_pair="BTC-USDT",
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )

        cancel_replace_mock.assert_awaited_once()

    async def test_edit_order_returns_client_id_synchronously(self):
        """edit_order() must return the client_order_id synchronously."""
        order = self.exchange._make_tracked_order(client_order_id="OID-SYNC")
        result = self.exchange.edit_order(
            client_order_id="OID-SYNC",
            trading_pair="BTC-USDT",
            new_price=Decimal("110"),
        )
        self.assertEqual("OID-SYNC", result)

    async def test_edit_order_unknown_order_returns_none(self):
        result = await self.exchange._execute_edit(
            client_order_id="DOES-NOT-EXIST",
            trading_pair="BTC-USDT",
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )
        self.assertIsNone(result)

    async def test_edit_order_non_editable_state_emits_failed_event(self):
        order = self.exchange._make_tracked_order(state=OrderState.FILLED)

        result = await self.exchange._execute_edit(
            client_order_id="OID-1",
            trading_pair="BTC-USDT",
            new_price=Decimal("110"),
            new_amount=Decimal("1"),
        )

        self.assertIsNone(result)
        self.assertEqual(1, len(self.exchange.edit_failed_logger.event_log))
        self.assertTrue(self.exchange.edit_failed_logger.event_log[0].recoverable)

    async def test_edit_order_uses_original_price_when_new_price_is_none(self):
        """If new_price=None, the existing order price is used."""
        order = self.exchange._make_tracked_order(price=Decimal("99"), amount=Decimal("1"))
        self.exchange._place_edit_result = ("OID-1", "EX-NEW", 1_700_000_001.0)

        with patch.object(type(self.exchange), "is_edit_order_supported_by_exchange",
                          new_callable=lambda: property(lambda self: True)):
            native_mock = AsyncMock(return_value="OID-1")
            self.exchange._execute_native_edit = native_mock

            await self.exchange._execute_edit(
                client_order_id="OID-1",
                trading_pair="BTC-USDT",
                new_price=None,
                new_amount=Decimal("1"),
            )

        _, call_kwargs = native_mock.call_args
        self.assertEqual(Decimal("99"), call_kwargs["new_price"])

    async def test_edit_order_exception_emits_failed_event(self):
        """An unexpected exception inside _execute_native_edit propagates as OrderEditFailed event."""
        order = self.exchange._make_tracked_order()

        with patch.object(type(self.exchange), "is_edit_order_supported_by_exchange",
                          new_callable=lambda: property(lambda self: True)):
            self.exchange._execute_native_edit = AsyncMock(side_effect=RuntimeError("unexpected"))

            result = await self.exchange._execute_edit(
                client_order_id="OID-1",
                trading_pair="BTC-USDT",
                new_price=Decimal("110"),
                new_amount=Decimal("1"),
            )

        self.assertIsNone(result)
        self.assertEqual(1, len(self.exchange.edit_failed_logger.event_log))
