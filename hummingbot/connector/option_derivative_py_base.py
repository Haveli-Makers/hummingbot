import asyncio
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.options_trading import OptionsTrading
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import PerpetualDerivativeInFlightOrder
from hummingbot.core.data_type.option_api_order_book_data_source import OptionAPIOrderBookDataSource
from hummingbot.core.data_type.option_info import OptionInfo, OptionInstrumentInfo
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather


class OptionDerivativePyBase(ExchangePyBase, ABC):
    """
    Base class for options connectors. Options are listed and expire continuously, so on top of the usual exchange
    state this keeps an index of the instruments, their live option info (mark price, IV, Greeks) and net positions.
    """
    VALID_POSITION_ACTIONS = [PositionAction.OPEN, PositionAction.CLOSE]

    def __init__(self,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100")):
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._options_trading = OptionsTrading(self.trading_pairs)
        self._option_info_listener_task: Optional[asyncio.Task] = None
        self._orderbook_ds: OptionAPIOrderBookDataSource = self._orderbook_ds  # for type-hinting

    @property
    def status_dict(self) -> Dict[str, bool]:
        status_d = super().status_dict
        status_d["option_info"] = self._options_trading.is_option_info_initialized()
        return status_d

    @property
    def position_mode(self) -> PositionMode:
        return PositionMode.ONEWAY

    @property
    def account_positions(self) -> Dict[str, Position]:
        return self._options_trading.account_positions

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def set_position_mode(self, mode: PositionMode):
        if mode != PositionMode.ONEWAY:
            self.logger().error(f"Position mode {mode} is not supported. Options positions are always net (ONEWAY).")

    def get_leverage(self, trading_pair: str) -> int:
        return 1

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        if leverage != 1:
            self.logger().warning("Leverage does not apply to options; the setting is ignored.")

    def get_position(self, trading_pair: str) -> Optional[Position]:
        return self._options_trading.get_position(trading_pair)

    def get_option_info(self, trading_pair: str) -> OptionInfo:
        return self._options_trading.get_option_info(trading_pair)

    def get_instrument(self, trading_pair: str) -> Optional[OptionInstrumentInfo]:
        return self._options_trading.get_instrument(trading_pair)

    def get_underlying_price(self, trading_pair: str) -> Decimal:
        return self.get_option_info(trading_pair).underlying_price

    def get_option_chain(self, underlying: str, expiry_timestamp: Optional[float] = None) -> List[OptionInstrumentInfo]:
        """
        Returns the live (not expired) instruments of an underlying, optionally for one expiry only,
        sorted by expiry, option type and strike.
        """
        now = self._time()
        chain = [
            instrument for instrument in self._options_trading.instruments.values()
            if instrument.underlying == underlying
            and not instrument.is_expired(now)
            and (expiry_timestamp is None or instrument.expiry_timestamp == expiry_timestamp)
        ]
        return sorted(chain, key=lambda i: (i.expiry_timestamp, i.option_type.value, i.strike))

    def is_expired(self, trading_pair: str) -> bool:
        instrument = self.get_instrument(trading_pair)
        return instrument is not None and instrument.is_expired(self._time())

    async def exchange_symbols_for_live_pairs(self, trading_pairs: List[str]) -> List[str]:
        """
        Returns the exchange symbols of the pairs that are still listed and not expired. Expired or delisted pairs are
        skipped, so one expired instrument never breaks the subscriptions of the others.
        """
        symbols = []
        for trading_pair in trading_pairs:
            if self.is_expired(trading_pair):
                continue
            try:
                symbols.append(await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair))
            except KeyError:
                self.logger().warning(f"{trading_pair} is no longer listed on {self.name}; skipping it.")
        return symbols

    @abstractmethod
    def get_buy_collateral_token(self, trading_pair: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_sell_collateral_token(self, trading_pair: str) -> str:
        raise NotImplementedError

    def start_tracking_order(
        self,
        order_id: str,
        exchange_order_id: Optional[str],
        trading_pair: str,
        trade_type: TradeType,
        price: Decimal,
        amount: Decimal,
        order_type: OrderType,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ):
        self._order_tracker.start_tracking_order(
            PerpetualDerivativeInFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                amount=amount,
                price=price,
                creation_timestamp=self.current_timestamp,
                leverage=1,
                position=position_action,
            )
        )

    @abstractmethod
    def _create_order_book_data_source(self) -> OptionAPIOrderBookDataSource:
        raise NotImplementedError

    @abstractmethod
    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        raise NotImplementedError

    @abstractmethod
    async def _update_positions(self):
        raise NotImplementedError

    async def start_network(self):
        await super().start_network()
        self._options_trading.start()
        self._option_info_listener_task = safe_ensure_future(self._listen_for_option_info())

    async def stop_network(self):
        self._options_trading.stop()
        if self._option_info_listener_task is not None:
            self._option_info_listener_task.cancel()
            self._option_info_listener_task = None
        await super().stop_network()

    async def _create_order(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ):
        if position_action not in self.VALID_POSITION_ACTIONS:
            raise ValueError(
                f"Invalid position action {position_action}. Must be one of {self.VALID_POSITION_ACTIONS}"
            )
        await super()._create_order(
            trade_type,
            order_id,
            trading_pair,
            amount,
            order_type,
            price,
            position_action=position_action,
            **kwargs,
        )

    def get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        return self._get_fee(
            base_currency, quote_currency, order_type, order_side, position_action, amount, price, is_maker
        )

    @abstractmethod
    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        raise NotImplementedError

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_positions(),
            self._update_balances(),
            self._update_order_status(),
        )

    async def _listen_for_option_info(self):
        await self._init_option_info()
        await self._orderbook_ds.listen_for_option_info(output=self._options_trading.option_info_stream)

    async def _init_option_info(self):
        for trading_pair in self.trading_pairs:
            while True:
                try:
                    option_info = await self._orderbook_ds.get_option_info(trading_pair)
                    self._options_trading.initialize_option_info(option_info)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().network(
                        f"Error fetching option info for {trading_pair}. Retrying in 5 seconds.",
                        exc_info=True,
                    )
                    await self._sleep(5.0)
