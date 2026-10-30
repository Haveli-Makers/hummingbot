from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional


class OptionType(Enum):
    CALL = "call"
    PUT = "put"


@dataclass(frozen=True)
class OptionInstrumentInfo:
    """
    Static details of one option contract.
    """
    trading_pair: str
    underlying: str
    option_type: OptionType
    strike: Decimal
    expiry_timestamp: float

    def is_expired(self, timestamp: float) -> bool:
        return timestamp >= self.expiry_timestamp


@dataclass
class OptionInfo:
    """
    Live market data of one option contract, as sent by the exchange. Greeks are per one unit of the underlying.
    """
    trading_pair: str
    mark_price: Decimal
    underlying_price: Decimal
    mark_iv: Optional[Decimal] = None
    bid_iv: Optional[Decimal] = None
    ask_iv: Optional[Decimal] = None
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    rho: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    price_band_lower: Optional[Decimal] = None
    price_band_upper: Optional[Decimal] = None
    timestamp: float = 0

    def update(self, info_update: "OptionInfoUpdate"):
        for key, value in asdict(info_update).items():
            if key != "trading_pair" and value is not None:
                setattr(self, key, value)


@dataclass
class OptionInfoUpdate:
    trading_pair: str
    mark_price: Optional[Decimal] = None
    underlying_price: Optional[Decimal] = None
    mark_iv: Optional[Decimal] = None
    bid_iv: Optional[Decimal] = None
    ask_iv: Optional[Decimal] = None
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    rho: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    price_band_lower: Optional[Decimal] = None
    price_band_upper: Optional[Decimal] = None
    timestamp: Optional[float] = None
