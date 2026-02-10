import inspect

from sqlalchemy import JSON, Column, Index, PrimaryKeyConstraint, Text, BigInteger, Float, Numeric

from hummingbot.model import HummingbotBase


class MarketData(HummingbotBase):
    __tablename__ = "MarketData"
    __table_args__ = (
        PrimaryKeyConstraint("timestamp", "exchange", "trading_pair"),
        Index("idx_market_data_timestamp", "timestamp"),
        Index("idx_market_data_trading_pair", "trading_pair"),
        Index("idx_market_data_exchange", "exchange"),
    )

    timestamp = Column(BigInteger, nullable=False)
    exchange = Column(Text, nullable=False)
    trading_pair = Column(Text, nullable=False)
    mid_price = Column(Float, nullable=False)
    best_bid = Column(Float, nullable=False)
    best_ask = Column(Float, nullable=False)
    spread = Column(Numeric(5, 2), nullable=True)
    order_book = Column(JSON)

    def __repr__(self) -> str:
        list_of_fields = [f"{name}: {value}" for name, value in inspect.getmembers(self) if isinstance(value, Column)]
        return ','.join(list_of_fields)
