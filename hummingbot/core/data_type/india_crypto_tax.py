"""
India Crypto Tax Calculator

Implements TDS (Tax Deducted at Source) and Income Tax calculations per Indian tax law:

Section 194S — 1% TDS on every crypto transfer (applicable from 1 July 2022):
  • INR markets (e.g., BTC-INR):
      - SELLER pays 1% TDS on the INR proceeds they receive.
      - BUYER pays NO TDS (exempt when paying with INR).
  • Crypto-Crypto markets (e.g., BTC-USDT, ETH-BTC):
      - BOTH buyer and seller pay 1% TDS on the fill value in the QUOTE asset.
      - Seller receives 1% less of the quote asset.
      - Buyer pays 1% more of the quote asset (extra cost on top of the trade).

Section 115BBH — 30% flat tax on crypto profits, no loss offset allowed.
TDS is advance tax, fully credited against the 30% liability at ITR filing.

References:
  WazirX: https://support.wazirx.com/hc/en-us/articles/4701662097050-TDS-on-Crypto-Trades
  CoinDCX: https://coindcx.com/calculators/crypto-tax-calculator/
"""
import logging
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from hummingbot.core.data_type.common import TradeType

S_DECIMAL_0 = Decimal("0")

# Default rates per Indian tax law
DEFAULT_TDS_RATE = Decimal("0.01")          # 1% TDS (Section 194S)
DEFAULT_PROFIT_TAX_RATE = Decimal("0.30")   # 30% flat tax (Section 115BBH)


class MarketType(Enum):
    INR = "inr"
    CRYPTO_CRYPTO = "crypto_crypto"


@dataclass
class IndiaCryptoTaxConfig:
    """Configuration for India crypto tax rates."""
    tds_rate: Decimal = DEFAULT_TDS_RATE
    profit_tax_rate: Decimal = DEFAULT_PROFIT_TAX_RATE


@dataclass
class TDSResult:
    """TDS deduction details for a single fill (buy or sell side)."""
    fill_value_quote: Decimal
    tds_rate: Decimal
    tds_amount_quote: Decimal
    is_applicable: bool


@dataclass
class ProfitTaxResult:
    """Profit tax calculation result for a buy-sell pair."""
    taxable_profit: Decimal
    tax_rate: Decimal
    tax_liability: Decimal
    tds_already_paid: Decimal
    additional_tax_due: Decimal


def calculate_tds(
    fill_value_quote: Decimal,
    is_buyer: bool,
    market_type: MarketType = MarketType.CRYPTO_CRYPTO,
    config: IndiaCryptoTaxConfig = None,
) -> TDSResult:
    if config is None:
        config = IndiaCryptoTaxConfig()

    if market_type == MarketType.INR and is_buyer:
        return TDSResult(
            fill_value_quote=fill_value_quote,
            tds_rate=config.tds_rate,
            tds_amount_quote=S_DECIMAL_0,
            is_applicable=False,
        )

    tds_amount = fill_value_quote * config.tds_rate
    return TDSResult(
        fill_value_quote=fill_value_quote,
        tds_rate=config.tds_rate,
        tds_amount_quote=tds_amount,
        is_applicable=True,
    )


def calculate_profit_tax(taxable_profit: Decimal,
                         tds_already_paid: Decimal = S_DECIMAL_0,
                         config: IndiaCryptoTaxConfig = None) -> ProfitTaxResult:
    if config is None:
        config = IndiaCryptoTaxConfig()

    if taxable_profit > S_DECIMAL_0:
        tax_liability = taxable_profit * config.profit_tax_rate
    else:
        tax_liability = S_DECIMAL_0

    additional_tax_due = tax_liability - tds_already_paid

    return ProfitTaxResult(
        taxable_profit=taxable_profit,
        tax_rate=config.profit_tax_rate,
        tax_liability=tax_liability,
        tds_already_paid=tds_already_paid,
        additional_tax_due=additional_tax_due,
    )


def get_market_type(trading_pair: str) -> MarketType:
    """Return MarketType based on the quote currency of *trading_pair* (e.g. 'BTC-INR')."""
    quote = trading_pair.split("-")[-1].upper()
    return MarketType.INR if quote == "INR" else MarketType.CRYPTO_CRYPTO


class IndiaCryptoTaxTracker:
    """
    Encapsulates all India crypto tax tracking state and logic for a single exchange connector.

    Use via composition: each connector holds ``self._tax = IndiaCryptoTaxTracker()``.
    """

    def __init__(self) -> None:
        self.tax_config: IndiaCryptoTaxConfig = IndiaCryptoTaxConfig()
        self._pending_buy_fills: Dict[str, deque] = {}
        self._tds_by_trade_id: Dict[str, Decimal] = {}

    def record_tds(self, trade_id: str, amount: Decimal) -> None:
        """Store the pre-computed TDS amount for *trade_id*."""
        self._tds_by_trade_id[trade_id] = amount

    def get_tds(self, trade_id: str) -> Decimal:
        """Retrieve the stored TDS for *trade_id*, defaulting to 0."""
        return self._tds_by_trade_id.get(trade_id, Decimal("0"))

    def calc_and_record_tds(
        self,
        trade_id: str,
        fill_value_quote: Decimal,
        is_buyer: bool,
        trading_pair: str,
    ) -> Decimal:
        """Calculate TDS for a fill, store it by trade_id, and return the amount."""
        tds_amount = calculate_tds(
            fill_value_quote=fill_value_quote,
            is_buyer=is_buyer,
            market_type=get_market_type(trading_pair),
            config=self.tax_config,
        ).tds_amount_quote
        self.record_tds(trade_id, tds_amount)
        return tds_amount

    def track_and_log(
        self,
        trade_type: "TradeType",
        trading_pair: str,
        fill_base: Decimal,
        fill_value: Decimal,
        fee_amount: Decimal,
        tds_amount: Decimal,
        quote: str,
        logger: logging.Logger,
    ) -> None:
        """Record a fill in the FIFO inventory and emit a tax log entry."""
        from hummingbot.core.data_type.common import TradeType
        from hummingbot.core.data_type.order_profit import calculate_order_profit, format_profit_report

        if trade_type == TradeType.BUY:
            if trading_pair not in self._pending_buy_fills:
                self._pending_buy_fills[trading_pair] = deque()
            self._pending_buy_fills[trading_pair].append(
                (fill_base, fill_value, fee_amount, tds_amount)
            )
            total_cost = fill_value + fee_amount + tds_amount
            mtype = get_market_type(trading_pair)
            tds_note = (
                "(1% on crypto-crypto buy)"
                if mtype == MarketType.CRYPTO_CRYPTO
                else "(not applicable on INR-market buy)"
            )
            logger.info(
                f"Tax Report ({trading_pair}) - Buy Fill:\n"
                f"  Buy Value:  {fill_value:.2f} {quote}\n"
                f"  Trade Fee:  {fee_amount:.2f} {quote}\n"
                f"  TDS {tds_note}: {tds_amount:.2f} {quote}\n"
                f"  Total Cost: {total_cost:.2f} {quote}"
            )
            return

        pending_buys = self._pending_buy_fills.get(trading_pair, deque())
        if not pending_buys:
            logger.info(
                f"Tax Report ({trading_pair}) - Sell Fill:\n"
                f"  Sell Value:  {fill_value:.2f} {quote}\n"
                f"  Sell Fee:    {fee_amount:.2f} {quote}\n"
                f"  TDS (1%):    {tds_amount:.2f} {quote}\n"
                f"  Net Received: {fill_value - fee_amount - tds_amount:.2f} {quote}\n"
                f"  (No matching buy orders tracked yet for profit calculation)"
            )
            return

        remaining_sell_base = fill_base
        total_buy_value = Decimal("0")
        total_buy_fee = Decimal("0")
        total_buy_tds = Decimal("0")

        while remaining_sell_base > Decimal("0") and pending_buys:
            buy_base, buy_val, buy_fee, buy_tds = pending_buys[0]
            if buy_base <= remaining_sell_base:
                pending_buys.popleft()
                total_buy_value += buy_val
                total_buy_fee += buy_fee
                total_buy_tds += buy_tds
                remaining_sell_base -= buy_base
            else:
                ratio = remaining_sell_base / buy_base
                total_buy_value += buy_val * ratio
                total_buy_fee += buy_fee * ratio
                total_buy_tds += buy_tds * ratio
                pending_buys[0] = (
                    buy_base - remaining_sell_base,
                    buy_val * (1 - ratio),
                    buy_fee * (1 - ratio),
                    buy_tds * (1 - ratio),
                )
                remaining_sell_base = Decimal("0")

        matched_sell_base = fill_base - remaining_sell_base
        if remaining_sell_base > Decimal("0") and fill_base > Decimal("0"):
            matched_ratio = matched_sell_base / fill_base
            logger.warning(
                f"Tax Report ({trading_pair}) - Sell Fill partially unmatched:\n"
                f"  Sold {fill_base} but only {matched_sell_base} matched tracked buys "
                f"({remaining_sell_base} excess — pre-existing inventory or post-restart sells).\n"
                f"  Profit/tax calculated only for the matched {matched_sell_base} portion."
            )
            matched_sell_value = fill_value * matched_ratio
            matched_sell_fee = fee_amount * matched_ratio
        else:
            matched_sell_value = fill_value
            matched_sell_fee = fee_amount

        result = calculate_order_profit(
            buy_value_quote=total_buy_value,
            sell_value_quote=matched_sell_value,
            buy_fee_quote=total_buy_fee,
            sell_fee_quote=matched_sell_fee,
            buy_tds_quote=total_buy_tds,
            market_type=get_market_type(trading_pair),
            tax_config=self.tax_config,
        )
        report = format_profit_report(result, quote_currency=quote)
        logger.info(f"Tax & Profit Report ({trading_pair}):\n{report}")
