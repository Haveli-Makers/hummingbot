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
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

S_DECIMAL_0 = Decimal("0")

# Default rates per Indian tax law
DEFAULT_TDS_RATE = Decimal("0.01")          # 1% TDS (Section 194S)
DEFAULT_PROFIT_TAX_RATE = Decimal("0.30")   # 30% flat tax (Section 115BBH)


class MarketType(Enum):
    """
    Determines which sides of a trade owe TDS (Section 194S).

    INR:           Quote currency is INR (e.g., BTC-INR).
                   Only the SELLER pays TDS on their INR proceeds.
    CRYPTO_CRYPTO: Quote currency is a crypto asset (USDT, BTC, ETH, etc.).
                   BOTH buyer and seller pay 1% TDS on the fill value in
                   the quote asset.
    """
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
    """
    Calculate TDS for a single trade fill (Section 194S).

    INR markets (quote = INR):
      - SELLER: TDS = fill_value × 1%   (seller receives 1% less INR)
      - BUYER:  TDS = 0                  (buyers in INR markets are exempt)

    Crypto-Crypto markets (quote = USDT / BTC / ETH / etc.):
      - SELLER: TDS = fill_value × 1%   (seller receives 1% less quote asset)
      - BUYER:  TDS = fill_value × 1%   (buyer pays 1% extra in quote asset)

    :param fill_value_quote: fill_amount × fill_price in the quote currency
    :param is_buyer: True for the buy side, False for the sell side
    :param market_type: INR or CRYPTO_CRYPTO (default CRYPTO_CRYPTO for safety)
    :param config: tax rates config (uses defaults if None)
    :return: TDSResult; tds_amount_quote == 0 when is_applicable is False
    """
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
    """
    Calculate 30% income tax on crypto profit (Section 115BBH).

    Tax base is the gross transfer gain (sell value minus cost of acquisition).
    Under s.115BBH(2)(b) no deduction for exchange fees or any other expenditure
    is permitted — only the cost of acquisition may be subtracted.

    If taxable_profit > 0: tax = taxable_profit × 30%, additional_due = tax - tds_paid
    If taxable_profit <= 0: tax = 0; TDS already paid is claimable as refund at ITR.

    :param taxable_profit: Gross transfer gain (sell_value - buy_value), fees excluded
    :param tds_already_paid: Total TDS deducted at source — include BOTH buy-side
                             and sell-side TDS for crypto-crypto markets
    :param config: Tax config with rates (uses defaults if None)
    :return: ProfitTaxResult with full tax breakdown
    """
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
