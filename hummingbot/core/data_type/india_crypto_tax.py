"""
India Crypto Tax Calculator

Implements TDS (Tax Deducted at Source) and Income Tax calculations per Indian tax law:
- TDS: 1% deducted on every sell transaction from the sell value (Section 194S)
- Income Tax: 30% flat tax on crypto profits (Section 115BBH), no loss offset allowed
- TDS is an advance tax adjusted against the 30% liability during ITR filing
"""
from dataclasses import dataclass
from decimal import Decimal

S_DECIMAL_0 = Decimal("0")
S_DECIMAL_1 = Decimal("1")

# Default rates per Indian tax law
DEFAULT_TDS_RATE = Decimal("0.01")          # 1% TDS on sell value
DEFAULT_PROFIT_TAX_RATE = Decimal("0.30")   # 30% flat tax on profits


@dataclass
class IndiaCryptoTaxConfig:
    """Configuration for India crypto tax rates."""
    tds_rate: Decimal = DEFAULT_TDS_RATE
    profit_tax_rate: Decimal = DEFAULT_PROFIT_TAX_RATE


@dataclass
class TDSResult:
    """TDS deduction details for a single sell trade."""
    sell_value_quote: Decimal
    tds_rate: Decimal
    tds_amount_quote: Decimal


@dataclass
class ProfitTaxResult:
    """Profit tax calculation result for a buy-sell pair."""
    profit_after_fees: Decimal
    tax_rate: Decimal
    tax_liability: Decimal
    tds_already_paid: Decimal
    additional_tax_due: Decimal


def calculate_tds(sell_value_quote: Decimal,
                  config: IndiaCryptoTaxConfig = None) -> TDSResult:
    """
    Calculate TDS for a sell transaction.

    TDS = sell_value × 1%
    Applied only on sell transactions, deducted immediately at source.

    :param sell_value_quote: Total sell value in quote currency (amount × price)
    :param config: Tax config with rates (uses defaults if None)
    :return: TDSResult with deduction details
    """
    if config is None:
        config = IndiaCryptoTaxConfig()

    tds_amount = sell_value_quote * config.tds_rate

    return TDSResult(
        sell_value_quote=sell_value_quote,
        tds_rate=config.tds_rate,
        tds_amount_quote=tds_amount,
    )


def calculate_profit_tax(profit_after_fees: Decimal,
                         tds_already_paid: Decimal = S_DECIMAL_0,
                         config: IndiaCryptoTaxConfig = None) -> ProfitTaxResult:
    """
    Calculate 30% income tax on crypto profit.

    If profit > 0: tax = profit × 30%, additional_due = tax - tds_paid
    If profit <= 0: tax = 0, TDS can be claimed as refund

    :param profit_after_fees: Net profit after all trade fees
    :param tds_already_paid: Total TDS deducted from sell transactions
    :param config: Tax config with rates (uses defaults if None)
    :return: ProfitTaxResult with full tax breakdown
    """
    if config is None:
        config = IndiaCryptoTaxConfig()

    if profit_after_fees > S_DECIMAL_0:
        tax_liability = profit_after_fees * config.profit_tax_rate
    else:
        tax_liability = S_DECIMAL_0

    additional_tax_due = tax_liability - tds_already_paid

    return ProfitTaxResult(
        profit_after_fees=profit_after_fees,
        tax_rate=config.profit_tax_rate,
        tax_liability=tax_liability,
        tds_already_paid=tds_already_paid,
        additional_tax_due=additional_tax_due,
    )
