from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from hummingbot.core.data_type.india_crypto_tax import (
    IndiaCryptoTaxConfig,
    MarketType,
    ProfitTaxResult,
    TDSResult,
    calculate_profit_tax,
    calculate_tds,
)

S_DECIMAL_0 = Decimal("0")


@dataclass
class OrderProfitResult:
    """Complete profit breakdown for a buy-sell pair."""
    buy_value_quote: Decimal
    sell_value_quote: Decimal
    buy_fee_quote: Decimal
    sell_fee_quote: Decimal
    total_fees_quote: Decimal
    tds_on_sell: TDSResult
    tds_on_buy_quote: Decimal           # buy-side TDS (0 for INR markets)
    total_tds_quote: Decimal            # tds_on_sell.tds_amount + tds_on_buy_quote
    gross_profit_quote: Decimal         # sell_value - buy_value
    profit_after_fees: Decimal          # gross_profit - total_fees
    profit_tax: ProfitTaxResult
    net_profit_post_tax: Decimal        # profit_after_fees - tax_liability
    effective_tax_rate_pct: Decimal     # (total_fees + tax_liability) / gross_profit × 100
    net_return_pct: Decimal             # net_profit / total_cost × 100


def calculate_order_profit(
    buy_value_quote: Decimal,
    sell_value_quote: Decimal,
    buy_fee_quote: Decimal,
    sell_fee_quote: Decimal,
    buy_tds_quote: Decimal = S_DECIMAL_0,
    market_type: MarketType = MarketType.CRYPTO_CRYPTO,
    tax_config: Optional[IndiaCryptoTaxConfig] = None,
) -> OrderProfitResult:
    """
    Calculate net profit for a buy-sell order pair with Indian crypto tax.

    :param buy_value_quote: Buy trade value (amount × price) in quote currency
    :param sell_value_quote: Sell trade value (amount × price) in quote currency
    :param buy_fee_quote: Buy-side exchange fee in quote currency
    :param sell_fee_quote: Sell-side exchange fee in quote currency
    :param buy_tds_quote: TDS paid on the buy side (0 for INR markets; 1% of
                          buy value for crypto-crypto markets)
    :param market_type: INR or CRYPTO_CRYPTO — controls sell-side TDS applicability
    :param tax_config: India crypto tax config (uses defaults if None)
    :return: OrderProfitResult with complete breakdown
    """
    if tax_config is None:
        tax_config = IndiaCryptoTaxConfig()

    tds_result = calculate_tds(
        fill_value_quote=sell_value_quote,
        is_buyer=False,
        market_type=market_type,
        config=tax_config,
    )

    total_tds = tds_result.tds_amount_quote + buy_tds_quote

    total_fees = buy_fee_quote + sell_fee_quote

    gross_profit = sell_value_quote - buy_value_quote
    profit_after_fees = gross_profit - total_fees

    profit_tax = calculate_profit_tax(
        profit_after_fees=profit_after_fees,
        tds_already_paid=total_tds,
        config=tax_config,
    )

    net_profit_post_tax = profit_after_fees - profit_tax.tax_liability

    total_cost = buy_value_quote + buy_fee_quote
    if gross_profit > S_DECIMAL_0:
        effective_tax_rate = ((total_fees + profit_tax.tax_liability) / gross_profit) * Decimal("100")
    else:
        effective_tax_rate = S_DECIMAL_0

    if total_cost > S_DECIMAL_0:
        net_return = (net_profit_post_tax / total_cost) * Decimal("100")
    else:
        net_return = S_DECIMAL_0

    return OrderProfitResult(
        buy_value_quote=buy_value_quote,
        sell_value_quote=sell_value_quote,
        buy_fee_quote=buy_fee_quote,
        sell_fee_quote=sell_fee_quote,
        total_fees_quote=total_fees,
        tds_on_sell=tds_result,
        tds_on_buy_quote=buy_tds_quote,
        total_tds_quote=total_tds,
        gross_profit_quote=gross_profit,
        profit_after_fees=profit_after_fees,
        profit_tax=profit_tax,
        net_profit_post_tax=net_profit_post_tax,
        effective_tax_rate_pct=effective_tax_rate,
        net_return_pct=net_return,
    )


def format_profit_report(result: OrderProfitResult, quote_currency: str = "INR") -> str:
    """Format an OrderProfitResult into a human-readable log string."""
    lines = [
        f"  Buy Value:           {result.buy_value_quote:.2f} {quote_currency}",
        f"  Sell Value:          {result.sell_value_quote:.2f} {quote_currency}",
        f"  Buy Fee:             {result.buy_fee_quote:.2f} {quote_currency}",
        f"  Sell Fee:            {result.sell_fee_quote:.2f} {quote_currency}",
        f"  TDS on Sell (1%):    {result.tds_on_sell.tds_amount_quote:.2f} {quote_currency}",
        f"  TDS on Buy  (1%):    {result.tds_on_buy_quote:.2f} {quote_currency}",
        f"  Total TDS:           {result.total_tds_quote:.2f} {quote_currency}",
        f"  Gross Profit:        {result.gross_profit_quote:.2f} {quote_currency}",
        f"  Profit After Fees:   {result.profit_after_fees:.2f} {quote_currency}",
        f"  30% Tax Liability:   {result.profit_tax.tax_liability:.2f} {quote_currency}",
        f"  TDS Credit:          {result.profit_tax.tds_already_paid:.2f} {quote_currency}",
        f"  Additional Tax Due:  {result.profit_tax.additional_tax_due:.2f} {quote_currency}",
        f"  Net Profit (Post-Tax): {result.net_profit_post_tax:.2f} {quote_currency}",
        f"  Effective Tax Rate:  {result.effective_tax_rate_pct:.1f}%",
        f"  Net Return:          {result.net_return_pct:.2f}%",
    ]
    return "\n".join(lines)
