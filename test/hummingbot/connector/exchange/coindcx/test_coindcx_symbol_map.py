"""
CoinDCX lists some pairs on two routes (on 27 Nov 2026, ASR-USDT was both "ASRUSDT" on ecode B and "ASR_USDT" on
ecode G). The symbol map must still build, with one market per pair, and the trading rules must follow it.
"""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import patch

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


def market(symbol: str, base: str, quote: str, ecode: str, min_notional: float) -> dict:
    return {"symbol": symbol, "coindcx_name": f"{base}{quote}", "pair": f"{ecode}-{base}_{quote}", "ecode": ecode,
            "status": "active", "target_currency_short_name": base, "base_currency_short_name": quote,
            "min_quantity": 1, "max_quantity": 1000000, "min_notional": min_notional,
            "base_currency_precision": 4, "target_currency_precision": 2, "step": 0.01}


ASR_G = market("ASR_USDT", "ASR", "USDT", "G", 3)  # the newer route
ASR_B = market("ASRUSDT", "ASR", "USDT", "B", 5)
ETH_INR = market("ETHINR", "ETH", "INR", "I", 100)


class CoindcxSymbolMapTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="k", coindcx_api_secret="s", trading_pairs=["ETH-INR"],
                                        trading_required=False)

    async def symbol_map(self, markets) -> dict:
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)
        return dict(await self.exchange.trading_pair_symbol_map())

    async def test_a_pair_on_two_routes_keeps_the_plain_symbol_whatever_the_order(self):
        for markets in ([ASR_G, ASR_B, ETH_INR], [ASR_B, ASR_G, ETH_INR]):
            self.assertEqual({"ASRUSDT": "ASR-USDT", "ETHINR": "ETH-INR"}, await self.symbol_map(markets))

    async def test_without_a_plain_symbol_the_first_listed_wins(self):
        markets = [market("NAKA-USDT", "NAKA", "USDT", "G", 3), market("NAKA_USDT", "NAKA", "USDT", "H", 3)]
        self.assertEqual({"NAKA-USDT": "NAKA-USDT"}, await self.symbol_map(markets))

    async def test_trading_rules_follow_the_map_without_warnings_for_the_dropped_market(self):
        markets = [ASR_G, ASR_B, ETH_INR]
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)
        with patch.object(self.exchange.logger(), "warning") as warning:
            rules = await self.exchange._format_trading_rules(markets)
        by_pair = {rule.trading_pair: rule for rule in rules}
        self.assertEqual(2, len(rules))
        self.assertEqual({"ASR-USDT", "ETH-INR"}, set(by_pair))
        self.assertEqual(Decimal("5"), by_pair["ASR-USDT"].min_notional_size)  # the mapped market's rules (ecode B)
        warning.assert_not_called()

    async def test_update_trading_rules_works_with_a_pair_on_two_routes(self):
        # Both requests read markets_details: the rules, and the symbol map the first rule lookup builds.
        markets = [ASR_G, ASR_B, ETH_INR]
        with patch.object(self.exchange, "_make_trading_rules_request", return_value=markets), \
                patch.object(self.exchange, "_make_trading_pairs_request", return_value=markets):
            await self.exchange._update_trading_rules()
        self.assertEqual({"ASR-USDT", "ETH-INR"}, set(self.exchange._trading_rules))
        self.assertEqual("ETHINR", await self.exchange.exchange_symbol_associated_to_pair("ETH-INR"))
