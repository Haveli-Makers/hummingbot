import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from test.hummingbot.connector.derivative.delta_options.test_delta_options_utils import option_product
from unittest.mock import MagicMock, patch

from hummingbot.connector.derivative.delta_options import delta_options_chain as chain_module
from hummingbot.core.data_type.option_info import OptionInstrumentInfo, OptionType

TODAY = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()
TOMORROW = TODAY + 86400


def instrument(kind, strike, expiry):
    return OptionInstrumentInfo(trading_pair=f"{kind}_BTC_{strike}-USD", underlying="BTC",
                                option_type=OptionType.CALL if kind == "C" else OptionType.PUT,
                                strike=Decimal(strike), expiry_timestamp=expiry)


CHAIN = [instrument(kind, strike, expiry) for expiry in (TODAY, TOMORROW)
         for kind in ("C", "P") for strike in (96000, 98000, 100000, 102000, 104000)]


class PickStrangleTests(unittest.TestCase):
    def test_picks_the_call_and_put_nearest_the_distance_from_spot(self):
        call, put = chain_module.pick_strangle(CHAIN, Decimal("100500"), Decimal("0.02"), now=TODAY - 5 * 3600)

        self.assertEqual(("C_BTC_102000-USD", "P_BTC_98000-USD"), (call.trading_pair, put.trading_pair))
        self.assertEqual((TODAY, TODAY), (call.expiry_timestamp, put.expiry_timestamp))

    def test_an_expiry_too_close_is_skipped(self):
        call, put = chain_module.pick_strangle(CHAIN, Decimal("100500"), Decimal("0.02"), Decimal("1"),
                                               now=TODAY - 1800)

        self.assertEqual((TOMORROW, TOMORROW), (call.expiry_timestamp, put.expiry_timestamp))

    def test_only_out_of_the_money_strikes_are_used(self):
        call, put = chain_module.pick_strangle(CHAIN, Decimal("100500"), Decimal("0"), now=TODAY - 5 * 3600)

        self.assertEqual((Decimal("102000"), Decimal("100000")), (call.strike, put.strike))

    def test_no_suitable_expiry_raises(self):
        only_calls = [i for i in CHAIN if i.option_type == OptionType.CALL]

        with self.assertRaises(ValueError):
            chain_module.pick_strangle(only_calls, Decimal("100500"), now=TODAY - 5 * 3600)


class FetchOptionChainTests(unittest.TestCase):
    def test_reads_every_page_and_keeps_live_options_of_the_underlying(self):
        page_1 = {"result": [option_product("C-BTC-100000-251226", id=1),
                             option_product("P-ETH-3000-251226", "put_options", id=2,
                                            underlying_asset={"symbol": "ETH"})],
                  "meta": {"after": "cursor-2"}}
        page_2 = {"result": [option_product("P-BTC-98000-251226", "put_options", id=3),
                             option_product("C-BTC-90000-251226", id=4, state="expired")],
                  "meta": {"after": None}}
        ticker = {"result": {"symbol": "C-BTC-100000-251226", "spot_price": "100512.5"}}

        with patch.object(chain_module, "_get", side_effect=[page_1, page_2, ticker]) as get:
            instruments, spot = chain_module.fetch_option_chain("BTC")

        self.assertEqual(["C_BTC_100000_251226-USD", "P_BTC_98000_251226-USD"], [i.trading_pair for i in instruments])
        self.assertEqual(Decimal("100512.5"), spot)
        self.assertEqual("cursor-2", get.call_args_list[1].args[1]["after"])
        self.assertEqual("/v2/tickers/C-BTC-100000-251226", get.call_args_list[2].args[0])

    def test_no_options_means_no_spot(self):
        with patch.object(chain_module, "_get", return_value={"result": [], "meta": {}}):
            self.assertEqual(([], None), chain_module.fetch_option_chain("BTC"))

    def test_requests_go_to_delta_with_the_parameters(self):
        response = MagicMock()
        response.read.return_value = json.dumps({"result": []}).encode()
        response.__enter__.return_value = response

        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            chain_module._get("/v2/products", {"states": "live", "page_size": 100})

        request = urlopen.call_args.args[0]
        self.assertEqual("https://api.india.delta.exchange/v2/products?states=live&page_size=100", request.full_url)
        self.assertEqual("hummingbot", request.get_header("User-agent"))
