from hummingbot.data_feed.candles_feed.coinswitch_spot_candles.coinswitch_spot_candles import CoinswitchSpotCandles


class CsxSpotCandles(CoinswitchSpotCandles):
    """
    CSX has no independent candles API of its own. Rather than maintain a second copy of
    CoinswitchSpotCandles that quietly serves CoinSwitch PRO's "coinswitchx" venue data under
    a different name, this is an explicit alias/subclass: selecting "csx" is documented and
    behaves identically to selecting "coinswitch", both backed by the same upstream data.
    """

    @property
    def name(self) -> str:
        return f"csx_{self._trading_pair}"
