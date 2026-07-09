import argparse
import asyncio
import calendar
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig, HistoricalCandlesConfig

SUPPORTED_CONNECTORS: List[str] = sorted(CandlesFactory._candles_map.keys())

INTERVAL = "1d"
CONCURRENCY = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("monthly_volume_standalone")


@dataclass
class VolumeResult:
    connector: str
    trading_pair: str
    month: Optional[str] = None
    base_volume: Optional[float] = None
    quote_volume: Optional[float] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _last_n_months(n: int) -> List[Tuple[int, int, str]]:
    """
    Returns (start_time, end_time, label) as UTC unix timestamps (seconds) for each of the
    last n full calendar months, most recent first.
    """
    now = datetime.now(timezone.utc)
    year, mon = now.year, now.month

    months = []
    for _ in range(n):
        mon -= 1
        if mon == 0:
            mon = 12
            year -= 1

        start = datetime(year, mon, 1, tzinfo=timezone.utc)
        last_day = calendar.monthrange(year, mon)[1]
        end = datetime(year, mon, last_day, 23, 59, 59, tzinfo=timezone.utc)
        label = f"{year:04d}-{mon:02d}"
        months.append((int(start.timestamp()), int(end.timestamp()), label))

    return months


async def fetch_connector_volume(connector_name: str, trading_pair: str,
                                 start_time: int, end_time: int) -> VolumeResult:
    config = CandlesConfig(connector=connector_name, trading_pair=trading_pair, interval=INTERVAL, max_records=1)
    candle = CandlesFactory.get_candle(config)
    historical_config = HistoricalCandlesConfig(
        connector_name=connector_name,
        trading_pair=trading_pair,
        interval=INTERVAL,
        start_time=start_time,
        end_time=end_time,
    )
    candles_df = await candle.get_historical_candles(historical_config)
    if candles_df is None or candles_df.empty:
        raise ValueError("no candle data returned")

    return VolumeResult(
        connector=connector_name,
        trading_pair=trading_pair,
        base_volume=float(candles_df["volume"].sum()),
        quote_volume=float(candles_df["quote_asset_volume"].sum()),
    )


async def fetch_all(targets: List[Tuple[str, str]], start_time: int, end_time: int) -> List[VolumeResult]:
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def _bounded(connector_name: str, trading_pair: str) -> VolumeResult:
        async with semaphore:
            try:
                return await fetch_connector_volume(connector_name, trading_pair, start_time, end_time)
            except Exception as e:
                logger.warning(f"Skipping {connector_name} ({trading_pair}): {e}")
                return VolumeResult(connector=connector_name, trading_pair=trading_pair, error=str(e))

    return await asyncio.gather(*(_bounded(connector_name, trading_pair) for connector_name, trading_pair in targets))


def _print_results(all_results: List[VolumeResult]):
    ok = [r for r in all_results if r.ok]
    failed = [r for r in all_results if not r.ok]
    ok.sort(key=lambda r: (r.month, r.quote_volume), reverse=True)

    print("\nMonthly volume\n")
    print(f"{'connector':<22}{'pair':<14}{'base_volume':>20}{'quote_volume':>20}{'month':>10}")
    for r in ok:
        quote_volume = f"{r.quote_volume:,.2f}" if r.quote_volume else "N/A"
        print(f"{r.connector:<22}{r.trading_pair:<14}{r.base_volume:>20,.4f}{quote_volume:>20}{r.month:>10}")

    if failed:
        print(f"\n{len(failed)} connector(s) skipped:")
        for r in failed:
            print(f"  {r.connector} ({r.trading_pair}, {r.month}): {r.error}")


def _print_summary(all_results: List[VolumeResult]):
    sums: Dict[Tuple[str, str], Dict[str, float]] = {}
    for r in all_results:
        if not r.ok:
            continue
        key = (r.connector, r.trading_pair)
        totals = sums.setdefault(key, {"base_volume": 0.0, "quote_volume": 0.0, "months": 0})
        totals["base_volume"] += r.base_volume
        totals["quote_volume"] += r.quote_volume
        totals["months"] += 1

    rows = sorted(sums.items(), key=lambda item: item[1]["quote_volume"], reverse=True)

    print(f"\nSum across {max((v['months'] for _, v in rows), default=0)} month(s)\n")
    print(f"{'connector':<22}{'pair':<14}{'months':>8}{'base_volume':>20}{'quote_volume':>20}")
    for (connector, trading_pair), totals in rows:
        quote_volume = f"{totals['quote_volume']:,.2f}" if totals["quote_volume"] else "N/A"
        print(f"{connector:<22}{trading_pair:<14}{totals['months']:>8}"
              f"{totals['base_volume']:>20,.4f}{quote_volume:>20}")


def _build_targets(connectors: List[str], trading_pairs: List[str]) -> List[Tuple[str, str]]:
    return [(connector, trading_pair) for connector in connectors for trading_pair in trading_pairs]


async def run(args: argparse.Namespace):
    connectors = [c.strip() for c in args.connectors.split(",") if c.strip()] if args.connectors else SUPPORTED_CONNECTORS
    trading_pairs = [p.strip() for p in args.trading_pairs.split(",") if p.strip()]
    targets = _build_targets(connectors, trading_pairs)

    all_results: List[VolumeResult] = []
    for start_time, end_time, label in _last_n_months(args.months):
        logger.info(f"Fetching {label} volume for {len(targets)} connector/pair combination(s)...")
        results = await fetch_all(targets, start_time, end_time)
        for result in results:
            result.month = label
        all_results.extend(results)

    _print_results(all_results)
    _print_summary(all_results)

    await ConnectionsFactory().close()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch monthly trading volume for trading pairs across connectors "
                    "supported by hummingbot's candles data feed."
    )
    parser.add_argument(
        "--connectors",
        default="",
        help=f"Comma-separated connector names to include (default: all {len(SUPPORTED_CONNECTORS)} supported: "
             f"{', '.join(SUPPORTED_CONNECTORS)})",
    )
    parser.add_argument(
        "--trading-pairs",
        default="BTC-USDT",
        help="Comma-separated trading pairs to fetch for each connector, e.g. BTC-USDT,ETH-USDT",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=1,
        help="Number of most recent full calendar months to fetch (default: 1)",
    )

    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Interrupted, exiting")


if __name__ == "__main__":
    """
    Run standalone, e.g.:
    python -m hummingbot.scripts.monthly_volume_standalone --connectors binance,coindcx --trading-pairs BTC-USDT --months 2
    python -m hummingbot.scripts.monthly_volume_standalone --connectors binance --trading-pairs BTC-USDT,ETH-USDT --months 3
    """
    main()
