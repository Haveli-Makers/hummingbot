import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hummingbot.core.volume_oracle.volume_oracle import VolumeOracle

logging.basicConfig(level=logging.WARNING, format="%(name)s - %(levelname)s - %(message)s")

logging.basicConfig(level=logging.WARNING, format="%(name)s - %(levelname)s - %(message)s")


async def main():
    exchange = sys.argv[1] if len(sys.argv) > 1 else "binance"
    raw_pairs = [a.upper() for a in sys.argv[2:]] if len(sys.argv) > 2 else []
    trading_pairs = raw_pairs if raw_pairs and raw_pairs != ["ALL"] else None

    source = VolumeOracle.source_for_exchange(exchange)
    oracle = VolumeOracle(source=source)

    filter_desc = f" for {trading_pairs}" if trading_pairs else " (all pairs)"
    print(f"Fetching 24h volumes on {oracle.source.name}{filter_desc}...")
    try:
        results = await oracle.get_all_24h_volumes(trading_pairs=trading_pairs)
        print(f"Found {len(results)} symbols")

        if trading_pairs and not results:
            raise ValueError(f"No results for {trading_pairs} on {oracle.source.name}")

        for symbol, result in results.items():
            print(f"\n  Symbol       : {symbol}")
            print(f"  Exchange     : {result['exchange']}")
            print(f"  Base Volume  : {result['base_volume']}")
            if "quote_volume" in result:
                print(f"  Quote Volume : {result['quote_volume']}")
            print(f"  Last Price   : {result['last_price']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await oracle.close()


if __name__ == "__main__":
    asyncio.run(main())
