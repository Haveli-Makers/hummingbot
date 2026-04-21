import asyncio
import os
import sys

from hummingbot.core.volume_oracle.volume_oracle import VolumeOracle

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


async def main():
    exchange = sys.argv[1] if len(sys.argv) > 1 else "binance"
    symbol_filter = sys.argv[2].upper() if len(sys.argv) > 2 else ""

    source = VolumeOracle.source_for_exchange(exchange)
    oracle = VolumeOracle(source=source)

    print(f"Fetching bulk 24h volumes on {oracle.source.name}...")
    try:
        results = await oracle.get_all_24h_volumes()
        print(f"Found {len(results)} symbols")

        if symbol_filter:
            filtered = {key: value for key, value in results.items() if key.upper() == symbol_filter}
            if not filtered:
                raise ValueError(f"Symbol {symbol_filter} not found on {oracle.source.name}")
            results = filtered

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
