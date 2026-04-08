import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hummingbot.core.volume_oracle.volume_oracle import VolumeOracle


async def main():
    exchange = sys.argv[1] if len(sys.argv) > 1 else "binance"
    pair = sys.argv[2] if len(sys.argv) > 2 else "BTC-USDT"

    source = VolumeOracle.source_for_exchange(exchange)
    oracle = VolumeOracle(source=source)

    print(f"Fetching 24h volume for {pair} on {oracle.source.name}...")
    try:
        result = await oracle.get_24h_volume(pair)
        print(f"\n  Exchange     : {result['exchange']}")
        print(f"  Pair         : {result['trading_pair']}")
        print(f"  Symbol       : {result['symbol']}")
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
