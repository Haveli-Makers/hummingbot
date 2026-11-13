"""
Offline rehearsal of scripts/options_strangle_example.py. Nothing touches the network.

The REAL script runs under a real Hummingbot Clock against the REAL DeltaOptionsDerivative, which talks to a simulated
Delta (rehearse_hold's SettlingDelta): orders fill at the book, and each position is settled at its option's
expiry as Delta did on 10 Nov. The options expire a few seconds after the start.

    python rehearse_strangle.py

Scenarios: the full cycle (buy both legs, expire, settle, done) and a stop before expiry (closes both legs).
"""
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import importlib.util  # noqa: E402

import rehearse_hold as stage  # noqa: E402  (simulated Delta and its setup; also puts the repo on sys.path)

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS  # noqa: E402
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative  # noqa
from hummingbot.core.clock import Clock, ClockMode  # noqa: E402
from hummingbot.core.utils.async_utils import safe_ensure_future  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "options_strangle_example", "/home/vinayak/hm/hummingbot/scripts/options_strangle_example.py")
example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(example)

SPOT = stage.SPOT  # 100000


def strangle_world(seconds, settle_index):
    """A call and a put around spot, expiring `seconds` from now, plus their neighbours."""
    expiry = datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc).replace(microsecond=0)
    books = {
        ("C", 102000): ([(Decimal("40"), 5)], [(Decimal("45"), 5)], Decimal("42")),
        ("C", 104000): ([(Decimal("10"), 5)], [(Decimal("12"), 5)], Decimal("11")),
        ("P", 98000): ([(Decimal("35"), 5)], [(Decimal("40"), 5)], Decimal("37")),
        ("P", 96000): ([(Decimal("9"), 5)], [(Decimal("11"), 5)], Decimal("10")),
    }
    products = [stage._product(kind, strike, expiry, 700 + n) for n, (kind, strike) in enumerate(books)]
    market = {stage._symbol(kind, strike, expiry): book for (kind, strike), book in books.items()}
    bands = {symbol: (Decimal("0.1"), mark * 3) for symbol, (_, _, mark) in market.items()}
    stage.use_world(products, market, bands, settle_index=settle_index)
    return f"C_BTC_102000_{expiry:%d%m%y}-USD", f"P_BTC_98000_{expiry:%d%m%y}-USD"


async def scenario(name, seconds, settle_index, stop_after=None, timeout=40):
    print(f"\n\n######## SCENARIO: {name} ########")
    call, put = strangle_world(seconds, settle_index)
    connector = DeltaOptionsDerivative("key", "secret", trading_pairs=[call, put], trading_required=True,
                                       delta_options_stop_before_expiry_minutes=0)
    await stage.fake_wait_ready(connector, stage.hold.verify.Results())
    fake = stage.FAKES[-1]
    config = example.OptionsStrangleConfig(call_pair=call, put_pair=put, hedge=False)
    example.OptionsStrangleExample.init_markets(config)
    strategy = example.OptionsStrangleExample({"delta_options": connector}, config)
    clock = Clock(ClockMode.REALTIME, tick_size=0.5)
    clock.add_iterator(strategy)
    started = time.time()
    with clock:
        running = safe_ensure_future(clock.run())
        try:
            while time.time() - started < timeout and strategy.state != "done":
                if stop_after is not None and time.time() - started >= stop_after:
                    await strategy.on_stop()
                    break
                await asyncio.sleep(0.25)
        finally:
            running.cancel()
            # Let the clock finish cancelling before its context closes.
            await asyncio.gather(running, return_exceptions=True)
    fake.stop()
    await connector.stop_network()
    print(f"  >> state {strategy.state} after {time.time() - started:.1f}s")
    print(f"  >> fills: {[(f.trading_pair, f.trade_type.name, str(f.amount), str(f.price)) for f in strategy.fills]}")
    print(f"  >> settlements: {[(p, str(e.price)) for p, e in strategy.settlements.items()]}")
    print(f"  >> positions on the simulated Delta: {fake.positions}; open orders: {len(fake.open_orders())}")
    print(f"  >> status:\n{strategy.format_status()}")
    if strategy.state == "done":
        print(f"  >> {strategy.result()}")
    return strategy, fake, (call, put)


async def main():
    stage.hold.verify._install_log_capture()
    stage.hold.verify._quiet_app_warnings()
    DeltaOptionsDerivative.ready = property(lambda self: True)
    DeltaOptionsDerivative.current_timestamp = property(lambda self: time.time())
    DeltaOptionsDerivative.EXPIRY_CHECK_INTERVAL = 0.5
    CONSTANTS.SETTLEMENT_READ_DELAY, CONSTANTS.SETTLEMENT_POLL_INTERVAL = 0.5, 1.0
    outcomes = {}

    strategy, fake, (call, put) = await scenario("full cycle: buy, expire, settle", seconds=8,
                                                 settle_index=SPOT + 3000)
    bought = sorted((f.trading_pair, f.trade_type.name) for f in strategy.fills)
    settled = {p: str(e.price) for p, e in strategy.settlements.items()}
    outcomes["full cycle"] = (
        strategy.state == "done" and bought == sorted([(call, "BUY"), (put, "BUY")])
        and settled == {call: "1000", put: "0"} and not fake.positions and not fake.open_orders(),
        f"state {strategy.state}, fills {bought}, settlements {settled}")

    strategy, fake, (call, put) = await scenario("stop before expiry closes both legs", seconds=60,
                                                 settle_index=SPOT, stop_after=4)
    sides = sorted((f.trading_pair, f.trade_type.name) for f in strategy.fills)
    outcomes["stop"] = (
        sides == sorted([(call, "BUY"), (put, "BUY"), (call, "SELL"), (put, "SELL")]) and not fake.positions
        and not fake.open_orders() and strategy.state == "stopped",
        f"fills {sides}, positions left {fake.positions}")

    print("\n\n######## SUMMARY ########")
    for name, (passed, detail) in outcomes.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name}: {detail}")


if __name__ == "__main__":
    asyncio.run(main())
