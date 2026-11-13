"""
Picks Delta options before a bot starts. Hummingbot calls a script's `init_markets` synchronously, before any
connector runs, so the option chain is read here over plain HTTP from Delta's public API, without keys.
"""
import json
import time
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from hummingbot.connector.derivative.delta_options import (
    delta_options_constants as CONSTANTS,
    delta_options_utils as utils,
    delta_options_web_utils as web_utils,
)
from hummingbot.core.data_type.option_info import OptionInstrumentInfo, OptionType

REQUEST_TIMEOUT = 10.0


def _get(path_url: str, params: Optional[Dict[str, Any]] = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> Any:
    url = web_utils.public_rest_url(path_url, domain)
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": CONSTANTS.USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return json.loads(response.read())


def fetch_option_chain(underlying: str = "BTC",
                       domain: str = CONSTANTS.DEFAULT_DOMAIN) -> Tuple[List[OptionInstrumentInfo], Optional[Decimal]]:
    """
    Every live vanilla option on the underlying, and the underlying's spot price (read from one option's ticker; None
    when Delta lists no option on it).
    """
    params: Dict[str, Any] = {
        "contract_types": CONSTANTS.OPTION_CONTRACT_TYPES,
        "states": CONSTANTS.LIVE_STATE,
        "page_size": CONSTANTS.PRODUCTS_PAGE_SIZE,
    }
    instruments = []
    for _ in range(CONSTANTS.MAX_PRODUCT_PAGES):
        response = _get(CONSTANTS.PRODUCTS_PATH_URL, params, domain)
        for product in response.get("result") or []:
            asset = str((product.get("underlying_asset") or {}).get("symbol") or "") if isinstance(product, dict) else ""
            if not utils.is_live_vanilla_option(product) or asset.upper() != underlying.upper():
                continue
            trading_pair = utils.trading_pair_from_symbol(product["symbol"], product["quoting_asset"]["symbol"])
            instruments.append(utils.instrument_from_product(product, trading_pair))
        after = (response.get("meta") or {}).get("after")
        if not after:
            break
        params = {**params, "after": after}
    if not instruments:
        return [], None
    symbol = utils.symbol_from_trading_pair(instruments[0].trading_pair)
    result = _get(CONSTANTS.TICKERS_PATH_URL.format(symbols=symbol), domain=domain).get("result")
    ticker = result if isinstance(result, dict) else (result or [{}])[0]
    spot = ticker.get("spot_price")
    return instruments, Decimal(str(spot)) if spot not in (None, "") else None


def pick_strangle(chain: List[OptionInstrumentInfo], spot: Decimal, otm: Decimal = Decimal("0.02"),
                  min_hours_to_expiry: Decimal = Decimal("1"),
                  now: Optional[float] = None) -> Tuple[OptionInstrumentInfo, OptionInstrumentInfo]:
    """
    The call and the put of a strangle: in the soonest expiry at least `min_hours_to_expiry` away, the call whose
    strike is nearest `otm` above spot and the put nearest `otm` below it. Raises ValueError if no expiry has both.
    """
    now = time.time() if now is None else now
    earliest = now + float(min_hours_to_expiry) * 3600
    for expiry in sorted({i.expiry_timestamp for i in chain if i.expiry_timestamp >= earliest}):
        same = [i for i in chain if i.expiry_timestamp == expiry]
        calls = [i for i in same if i.option_type == OptionType.CALL and i.strike > spot]
        puts = [i for i in same if i.option_type == OptionType.PUT and i.strike < spot]
        if calls and puts:
            call = min(calls, key=lambda i: abs(i.strike - spot * (1 + otm)))
            put = min(puts, key=lambda i: abs(i.strike - spot * (1 - otm)))
            return call, put
    raise ValueError(f"No expiry at least {min_hours_to_expiry} h away lists both a call above and a put below "
                     f"spot {spot}.")
