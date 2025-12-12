from typing import Optional


def wazirx_pair_to_hb_pair(symbol: str) -> str:
    """Convert WazirX symbol like 'btcinr' or 'btcusdt' to Hummingbot format 'BTC-INR' or 'BTC-USDT'.
    This function makes a best-effort conversion based on common quote currencies.
    """
    s = symbol.upper()
    # handle symbols containing underscore or hyphen
    if "_" in s:
        parts = s.split("_")
        return f"{parts[0]}-{parts[1]}"

    # common quotes
    quotes = ["USDT", "USDC", "INR", "BTC", "ETH", "BUSD", "TRX"]
    for q in quotes:
        if s.endswith(q):
            base = s[:-len(q)]
            if base:
                return f"{base}-{q}"

    # fallback: return as-is
    return s


def hb_pair_to_wazirx_symbol(hb_pair: str) -> str:
    return hb_pair.replace("-", "").upper()
