"""
CoinDCX Balance Checker Script
==============================
This script fetches and displays your CoinDCX account balances.

Usage:
    1. Create a .env file in the hummingbot root directory with:
       COINDCX_API_KEY=your_api_key
       COINDCX_SECRET_KEY=your_secret_key

    2. Run from hummingbot directory:
       python scripts/utility/coindcx_balance_checker.py
"""

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

script_dir = Path(__file__).resolve().parent
hummingbot_root = script_dir.parent.parent
sys.path.insert(0, str(hummingbot_root))


def load_env_file():
    """Load environment variables from .env file."""
    env_path = hummingbot_root / ".env"
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    os.environ[key] = value


load_env_file()


REST_URL = "https://api.coindcx.com"
USER_BALANCES_PATH_URL = "/exchange/v1/users/balances"


def get_credentials() -> tuple:
    """
    Get API credentials from environment variables (loaded from .env file).
    """
    api_key = os.environ.get("COINDCX_API_KEY", "")
    secret_key = os.environ.get("COINDCX_SECRET_KEY", "")
    
    if not api_key or not secret_key or api_key == "your_api_key_here":
        env_path = hummingbot_root / ".env"
        print("\n❌ API credentials not configured!")
        print("\nPlease edit the .env file at:")
        print(f"   {env_path}")
        print("\nAnd set your CoinDCX API credentials:")
        print("   COINDCX_API_KEY=your_actual_api_key")
        print("   COINDCX_SECRET_KEY=your_actual_secret_key")
        return "", ""
    
    return api_key, secret_key


def generate_signature(secret_key: str, payload: str) -> str:
    """
    Generates HMAC SHA256 signature for the given payload.
    
    :param secret_key: Your CoinDCX secret key
    :param payload: JSON string of the request body
    :return: Hexadecimal signature string
    """
    secret_bytes = bytes(secret_key, encoding='utf-8')
    signature = hmac.new(secret_bytes, payload.encode(), hashlib.sha256).hexdigest()
    return signature


async def fetch_balances(api_key: str, secret_key: str) -> Optional[List[Dict]]:
    """
    Fetch account balances from CoinDCX API.
    
    :param api_key: Your CoinDCX API key
    :param secret_key: Your CoinDCX secret key
    :return: List of balance dictionaries or None on error
    """
    url = f"{REST_URL}{USER_BALANCES_PATH_URL}"
    
    # Prepare request body with timestamp
    timestamp = int(time.time() * 1000)
    body = {"timestamp": timestamp}
    json_body = json.dumps(body, separators=(',', ':'))
    
    # Generate signature
    signature = generate_signature(secret_key, json_body)
    
    # Prepare headers
    headers = {
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature,
        "Content-Type": "application/json"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=json_body) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error_text = await response.text()
                    print(f"Error: HTTP {response.status}")
                    print(f"Response: {error_text}")
                    return None
    except Exception as e:
        print(f"Error fetching balances: {e}")
        return None


def format_balance(balance: Decimal, decimals: int = 8) -> str:
    """Format balance with proper decimal places."""
    if balance == 0:
        return "0"
    formatted = f"{balance:.{decimals}f}".rstrip('0').rstrip('.')
    return formatted


def display_balances(balances: List[Dict], show_zero: bool = False, min_value_inr: float = 0):
    """
    Display balances in a formatted table.
    
    :param balances: List of balance dictionaries from API
    :param show_zero: Whether to show zero balances
    :param min_value_inr: Minimum INR value to display (filter small balances)
    """
    print("\n" + "=" * 60)
    print("                    CoinDCX Account Balances")
    print("=" * 60)
    
    # Filter and sort balances
    non_zero_balances = []
    for b in balances:
        balance = Decimal(str(b.get("balance", 0)))
        locked_balance = Decimal(str(b.get("locked_balance", 0)))
        total = balance + locked_balance
        
        if total > 0 or show_zero:
            non_zero_balances.append({
                "currency": b.get("currency", ""),
                "available": balance,
                "locked": locked_balance,
                "total": total
            })
    
    # Sort by total balance descending
    non_zero_balances.sort(key=lambda x: x["total"], reverse=True)
    
    if not non_zero_balances:
        print("\nNo balances found.")
        return
    
    # Print header
    print(f"\n{'Currency':<12} {'Available':>18} {'Locked':>18} {'Total':>18}")
    print("-" * 70)
    
    # Print balances
    for b in non_zero_balances:
        currency = b["currency"]
        available = format_balance(b["available"])
        locked = format_balance(b["locked"])
        total = format_balance(b["total"])
        
        print(f"{currency:<12} {available:>18} {locked:>18} {total:>18}")
    
    print("-" * 70)
    print(f"Total assets with balance: {len(non_zero_balances)}")
    print("=" * 60)


async def get_inr_prices() -> Dict[str, Decimal]:
    """
    Fetch INR prices from CoinDCX ticker to calculate portfolio value.
    """
    url = "https://api.coindcx.com/exchange/ticker"
    prices = {}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        market = ticker.get("market", "")
                        if market.endswith("INR"):
                            base = market[:-3]  # Remove INR suffix
                            bid = ticker.get("bid")
                            ask = ticker.get("ask")
                            if bid and ask:
                                try:
                                    mid = (Decimal(str(bid)) + Decimal(str(ask))) / 2
                                    prices[base] = mid
                                except Exception:
                                    pass
                    # INR itself
                    prices["INR"] = Decimal("1")
    except Exception as e:
        print(f"Warning: Could not fetch INR prices: {e}")
    
    return prices


async def display_portfolio_value(balances: List[Dict]):
    """
    Display total portfolio value in INR.
    """
    prices = await get_inr_prices()
    
    if not prices:
        print("\nCould not calculate portfolio value (no price data)")
        return
    
    total_inr = Decimal("0")
    valued_assets = []
    unvalued_assets = []
    
    for b in balances:
        currency = b.get("currency", "")
        balance = Decimal(str(b.get("balance", 0)))
        locked = Decimal(str(b.get("locked_balance", 0)))
        total = balance + locked
        
        if total > 0:
            if currency in prices:
                value_inr = total * prices[currency]
                total_inr += value_inr
                valued_assets.append({
                    "currency": currency,
                    "amount": total,
                    "price_inr": prices[currency],
                    "value_inr": value_inr
                })
            else:
                unvalued_assets.append({
                    "currency": currency,
                    "amount": total
                })
    
    # Sort by value
    valued_assets.sort(key=lambda x: x["value_inr"], reverse=True)
    
    print("\n" + "=" * 70)
    print("                    Portfolio Value (INR)")
    print("=" * 70)
    print(f"\n{'Currency':<10} {'Amount':>16} {'Price (INR)':>14} {'Value (INR)':>16}")
    print("-" * 70)
    
    for asset in valued_assets:
        if asset["value_inr"] >= 1:  # Only show assets worth >= 1 INR
            print(f"{asset['currency']:<10} {format_balance(asset['amount']):>16} "
                  f"{format_balance(asset['price_inr'], 2):>14} "
                  f"{format_balance(asset['value_inr'], 2):>16}")
    
    print("-" * 70)
    print(f"{'TOTAL':>42} ₹{format_balance(total_inr, 2):>16}")
    print("=" * 70)
    
    if unvalued_assets:
        print(f"\nNote: {len(unvalued_assets)} assets could not be valued (no INR pair)")


async def main():
    """Main function to run the balance checker."""
    print("\n" + "=" * 50)
    print("        🔍 CoinDCX Balance Checker")
    print("=" * 50)
    
    # Get credentials
    api_key, secret_key = get_credentials()
    
    if not api_key or not secret_key:
        return
    
    print(f"\n✓ API Key: {api_key[:8]}...{api_key[-4:] if len(api_key) > 12 else ''}")
    print("\nFetching balances from CoinDCX...")
    
    # Fetch balances
    balances = await fetch_balances(api_key, secret_key)
    
    if balances is None:
        print("\n❌ Failed to fetch balances. Please check your API credentials.")
        return
    
    if isinstance(balances, dict) and "message" in balances:
        print(f"\n❌ API Error: {balances['message']}")
        return
    
    if isinstance(balances, dict) and "error" in balances:
        print(f"\n❌ API Error: {balances['error']}")
        return
    
    # Display balances
    display_balances(balances)
    
    # Display portfolio value
    await display_portfolio_value(balances)
    
    print("\n✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
