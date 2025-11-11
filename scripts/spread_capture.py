"""
Spread Capture Strategy

This strategy collects bid/ask spread data from Binance (or other connectors) and stores
snapshot samples at configurable intervals. It also computes rolling averages of spreads
over a defined time window and persists results to SQLite.

"""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.model import HummingbotBase
from hummingbot.model.spread_samples import SpreadSample
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


class SpreadSamplerConfig(StrategyV2ConfigBase):
    """
    Generic config for the spread sampler. Accepts connector/exchange aliases.
    quote is a list of quotes to include (e.g. ["USDT", "BUSD"]). Backwards-compatible with a single string.
    """
    script_file_name: str = os.path.basename(__file__)

    connector_name: Optional[str] = Field(None, description="Primary connector/exchange name (e.g., binance).")
    exchange: Optional[str] = Field(None, description="Alias for connector_name.")
    connector: Optional[str] = Field(None, description="Alias for connector_name.")
    quote: List[str] = Field(default_factory=lambda: ["USDT"], description="List of quote assets to filter trading pairs (e.g., ['USDT']).")
    snapshot_interval_min: int = Field(default=15, description="Snapshot interval in minutes.")
    averaging_window_hours: int = Field(default=24, description="Averaging window in hours.")
    averaging_window_min: Optional[int] = Field(default=None, description="Optional averaging window in minutes.")
    markets: Dict[str, set] = Field(default_factory=dict)
    candles_config: List[CandlesConfig] = Field(default_factory=list)


class SpreadSampler(StrategyV2Base):
    markets: ClassVar[Dict[str, set]] = {}

    def __init__(self, connectors: Optional[dict] = None, config: Optional[Any] = None):
        if connectors is None:
            connectors = {}
        if not isinstance(connectors, dict):
            try:
                connectors = dict(connectors)
            except Exception:
                connectors = {}

        # produce normalized configuration object
        def _normalize_data(config_dict: dict) -> dict:
            normalized_config = dict(config_dict)
            # connector_name from aliases
            if "connector_name" not in normalized_config or not normalized_config.get("connector_name"):
                for alias in ("exchange", "connector"):
                    if alias in normalized_config and normalized_config.get(alias):
                        normalized_config["connector_name"] = normalized_config.get(alias)
                        break
            # normalize quote to list uppercase
            quote = normalized_config.get("quote", None)
            if isinstance(quote, str):
                normalized_config["quote"] = [quote.strip().upper()]
            elif isinstance(quote, (list, tuple, set)):
                normalized_config["quote"] = [str(item).strip().upper() for item in quote if item is not None]
            else:
                normalized_config["quote"] = ["USDT"]
            # normalize markets values to lists of normalized pairs (BASE-QUOTE)
            markets = normalized_config.get("markets", {}) or {}
            normalize_markets: Dict[str, List[str]] = {}
            for exchange_name, market_list in markets.items():
                normalized_pairs: List[str] = []
                if isinstance(market_list, (list, tuple, set)):
                    market_entries = market_list
                elif market_list is None:
                    market_entries = []
                else:
                    market_entries = [market_list]
                for market_pair in market_entries:
                    if not isinstance(market_pair, str):
                        continue
                    normalized_pair = market_pair.replace("/", "-").replace("_", "-").upper()
                    normalized_pairs.append(normalized_pair)
                normalize_markets[exchange_name] = normalized_pairs
            normalized_config["markets"] = normalize_markets
            return normalized_config

        config_obj = None
        try:
            if isinstance(config, SpreadSamplerConfig):
                config_obj = config
            elif isinstance(config, dict):
                normalized_data = _normalize_data(config)
                try:
                    config_obj = SpreadSamplerConfig(**normalized_data)
                except Exception:
                    fallback_config = type("Cfg", (), {})()
                    for key, value in normalized_data.items():
                        setattr(fallback_config, key, value)
                    config_obj = fallback_config

        except Exception:
            config_obj = None

        if config_obj is None:
            try:
                config_obj = SpreadSamplerConfig()
            except Exception:
                raise ValueError("SpreadSampler requires a valid config object.")

        # call parent
        super().__init__(connectors, config_obj)

        self._started_at: Optional[float] = None
        self._last_snapshot_at: float = 0.0
        self._last_window_report: float = 0.0
        self._samples: Dict[str, List[dict]] = {}
        self._pairs: List[str] = []
        self._pairs_discovered: bool = False
        self._symbol_cache_timestamp: float = 0.0
        self._symbol_cache: List[str] = []

        # debug: show what config/connectors look like at init
        try:
            connector_keys = list(getattr(self, "connectors", {}).keys())
            connector_name = getattr(self.config, "connector_name", None)
            quote = getattr(self.config, "quote", None)
            market_keys = list(getattr(self.config, "markets", {}).keys()) if getattr(self.config, "markets", None) else []
            self._log_msg(f"[spread_sampler] debug init connector_keys={connector_keys} connector_name={connector_name} quote={quote} markets_keys={market_keys}")
        except Exception:
            pass

    def _now(self) -> float:
        return time.time()

    def _fetch_exchange_symbols(self) -> List[str]:
        """Fetch tradable symbols from the exchange (e.g., Binance) via REST API."""
        current_time = self._now()

    # Use cached symbols if fetched within last 5 minutes
        if self._symbol_cache and (current_time - self._symbol_cache_timestamp) < 300:
            return self._symbol_cache

        try:
            connector_name = (getattr(self.config, "connector_name", "") or "").lower()
            if "binance" not in connector_name:
                return []

            api_url = "https://api.binance.com/api/v3/exchangeInfo"
            with urllib.request.urlopen(api_url, timeout=8) as response:
                data = json.loads(response.read().decode())

            exchange_symbols = data.get("symbols") or []
            quote_assets = [quote.upper() for quote in (getattr(self.config, "quote", []) or [])]
            filtered_symbols: List[str] = []

            for symbol_entry in exchange_symbols:
                try:
                    if symbol_entry.get("status", "").upper() != "TRADING":
                        continue
                    quote_asset = (symbol_entry.get("quoteAsset") or "").upper()
                    base_asset = (symbol_entry.get("baseAsset") or "").upper()
                    if quote_asset in quote_assets:
                        filtered_symbols.append(f"{base_asset}-{quote_asset}")
                except Exception as e:
                    self._log_msg(f"[spread_sampler] symbol parsing error: {e}")
                    continue

            self._symbol_cache = sorted(set(filtered_symbols))
            self._symbol_cache_timestamp = current_time
            return self._symbol_cache

        except Exception as e:
            self._log_msg(f"[spread_sampler] failed to fetch exchange symbols: {e}")
            return []

    def _fetch_exchange_top(self, pair: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            connector_name = (getattr(self.config, "connector_name", "") or "").lower()
            if "binance" not in connector_name:
                return None, None
            symbol_name = pair.replace("-", "").replace("/", "").upper()
            api_url = f"https://api.binance.com/api/v3/depth?symbol={urllib.parse.quote(symbol_name)}&limit=5"
            with urllib.request.urlopen(api_url, timeout=6) as resp:
                data = json.loads(resp.read().decode())
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            best_bid = Decimal(str(bids[0][0])) if bids and len(bids[0]) >= 1 else None
            best_ask = Decimal(str(asks[0][0])) if asks and len(asks[0]) >= 1 else None
            return best_bid, best_ask
        except Exception:
            return None, None

    def _get_db_session(self) -> Optional[Session]:
        try:
            app = HummingbotApplication.main_application()
            sess = getattr(app, "db_session", None)
            if isinstance(sess, Session):
                return sess
            get_sess = getattr(app, "get_db_session", None)
            if callable(get_sess):
                s = get_sess()
                if isinstance(s, Session):
                    return s
        except Exception:
            pass
        try:
            db_path = os.path.join(os.getcwd(), "data", "spread_capture.sqlite")
            engine = create_engine(f"sqlite:///{db_path}", echo=False)
            HummingbotBase.metadata.bind = engine
            return sessionmaker(bind=engine)()
        except Exception as e:
            self._log_msg(f"[spread_sampler] DB engine creation failed: {e}")
            return None

    def _store_sample_to_db(self,
                            pair: str,
                            ts: float,
                            bid: Optional[Decimal],
                            ask: Optional[Decimal],
                            mid: Optional[Decimal],
                            spread: Optional[Decimal],
                            source: Optional[str]):
        session = self._get_db_session()
        if session is None:
            self._log_msg(f"[spread_sampler] no DB session available, skipping persist for {pair}")
            return

        created_local = False
        try:
            try:
                engine = HummingbotBase.metadata.bind
            except Exception:
                engine = None
            created_local = engine is not None and getattr(session, "bind", None) is not None

            bid_d = Decimal(str(bid)) if bid is not None else None
            ask_d = Decimal(str(ask)) if ask is not None else None
            mid_d = Decimal(str(mid)) if mid is not None else None
            spread_d = Decimal(str(spread)) if spread is not None else None

            SpreadSample.add(
                session=session,
                pair=pair,
                timestamp=int(ts),
                bid=bid_d,
                ask=ask_d,
                mid=mid_d,
                spread=spread_d,
                connector=getattr(self.config, "connector_name", None),
                source=source,
            )

            try:
                session.commit()
                self._log_msg(f"[spread_sampler] persisted sample {pair} ts={int(ts)}")
            except Exception as e:
                try:
                    session.rollback()
                except Exception:
                    pass
                self._log_msg(f"[spread_sampler] DB commit failed: {e}")
        finally:
            if created_local:
                try:
                    session.close()
                except Exception:
                    pass

    def _discover_pairs(self) -> List[str]:
        candidate_pairs = set()
        connectors_dict = getattr(self, "connectors", None)
        connector_instance = connectors_dict.get(getattr(self.config, "connector_name", None)) if connectors_dict else None
        if connector_instance is not None:
            order_books = getattr(connector_instance, "order_books", None)
            if isinstance(order_books, dict):
                candidate_pairs.update(order_books.keys())
            trading_pairs = getattr(connector_instance, "trading_pairs", None)
            if isinstance(trading_pairs, (list, set, tuple)):
                candidate_pairs.update(trading_pairs)

        # include configured seed markets (all keys)
        try:
            configured_markets = getattr(self.config, "markets", {}) or {}
            for market_entries in configured_markets.values():
                if isinstance(market_entries, (list, set, tuple)):
                    candidate_pairs.update(market_entries)
                elif isinstance(market_entries, str):
                    candidate_pairs.add(market_entries)
        except Exception:
            pass

        # REST fallback symbol list for known connectors
        rest_pairs: List[str] = []
        try:
            rest_pairs = self._fetch_exchange_symbols()
            candidate_pairs.update(rest_pairs)
        except Exception:
            pass

        # normalize to BASE-QUOTE form 'BASE-QUOTE' and filter by configured quotes
        normalized_pairs = set()
        quote_assets = [quote.upper() for quote in (self.config.quote or [])]
        for raw_pair in candidate_pairs:
            if not isinstance(raw_pair, str):
                continue
            normalized_pair = raw_pair.replace("/", "-").replace("_", "-").upper()
            match_found = False
            for quote_asset in quote_assets:
                if normalized_pair.endswith(f"-{quote_asset}"):
                    normalized_pairs.add(normalized_pair)
                    match_found = True
                    break
            if match_found:
                continue
            for quote_asset in quote_assets:
                if normalized_pair.endswith(quote_asset) and "-" not in normalized_pair:
                    base_asset = normalized_pair[:-len(quote_asset)]
                    if base_asset:
                        normalized_pairs.add(f"{base_asset}-{quote_asset}")
                        break

        # debug: report discovery metrics
        try:
            self._log_msg(f"[spread_sampler] debug discover connector_present={connector_instance is not None} connector_name={self.config.connector_name} seed_count={sum(len(entries) for entries in (self.config.markets or {}).values())} rest_count={len(rest_pairs)} normalized_count={len(normalized_pairs)}")
        except Exception:
            pass

        return sorted(normalized_pairs)

    def _sample_top(self, trading_pair: str) -> None:
        connectors_dict = getattr(self, "connectors", None)
        connector_instance = connectors_dict.get(self.config.connector_name) if connectors_dict else None
        bid = None
        ask = None
        data_source = "connector"
        try:
            if connector_instance is not None:
                order_books = getattr(connector_instance, "order_books", {})
                order_book_entry = order_books.get(trading_pair)
                if order_book_entry is None:
                    alt_keys = [trading_pair.replace("-", "/"), trading_pair.replace("-", "")]
                    for key in alt_keys:
                        order_book_entry = order_books.get(key)
                        if order_book_entry is not None:
                            break
                if order_book_entry:
                    if isinstance(order_book_entry, dict):
                        bids = order_book_entry.get("bids") or []
                        asks = order_book_entry.get("asks") or []
                        if bids:
                            bid = Decimal(str(bids[0][0])) if bids and len(bids[0]) >= 1 else None
                        if asks:
                            ask = Decimal(str(asks[0][0])) if asks and len(asks[0]) >= 1 else None
                    else:
                        try:
                            snapshot = getattr(order_book_entry, "get_snapshot", lambda: None)()
                            if snapshot:
                                bids = snapshot.get("bids") or []
                                asks = snapshot.get("asks") or []
                                if bids:
                                    bid = Decimal(str(bids[0][0]))
                                if asks:
                                    ask = Decimal(str(asks[0][0]))
                        except Exception:
                            pass

            if (bid is None or ask is None):
                fallback_bid, fallback_ask = self._fetch_exchange_top(trading_pair)
                if fallback_bid is not None or fallback_ask is not None:
                    self._log_msg(f"[spread_sampler] REST fallback {trading_pair} -> bid={fallback_bid} ask={fallback_ask}")
                    bid = bid or fallback_bid
                    ask = ask or fallback_ask
                    data_source = "rest"

            spread = None
            try:
                if bid is not None and ask is not None:
                    bid_d = Decimal(str(bid))
                    ask_d = Decimal(str(ask))
                    mid_d = (bid_d + ask_d) / Decimal("2")
                    if mid_d != Decimal("0"):
                        raw_spread = ((ask_d - bid_d) / mid_d) * Decimal("100")
                        spread = raw_spread.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                else:
                    mid_d = None
            except Exception:
                mid_d = None
                spread = None

            timestamp_now = self._now()
            self._samples.setdefault(trading_pair, []).append({"ts": timestamp_now, "bid": bid, "ask": ask, "spread": spread, "source": data_source})
            try:
                self._store_sample_to_db(pair=trading_pair, ts=timestamp_now, bid=bid, ask=ask, mid=mid_d, spread=spread, source=data_source)
            except Exception:
                pass
        except Exception:
            return

    def _take_snapshot(self) -> None:
        for trading_pair in self._pairs:
            self._sample_top(trading_pair)
        self._last_snapshot_at = self._now()
        for trading_pair in self._pairs:
            pair_samples = self._samples.get(trading_pair) or []
            if not pair_samples:
                continue
            last = pair_samples[-1]
            bid = last.get("bid")
            ask = last.get("ask")
            timestamp = last.get("ts")
            spread = last.get("spread")
            spread_text = f"{spread:.2f}%" if isinstance(spread, Decimal) else "None"
            self._log_msg(f"[spread_sampler] snapshot {trading_pair} ts={timestamp} bid={bid} ask={ask} spread={spread_text}")

    def _trim(self) -> None:
        window_min = self.config.averaging_window_min if (self.config.averaging_window_min is not None) else (self.config.averaging_window_hours * 60)
        cutoff = self._now() - (window_min * 60)
        for pair in list(self._samples.keys()):
            entries = self._samples.get(pair, [])
            self._samples[pair] = [e for e in entries if e.get("ts", 0) >= cutoff]
            if not self._samples[pair]:
                del self._samples[pair]

    def get_24h_report(self) -> List[Tuple[str, int, Decimal, Decimal, Decimal, Optional[Decimal], Optional[Decimal]]]:
        results = []
        for pair, entries in self._samples.items():
            if not entries:
                continue
            bids = [Decimal(str(e["bid"])) for e in entries if e.get("bid") is not None]
            asks = [Decimal(str(e["ask"])) for e in entries if e.get("ask") is not None]
            spreads = [e["spread"] for e in entries if isinstance(e.get("spread"), Decimal)]
            avg_bid = (sum(bids) / Decimal(len(bids))) if bids else Decimal("0")
            avg_ask = (sum(asks) / Decimal(len(asks))) if asks else Decimal("0")
            avg_mid = (avg_bid + avg_ask) / Decimal("2") if (avg_bid != Decimal("0") or avg_ask != Decimal("0")) else Decimal("0")
            avg_spread = None
            if spreads:
                try:
                    avg_spread = (sum(spreads) / Decimal(len(spreads))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                except Exception:
                    avg_spread = None
            current_mid = None
            results.append((pair, len(entries), avg_bid, avg_ask, avg_mid, current_mid, avg_spread))
        results.sort(key=lambda r: r[4], reverse=True)
        return results

    def _log_msg(self, msg: str) -> None:
        try:
            app = HummingbotApplication.main_application()
            app_logger = getattr(app, "logger", None)
            if app_logger and hasattr(app_logger, "info"):
                app_logger.info(msg)
                return
        except Exception:
            pass
        try:
            logger_attr = getattr(self, "logger", None)
            logger_instance = logger_attr() if callable(logger_attr) else logger_attr
            if logger_instance and hasattr(logger_instance, "info"):
                logger_instance.info(msg)
                return
        except Exception:
            pass
        print(msg)

    def on_tick(self):
        now = self._now()
        if self._started_at is None:
            self._started_at = now
            self._last_window_report = now

        if not self._discovered:
            self._pairs = self._discover_pairs()
            if self._pairs:
                self._discovered = True
                self._log_msg(f"[spread_sampler] discovered pairs: {self._pairs}")
                self._take_snapshot()
                self._trim()

        interval_s = max(1, int(self.config.snapshot_interval_min)) * 60
        if now - self._last_snapshot_at >= interval_s:
            self._take_snapshot()
            self._trim()

        window_min = self.config.averaging_window_min if (self.config.averaging_window_min is not None) else (self.config.averaging_window_hours * 60)
        window_s = window_min * 60
        if now - self._last_window_report >= window_s:
            cutoff = now - window_s
            pairs_contributed = 0
            for p in self._pairs:
                entries = self._samples.get(p) or []
                spreads = [e["spread"] for e in entries if isinstance(e.get("spread"), Decimal) and e.get("ts", 0) >= cutoff]
                if not spreads:
                    continue
                try:
                    avg_sp = (sum(spreads) / Decimal(len(spreads))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                except Exception:
                    continue
                pairs_contributed += 1
                self._log_msg(f"[spread_sampler] window_avg_spread {p} = {avg_sp:.2f}% samples={len(spreads)} window_min={window_min}")
            self._log_msg(f"[spread_sampler] window_spread_report pairs_contributing={pairs_contributed} window_min={window_min}")
            self._last_window_report = now


def print_24h_report(limit: int = 50) -> None:
    app = HummingbotApplication.main_application()
    strategy = getattr(app, "strategy", None)
    if strategy is None or not hasattr(strategy, "get_24h_report"):
        print("No running strategy with get_24h_report() found.")
        return
    report = strategy.get_24h_report()
    if not report:
        print("No samples available.")
        return
    for pair, count, avg_bid, avg_ask, avg_mid, current_mid, avg_spread in report[:limit]:
        sp_text = f"{avg_spread:.2f}%" if isinstance(avg_spread, Decimal) else "None"
        print(f"{pair} | samples={count} | avg_bid={avg_bid} | avg_ask={avg_ask} | avg_mid={avg_mid} | current_mid={current_mid} | spread={sp_text}")
