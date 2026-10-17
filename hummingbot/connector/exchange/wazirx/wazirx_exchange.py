import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
from bidict import bidict
from yarl import URL

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import WazirxAPIUserStreamDataSource
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferState,
    TransferType,
    TransferUpdate,
    WalletTransfer,
)
from hummingbot.connector.wallet_transfer.wallet_transfer_executor import WalletTransferExecutorMixin
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class WazirxExchange(WalletTransferExecutorMixin, ExchangePyBase):
    """
    WazirX exchange connector for spot trading.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    # Wallet-transfer capabilities (see WalletTransferExecutorMixin)
    supports_sub_to_master_transfer = True
    supports_master_to_sub_transfer = True
    # External transfers: WazirX withdraws only to Address Book entries whitelisted out of band
    # (there is no API to add one), so the destination is an address-book id/name, not an address.
    supports_withdrawal = True
    supports_deposit_address = True
    requires_whitelisted_address = True
    # The withdraw-history endpoint is heavily rate limited (429 / 2136) and `_wazirx_request` is
    # not throttled, so poll withdrawal status conservatively. ERC20 settles in minutes anyway.
    TRANSFER_STATUS_POLL_INTERVAL = 30.0

    def __init__(self,
                 wazirx_api_key: str,
                 wazirx_api_secret: str,
                 wazirx_master_api_key: Optional[str] = None,
                 wazirx_master_api_secret: Optional[str] = None,
                 wazirx_master_email: Optional[str] = None,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        """
        Initialize the WazirX exchange connector.
        """
        self.api_key = wazirx_api_key
        self.secret_key = wazirx_api_secret
        self._master_api_key = wazirx_master_api_key or None
        self._master_api_secret = wazirx_master_api_secret or None
        self._master_email = wazirx_master_email or None
        self._master_auth: Optional[WazirxAuth] = None
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)

        if trading_required and trading_pairs:
            for pair in trading_pairs:
                parts = pair.split("-")
                if len(parts) == 2:
                    for asset in parts:
                        self._account_balances[asset] = Decimal("0")
                        self._account_available_balances[asset] = Decimal("0")

    @property
    def authenticator(self):
        return WazirxAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "wazirx"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL)
        return pairs_prices

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, str]]:
        if not trading_pairs:
            return await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL)
        results = []
        for tp in trading_pairs:
            base, quote = tp.split("-", 1)
            symbol = f"{base.lower()}{quote.lower()}"
            try:
                resp = await self._api_get(
                    path_url=CONSTANTS.TICKER_24HR_PATH_URL,
                    params={"symbol": symbol},
                )
                if isinstance(resp, dict):
                    results.append(resp)
                elif isinstance(resp, list):
                    results.extend(resp)
            except Exception:
                self.logger().warning(f"Skipping {tp}: symbol not found on {self.name}")
        return results

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_msg = str(request_exception)
        return "2098" in error_msg or "out of receiving window" in error_msg.lower()

    @staticmethod
    def _is_rate_limited(exception: Exception) -> bool:
        """True for WazirX's rate-limit error (HTTP 429 / code 2136 "Too many api request")."""
        text = str(exception).lower()
        return "2136" in text or "too many api request" in text or "429" in text

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "Order does not exist" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "Order does not exist" in str(cancelation_exception)

    def _is_user_stream_initialized(self):
        return True

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return WazirxAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return WazirxAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def _wazirx_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        auth: Optional[WazirxAuth] = None,
        params_in_query: bool = False,
    ) -> Dict[str, Any]:
        """
        Make an authenticated or unauthenticated request to the WazirX API.

        :param auth: authenticator to sign the request with; defaults to the connector's own
            credentials. Pass the master authenticator for sub-account/master transfers.
        :param params_in_query: when True, signed params are sent in the URL query string instead
            of the request body. WazirX validates the signature against the query string exactly as
            received, so the URL must be sent verbatim (``encoded=True``) to keep percent-encoded
            values such as ``%2B`` (a ``+`` in a sub-account email) intact. Required for the
            sub-account fund-transfer endpoint, which rejects body-form params with
            "Signature is incorrect" (code 2005).
        """
        url = f"{CONSTANTS.REST_URL}{path}"
        params = params or {}

        async with aiohttp.ClientSession() as session:
            if is_auth_required:
                auth: WazirxAuth = auth or self._auth
                _, query_string = await auth.add_auth_params(params)
                headers = auth.get_headers()
                method_upper = method.upper()

                # GET always carries params in the (verbatim-encoded) query string.
                if method_upper == "GET" or params_in_query:
                    signed_url = URL(f"{url}?{query_string}", encoded=True)
                    if method_upper == "GET":
                        request_ctx = session.get(signed_url, headers=headers)
                    elif method_upper == "POST":
                        request_ctx = session.post(signed_url, headers=headers)
                    elif method_upper == "DELETE":
                        request_ctx = session.delete(signed_url, headers=headers)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")
                    async with request_ctx as response:
                        return await self._handle_response(response, method, url)
                elif method_upper == "POST":
                    async with session.post(url, data=query_string, headers=headers) as response:
                        return await self._handle_response(response, method, url)
                elif method_upper == "DELETE":
                    async with session.delete(url, data=query_string, headers=headers) as response:
                        return await self._handle_response(response, method, url)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
            else:
                headers = {}
                if method.upper() == "GET":
                    async with session.get(url, params=params, headers=headers) as response:
                        return await self._handle_response(response, method, url)
                elif method.upper() == "POST":
                    body = urlencode(params)
                    async with session.post(url, data=body, headers=headers) as response:
                        return await self._handle_response(response, method, url)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

    async def _handle_response(self, response: aiohttp.ClientResponse, method: str, url: str) -> Dict[str, Any]:
        if response.status >= 400:
            error_text = await response.text()
            raise IOError(f"Error executing request {method} {url}. HTTP status is {response.status}. Error: {error_text}")
        return await response.json()

    # ------------------------------------------------------------------
    # Wallet transfer support
    # ------------------------------------------------------------------
    @property
    def _master_authenticator(self) -> WazirxAuth:
        if self._master_auth is None:
            self._master_auth = WazirxAuth(
                api_key=self._master_api_key,
                secret_key=self._master_api_secret,
                time_provider=self._time_synchronizer,
            )
        return self._master_auth

    def _verify_master_credentials(self) -> None:
        if not self._master_api_key or not self._master_api_secret:
            raise ValueError(
                "WazirX master account API key and secret are required for sub-account to master "
                "transfers. Configure wazirx_master_api_key and wazirx_master_api_secret."
            )

    async def _place_internal_transfer(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        """
        Transfer funds between the master account and a sub-account (either direction) using the
        master account API key. WazirX identifies accounts by email and only allows moves between
        the master and its own sub-accounts (sub->sub is rejected).
        """
        if transfer.transfer_type == TransferType.MASTER_TO_SUB:
            # The sub-account email is the destination; the master side defaults from config.
            transfer.source = transfer.source or self._master_email
            missing_side = "from_account" if not transfer.source else None
        else:
            # SUB_TO_MASTER: the sub-account email is the source; master side defaults from config.
            transfer.destination = transfer.destination or self._master_email
            missing_side = "to_account" if not transfer.destination else None
        if missing_side:
            raise ValueError(
                f"A WazirX master account email is required (set wazirx_master_email or pass "
                f"{missing_side}) for {transfer.transfer_type.value} transfers."
            )

        params = {
            "currency": transfer.asset.lower(),
            "amount": f"{transfer.amount:f}",
            "fromEmail": transfer.source,
            "toEmail": transfer.destination,
        }
        resp = await self._wazirx_request(
            method="POST",
            path=CONSTANTS.SUB_ACCOUNT_FUND_TRANSFER_PATH_URL,
            params=params,
            is_auth_required=True,
            auth=self._master_authenticator,
            params_in_query=True,
        )

        status = str(resp.get("status", "")).lower()
        txn_id = resp.get("txnId")
        if status and status != "success":
            raise IOError(f"WazirX rejected the transfer: {resp}")

        return TransferUpdate(
            client_transfer_id=transfer.client_transfer_id,
            new_state=TransferState.COMPLETED,
            update_timestamp=self.current_timestamp,
            exchange_transfer_id=str(txn_id) if txn_id is not None else None,
        )

    async def get_sub_account_transfer_history(self, **query_params: Any) -> Any:
        """
        Fetch the sub-account fund-transfer history from WazirX. Requires master credentials.

        Any WazirX-supported filters (e.g. ``limit``) can be passed as keyword arguments; the
        ``timestamp``/``recvWindow``/``signature`` params are added automatically by the
        authenticator. Returns the raw response (a list of transfer records).
        """
        self._verify_master_credentials()
        return await self._wazirx_request(
            method="GET",
            path=CONSTANTS.SUB_ACCOUNT_FUND_TRANSFER_HISTORY_PATH_URL,
            params=dict(query_params),
            is_auth_required=True,
            auth=self._master_authenticator,
        )

    # ------------------------------------------------------------------
    # External transfers (crypto leaves / enters the exchange)
    #
    # These use the account's OWN credentials (not the master key) and are a separate category
    # from the sub-account/master transfers above: they settle on-chain, so they are reported as
    # SUBMITTED and confirmed later via the withdraw-history poll.
    # ------------------------------------------------------------------
    async def get_coins_info(self) -> Any:
        """GET /sapi/v1/coins - deposit/withdraw metadata per coin, including ``networkList``."""
        return await self._wazirx_request(
            method="GET", path=CONSTANTS.COINS_PATH_URL, is_auth_required=True,
        )

    async def get_address_book(self, coin: str, **query_params: Any) -> Any:
        """
        GET /sapi/v1/crypto/withdraw/address-book - the whitelisted withdrawal destinations.

        Returned addresses are MASKED (e.g. ``tb1p5cyxnu*****mvzv``), so entries are selected by
        ``id`` or ``name`` rather than by matching a full address.
        """
        params = {"coin": coin.lower(), **query_params}
        return await self._wazirx_request(
            method="GET", path=CONSTANTS.CRYPTO_WITHDRAW_ADDRESS_BOOK_PATH_URL,
            params=params, is_auth_required=True,
        )

    async def get_withdraw_history(self, **query_params: Any) -> Any:
        """GET /sapi/v1/crypto/withdraws - withdrawal records (filterable by ``withdrawOrderId``)."""
        return await self._wazirx_request(
            method="GET", path=CONSTANTS.CRYPTO_WITHDRAWS_PATH_URL,
            params=dict(query_params), is_auth_required=True,
        )

    async def _request_deposit_address(self, asset: str, network: Optional[str] = None) -> Dict[str, Any]:
        """GET /sapi/v1/crypto/deposits/address - the address to deposit ``asset`` INTO WazirX."""
        if not network:
            raise ValueError("WazirX requires a network to look up a deposit address (e.g. 'matic').")
        return await self._wazirx_request(
            method="GET", path=CONSTANTS.CRYPTO_DEPOSITS_ADDRESS_PATH_URL,
            params={"coin": asset.lower(), "network": network}, is_auth_required=True,
        )

    async def _resolve_address_book_entry(self, coin: str, address_book_id: str) -> Dict[str, Any]:
        """
        Find the Address Book entry matching ``address_book_id`` (its numeric ``id`` or its
        ``name``) and verify it is currently withdrawable.
        """
        entries = await self.get_address_book(coin)
        if isinstance(entries, dict):
            entries = entries.get("rows") or entries.get("data") or []
        wanted = str(address_book_id).strip().lower()
        match = next(
            (
                entry for entry in entries
                if isinstance(entry, dict)
                and wanted in {str(entry.get("id", "")).lower(), str(entry.get("name", "")).strip().lower()}
            ),
            None,
        )
        if match is None:
            available = [
                f"{e.get('id')}:{e.get('name')!r}[{(e.get('networkObj') or {}).get('network')}]"
                for e in entries if isinstance(e, dict)
            ]
            raise ValueError(
                f"No WazirX address-book entry for {coin.upper()} matching {address_book_id!r}. "
                f"Whitelist it in the WazirX app first. Available: {available or 'none'}"
            )
        if match.get("disabled") is True:
            raise ValueError(
                f"WazirX address-book entry {match.get('id')} ({match.get('name')!r}) is disabled."
            )
        if match.get("isWithdrawAllowed") is False:
            raise ValueError(
                f"WazirX address-book entry {match.get('id')} ({match.get('name')!r}) is not yet "
                f"withdrawable (isWithdrawAllowed=false, withdrawAllowedAfter="
                f"{match.get('withdrawAllowedAfter')})."
            )
        return match

    async def _get_coin_record(self, coin: str) -> Optional[Dict[str, Any]]:
        """
        Find the /sapi/v1/coins record for ``coin``.

        Verified live: the response is a flat list and each record keys the asset as ``currency``
        (NOT ``coin``, which the docs' sample implies). One fetch feeds the consent string, the
        enablement flags and the amount limits — /sapi/v1/coins is rate limited (~5/min), so
        callers should reuse the record rather than re-fetching.
        """
        coins = await self.get_coins_info()
        records = coins if isinstance(coins, list) else [coins]
        wanted = coin.lower()
        for record in records:
            if not isinstance(record, dict):
                continue
            if any(str(record.get(key, "")).lower() == wanted for key in ("currency", "coin", "symbol")):
                return record
        return None

    @staticmethod
    def _consent_message(network_entry: Dict[str, Any]) -> str:
        """
        Extract the mandatory ``withdrawConsent`` string.

        Verified live as ``{"helpUrl": ..., "message": "I confirm that this withdrawal ..."}``;
        a plain string is also accepted defensively.
        """
        consent = network_entry.get("withdrawConsent")
        if isinstance(consent, dict):
            return str(consent.get("message") or "")
        return str(consent or "")

    async def _resolve_withdraw_network(self, coin: str, network: Optional[str] = None) -> Dict[str, Any]:
        """
        Pick the ``networkList`` entry to withdraw over, and verify WazirX will actually accept it.

        Withdrawals are gated at two levels — the coin (``withdrawDetails.disabled``) and the
        network (``withdrawEnable``). Both were observed disabled ("Crypto withdrawals are disabled
        for maintenance"), so fail fast with the exchange's own wording rather than submitting a
        request that cannot succeed.
        """
        record = await self._get_coin_record(coin)
        if record is None:
            raise ValueError(f"WazirX does not list coin {coin.upper()}.")

        details = record.get("withdrawDetails") or {}
        if details.get("disabled"):
            message = (details.get("disabledMessage") or {}).get("description") \
                or "Crypto withdrawals are disabled"
            raise ValueError(f"WazirX: {message} (coin {coin.upper()}).")

        networks = [n for n in (record.get("networkList") or []) if isinstance(n, dict)]
        if not networks:
            raise ValueError(f"WazirX lists no withdrawal networks for {coin.upper()}.")

        if network:
            entry = next(
                (n for n in networks if str(n.get("network", "")).lower() == network.lower()), None
            )
            if entry is None:
                raise ValueError(
                    f"Unknown WazirX network {network!r} for {coin.upper()}. "
                    f"Available: {[n.get('network') for n in networks]}"
                )
        else:
            entry = next((n for n in networks if n.get("isDefault")), networks[0])

        if entry.get("withdrawEnable") is False:
            description = (entry.get("withdrawDesc") or {}).get("description") \
                or "withdrawals are disabled on this network"
            raise ValueError(
                f"WazirX network {entry.get('network')!r} ({entry.get('name')}) "
                f"for {coin.upper()}: {description}"
            )
        return entry

    @staticmethod
    def _validate_withdraw_amount(amount: Decimal, network_entry: Dict[str, Any], coin: str) -> None:
        """Enforce the network's published min/max before submitting."""
        minimum = network_entry.get("minWithdrawAmount")
        maximum = network_entry.get("maxWithdrawAmount")
        network = network_entry.get("network")
        if minimum not in (None, "") and amount < Decimal(str(minimum)):
            raise ValueError(
                f"WazirX minimum withdrawal for {coin.upper()} on {network} is {minimum} "
                f"(requested {amount})."
            )
        if maximum not in (None, "") and amount > Decimal(str(maximum)):
            raise ValueError(
                f"WazirX maximum withdrawal for {coin.upper()} on {network} is {maximum} "
                f"(requested {amount})."
            )

    async def _place_withdrawal(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        """
        POST /sapi/v1/crypto/withdraw - withdraw to a whitelisted Address Book destination.

        Returns SUBMITTED (not COMPLETED): the withdrawal is only accepted here, and is confirmed
        later by ``_request_transfer_status`` polling the withdraw history.
        """
        coin = transfer.asset.lower()
        entry = await self._resolve_address_book_entry(coin, transfer.address_book_id)
        # Record what we actually resolved so the tracked transfer is self-describing.
        transfer.address_book_id = str(entry.get("id"))
        transfer.address = transfer.address or entry.get("address")

        # An address-book entry is bound to ONE network (`networkObj`), e.g. a TRC20 address can
        # only be paid over `trx`. Take the network from the entry rather than guessing a default,
        # and refuse a caller-supplied network that contradicts it — sending an ERC20 withdrawal to
        # a Tron address would lose the funds.
        entry_network = (entry.get("networkObj") or {}).get("network")
        if entry_network:
            if transfer.network and transfer.network.lower() != str(entry_network).lower():
                raise ValueError(
                    f"WazirX address-book entry {entry.get('id')} ({entry.get('name')!r}) is bound to "
                    f"network {entry_network!r}, but {transfer.network!r} was requested."
                )
            transfer.network = str(entry_network)

        # One /sapi/v1/coins fetch drives enablement, the consent string and the amount limits.
        network_entry = await self._resolve_withdraw_network(coin, transfer.network)
        transfer.network = transfer.network or network_entry.get("network")
        self._validate_withdraw_amount(transfer.amount, network_entry, coin)

        consent = kwargs.get("withdraw_consent") or self._consent_message(network_entry)
        if not consent:
            raise ValueError(
                f"WazirX did not return a withdrawConsent message for {coin.upper()} on "
                f"{network_entry.get('network')}; refusing to submit without it."
            )
        params = {
            "coin": coin,
            "addressBookId": entry.get("id"),
            "amount": f"{transfer.amount:f}",
            "withdrawConsent": consent,
            "extra": kwargs.get("extra", "hummingbot wallet transfer"),
            "withdrawOrderId": transfer.client_transfer_id,
        }
        resp = await self._wazirx_request(
            method="POST",
            path=CONSTANTS.CRYPTO_WITHDRAW_PATH_URL,
            params=params,
            is_auth_required=True,
            params_in_query=True,
        )

        withdraw_id = resp.get("id") if isinstance(resp, dict) else None
        if withdraw_id is None:
            raise IOError(f"WazirX rejected the withdrawal: {resp}")

        return TransferUpdate(
            client_transfer_id=transfer.client_transfer_id,
            new_state=TransferState.SUBMITTED,
            update_timestamp=self.current_timestamp,
            exchange_transfer_id=str(withdraw_id),
        )

    async def _request_transfer_status(self, transfer: WalletTransfer) -> TransferUpdate:
        """
        Poll the withdraw history for this withdrawal and map its status.

        WazirX documents ``status`` as an INT naming Success/Failed/Pending/Cancelled without
        publishing the numeric mapping, so both the numeric and string forms are matched and
        anything unrecognised is treated as still pending.

        The withdraw-history endpoint is aggressively rate limited (429 / code 2136) and
        ``_wazirx_request`` bypasses the throttler, so a rate-limit hit is treated as a transient
        "still pending" rather than an error — the loop simply retries on the next (spaced) tick.
        """
        try:
            history = await self.get_withdraw_history(withdrawOrderId=transfer.client_transfer_id)
        except IOError as exception:
            if self._is_rate_limited(exception):
                return TransferUpdate(
                    client_transfer_id=transfer.client_transfer_id,
                    new_state=TransferState.SUBMITTED,
                    update_timestamp=self.current_timestamp,
                )
            raise
        rows = history if isinstance(history, list) else (history or {}).get("rows", [])
        record = next(
            (r for r in rows if isinstance(r, dict)
             and str(r.get("withdrawOrderId", "")) == transfer.client_transfer_id),
            None,
        )
        if record is None:
            # Not visible yet - stay SUBMITTED; the polling loop will retry until it times out.
            return TransferUpdate(
                client_transfer_id=transfer.client_transfer_id,
                new_state=TransferState.SUBMITTED,
                update_timestamp=self.current_timestamp,
            )

        status = str(record.get("status", "")).strip().lower()
        # Verified live: the field is `txid` (lowercase). The docs' sample showed `txId`; accept both.
        tx_hash = record.get("txid") or record.get("txId") or record.get("txHash")
        if status in CONSTANTS.WITHDRAW_STATUS_SUCCESS:
            new_state = TransferState.COMPLETED
        elif status in CONSTANTS.WITHDRAW_STATUS_FAILED:
            new_state = TransferState.FAILED
        else:
            new_state = TransferState.SUBMITTED

        return TransferUpdate(
            client_transfer_id=transfer.client_transfer_id,
            new_state=new_state,
            update_timestamp=self.current_timestamp,
            exchange_transfer_id=str(record.get("id")) if record.get("id") else None,
            tx_hash=tx_hash,
            misc_updates=(
                {"error_message": record.get("failureInfo"), "error_type": "WithdrawalFailed"}
                if new_state is TransferState.FAILED else None
            ),
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Place an order on the WazirX exchange.
        """
        symbol = trading_pair.replace("-", "").lower()

        wazirx_order_type = "limit" if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER] else order_type.name.lower()

        params = {
            "symbol": symbol,
            "side": trade_type.name.lower(),
            "type": wazirx_order_type,
            "quantity": f"{amount:f}",
        }
        if order_type.is_limit_type():
            params["price"] = f"{price:f}"

        try:
            resp = await self._wazirx_request(
                method="POST",
                path=CONSTANTS.CREATE_ORDER_PATH_URL,
                params=params,
                is_auth_required=True
            )
            order_id = str(resp.get("id", resp.get("orderId", "")))
            executed_amount = float(resp.get("executedQty", 0))
            return order_id, executed_amount
        except Exception:
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = tracked_order.trading_pair.replace("-", "").lower()

        exchange_order_id = tracked_order.exchange_order_id
        if not exchange_order_id:
            self.logger().warning(f"Cannot cancel order {order_id}: no exchange order ID available")
            return False

        params = {
            "symbol": symbol,
            "orderId": exchange_order_id,
        }
        try:
            resp = await self._wazirx_request(
                method="DELETE",
                path=CONSTANTS.CANCEL_ORDER_PATH_URL,
                params=params,
                is_auth_required=True
            )
            return resp.get("id") is not None or resp.get("orderId") is not None
        except Exception as e:
            self.logger().warning(f"Cancel order {order_id} failed: {e}")
            return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info
        """
        if isinstance(exchange_info_dict, list):
            trading_pair_rules = exchange_info_dict
        else:
            trading_pair_rules = exchange_info_dict.get("symbols", [])

        retval: List[TradingRule] = []

        for rule in trading_pair_rules:
            try:
                symbol = rule.get("symbol", "")
                if not symbol:
                    continue

                base_asset = rule.get("baseAsset", "")
                quote_asset = rule.get("quoteAsset", "")
                if not base_asset or not quote_asset:
                    continue

                hb_trading_pair = f"{base_asset.upper()}-{quote_asset.upper()}"

                filters = rule.get("filters", [])
                price_filter = next((f for f in filters if f.get("filterType") == "PRICE_FILTER"), {})
                lot_size_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), {})
                min_notional_filter = next(
                    (f for f in filters if f.get("filterType") in ["MIN_NOTIONAL", "NOTIONAL"]),
                    {},
                )

                try:
                    min_order_size = Decimal(lot_size_filter.get("minQty", "1e-8"))
                except Exception:
                    min_order_size = Decimal("1e-8")

                try:
                    max_order_size = Decimal(lot_size_filter.get("maxQty", "1e8"))
                except Exception:
                    max_order_size = Decimal("1e8")

                try:
                    tick_size = Decimal(price_filter.get("tickSize", "1e-8"))
                except Exception:
                    tick_size = Decimal("1e-8")

                try:
                    step_size = Decimal(lot_size_filter.get("stepSize", "1e-8"))
                except Exception:
                    step_size = Decimal("1e-8")

                try:
                    min_notional = Decimal(min_notional_filter.get("minNotional", "0"))
                except Exception:
                    min_notional = Decimal("0")

                trading_rule = TradingRule(
                    trading_pair=hb_trading_pair,
                    min_order_size=min_order_size,
                    max_order_size=max_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    min_quote_amount_increment=step_size,
                    min_notional_size=min_notional,
                )
                retval.append(trading_rule)
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")

        return retval

    async def _update_trading_fees(self):
        return

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event")
                if event_type == "orderUpdate":
                    order_data = event_message.get("order", {})
                    client_order_id = order_data.get("clientOrderId")
                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        new_state = CONSTANTS.ORDER_STATE.get(
                            order_data.get("status"),
                            OrderState.OPEN,
                        )
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message.get("timestamp", 0) / 1000,
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_data.get("orderId", "")),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "balanceUpdate":
                    balance_data = event_message.get("balance", {})
                    asset_name = balance_data.get("asset")
                    if asset_name is not None:
                        free_balance = Decimal(balance_data.get("free", "0"))
                        locked_balance = Decimal(balance_data.get("locked", "0"))
                        total_balance = free_balance + locked_balance
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

                else:
                    continue

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates: List[TradeUpdate] = []

        if order.current_state in [OrderState.FAILED, OrderState.CANCELED]:
            return trade_updates

        if order.exchange_order_id is not None:
            symbol = order.trading_pair.replace("-", "").lower()
            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id,
            }

            try:
                resp = await self._wazirx_request(
                    method="GET",
                    path=CONSTANTS.MY_TRADES_PATH_URL,
                    params=params,
                    is_auth_required=True
                )

                trades = resp if isinstance(resp, list) else resp.get("trades", [])

                for trade in trades:
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=trade.get("feeCurrency", trade.get("commissionAsset", "")),
                        flat_fees=[
                            TokenAmount(
                                amount=Decimal(trade.get("fee", trade.get("commission", "0"))),
                                token=trade.get("feeCurrency", trade.get("commissionAsset", "")),
                            )
                        ],
                    )

                    trade_update = TradeUpdate(
                        trade_id=str(trade.get("id", "")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(trade.get("orderId", "")),
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(trade.get("qty", "0")),
                        fill_quote_amount=Decimal(trade.get("quoteQty", "0")),
                        fill_price=Decimal(trade.get("price", "0")),
                        fill_timestamp=trade.get("time", 0) / 1000,
                    )
                    trade_updates.append(trade_update)
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "Too many" in error_msg:
                    self.logger().warning(f"Rate limit hit while fetching trade updates for order {order.client_order_id}. Will retry later.")
                else:
                    self.logger().error(f"Error fetching trade updates for order {order.client_order_id}: {e}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.current_state in [OrderState.FAILED, OrderState.CANCELED]:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=tracked_order.last_update_timestamp,
                new_state=tracked_order.current_state,
            )

        symbol = tracked_order.trading_pair.replace("-", "").lower()
        params = {
            "symbol": symbol,
            "orderId": tracked_order.exchange_order_id,
        }

        resp = await self._wazirx_request(
            method="GET",
            path=CONSTANTS.ORDER_STATUS_PATH_URL,
            params=params,
            is_auth_required=True
        )

        new_state = CONSTANTS.ORDER_STATE.get(resp.get("status"), OrderState.OPEN)
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(resp.get("id", resp.get("orderId", ""))),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=resp.get("updatedTime", resp.get("updateTime", 0)) / 1000,
            new_state=new_state,
        )
        return order_update

    async def _update_balances(self):
        try:
            resp = await self._wazirx_request(
                method="GET",
                path=CONSTANTS.USER_BALANCES_PATH_URL,
                is_auth_required=True
            )

            if isinstance(resp, list):
                balances = resp
            else:
                balances = resp.get("balances", [])

            for balance_entry in balances:
                asset_name = balance_entry.get("asset", "").upper()
                free_balance = Decimal(balance_entry.get("free", "0"))
                total_balance = Decimal(balance_entry.get("free", "0")) + Decimal(balance_entry.get("locked", "0"))
                self._account_available_balances[asset_name] = free_balance
                self._account_balances[asset_name] = total_balance
        except Exception as e:
            self.logger().warning(f"Error updating balances (will retry): {e}")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        if isinstance(exchange_info, list):
            symbols_data = exchange_info
        else:
            symbols_data = exchange_info.get("symbols", [])

        for symbol_data in symbols_data:
            symbol = symbol_data.get("symbol", "").lower()
            base_asset = symbol_data.get("baseAsset", "")
            quote_asset = symbol_data.get("quoteAsset", "")
            if symbol and base_asset and quote_asset:
                hb_trading_pair = f"{base_asset.upper()}-{quote_asset.upper()}"
                mapping[symbol] = hb_trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        result = {}

        for trading_pair in trading_pairs:
            try:
                symbol = trading_pair.replace("-", "").lower()
                url = f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_24HR_PATH_URL}"
                params = {"symbol": symbol}

                ra = await self._web_assistants_factory.get_rest_assistant()
                resp = await ra.execute_request(url=url, method=RESTMethod.GET, params=params, throttler_limit_id=CONSTANTS.TICKERS_PATH_URL)

                if isinstance(resp, dict):
                    last_price = float(resp.get("lastPrice", "0"))
                    result[trading_pair] = last_price

            except Exception as e:
                self.logger().error(f"Error fetching last traded price for {trading_pair}: {e}")
                result[trading_pair] = 0.0

        return result
