import hashlib
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from urllib.parse import unquote_plus, urlencode

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_auth import CoinswitchPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

# A throwaway Ed25519 key pair (32-byte secret, hex), never used anywhere real.
SECRET_HEX = hashlib.sha256(b"coinswitch-perpetual-auth-test").hexdigest()
PRIVATE_KEY = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SECRET_HEX))
API_KEY = PRIVATE_KEY.public_key().public_bytes_raw().hex()


class _FixedTime:
    def __init__(self, now: float = 1_719_905_777.483):
        self.now = now

    def time(self) -> float:
        return self.now


def _reference_message(method, path, params, epoch):
    """The reference client's signing: METHOD + unquote_plus(path?urlencode(params)) + epoch."""
    if params:
        path = path + "?" + urlencode(params)
    return method.upper() + unquote_plus(path) + epoch


class CoinswitchPerpetualAuthTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.clock = _FixedTime()
        self.auth = CoinswitchPerpetualAuth(api_key=API_KEY, secret_key=SECRET_HEX, time_provider=self.clock)

    def _verify(self, signature_hex: str, message: str):
        # Raises InvalidSignature when the bytes differ.
        PRIVATE_KEY.public_key().verify(bytes.fromhex(signature_hex), message.encode())

    def test_docs_signature_examples(self):
        self.assertEqual("GET/trade/api/v2/time1719905777483",
                         CoinswitchPerpetualAuth.signature_message("GET", "/trade/api/v2/time", None, "1719905777483"))
        self.assertEqual(
            "GET/trade/api/v2/orders?open=true&exchanges=coinswitchx,c2c11719905777483",
            CoinswitchPerpetualAuth.signature_message(
                "GET", "/trade/api/v2/orders", {"open": "true", "exchanges": "coinswitchx,c2c1"}, "1719905777483"))

    async def test_get_signs_the_sorted_decoded_query_and_sends_it_in_that_order(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://coinswitch.co/trade/api/v2/futures/transactions",
            params={"symbol": "BTCUSDT", "exchange": "EXCHANGE_2", "type": "funding fee", "limit": 20},
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)

        self.assertEqual(["exchange", "limit", "symbol", "type"], list(signed.params))
        self.assertEqual("20", signed.params["limit"])
        epoch = signed.headers["X-AUTH-EPOCH"]
        self.assertEqual("1719905777483", epoch)
        message = _reference_message("GET", "/trade/api/v2/futures/transactions", signed.params, epoch)
        self.assertEqual(
            "GET/trade/api/v2/futures/transactions?exchange=EXCHANGE_2&limit=20&symbol=BTCUSDT&type=funding fee"
            "1719905777483", message)
        self._verify(signed.headers["X-AUTH-SIGNATURE"], message)
        self.assertEqual(API_KEY, signed.headers["X-AUTH-APIKEY"])
        self.assertEqual("application/json", signed.headers["Content-Type"])

    async def test_post_body_is_not_signed_and_numbers_stay_plain(self):
        body = json.dumps({"exchange": "EXCHANGE_2", "symbol": "SHIBUSDT", "price": 0.00001234, "quantity": 22.0,
                           "reduce_only": True})
        self.assertIn("1.234e-05", body)
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://coinswitch.co/trade/api/v2/futures/order",
            data=body,
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)

        self._verify(signed.headers["X-AUTH-SIGNATURE"], "POST/trade/api/v2/futures/order" + signed.headers["X-AUTH-EPOCH"])
        self.assertEqual(
            '{"exchange":"EXCHANGE_2","symbol":"SHIBUSDT","price":0.00001234,"quantity":22.0,"reduce_only":true}',
            signed.data)
        self.assertEqual(json.loads(body), json.loads(signed.data))

    async def test_delete_with_body(self):
        request = RESTRequest(
            method=RESTMethod.DELETE,
            url="https://coinswitch.co/trade/api/v2/futures/order",
            data=json.dumps({"exchange": "EXCHANGE_2", "order_id": "abc"}),
            is_auth_required=True,
        )
        signed = await self.auth.rest_authenticate(request)
        self._verify(signed.headers["X-AUTH-SIGNATURE"],
                     "DELETE/trade/api/v2/futures/order" + signed.headers["X-AUTH-EPOCH"])

    async def test_epochs_strictly_increase_within_one_millisecond(self):
        epochs = []
        for _ in range(3):
            request = RESTRequest(method=RESTMethod.GET, url="https://coinswitch.co/trade/api/v2/futures/wallet_balance",
                                  is_auth_required=True)
            epochs.append(int((await self.auth.rest_authenticate(request)).headers["X-AUTH-EPOCH"]))
        self.assertEqual([1719905777483, 1719905777484, 1719905777485], epochs)

    async def test_keyless_auth_leaves_requests_unsigned(self):
        auth = CoinswitchPerpetualAuth(api_key="", secret_key="", time_provider=self.clock)
        self.assertFalse(auth.has_credentials)
        request = RESTRequest(method=RESTMethod.GET, url="https://coinswitch.co/trade/api/v2/futures/ticker",
                              params={"symbol": "BTCUSDT"}, is_auth_required=True,
                              headers={"Content-Type": "application/x-www-form-urlencoded", "X-Other": "kept"})
        signed = await auth.rest_authenticate(request)
        # Live, CoinSwitch refuses even the open all-pairs ticker with a form-urlencoded content type.
        self.assertEqual({"Content-Type": "application/json", "X-Other": "kept"}, signed.headers)
        self.assertNotIn("X-AUTH-SIGNATURE", signed.headers)
        self.assertEqual({"symbol": "BTCUSDT"}, signed.params)

    def test_bad_secrets_are_rejected(self):
        with self.assertRaises(ValueError):
            CoinswitchPerpetualAuth(api_key=API_KEY, secret_key="not-hex", time_provider=self.clock)
        with self.assertRaises(ValueError):
            CoinswitchPerpetualAuth(api_key=API_KEY, secret_key="abcd", time_provider=self.clock)

    def test_non_json_body_is_left_alone(self):
        self.assertEqual("a=b", CoinswitchPerpetualAuth.plain_number_json("a=b"))
        self.assertEqual('[1,"x",null]', CoinswitchPerpetualAuth.plain_number_json('[1, "x", null]'))

    async def test_ws_requests_pass_through(self):
        request = object()
        self.assertIs(request, await self.auth.ws_authenticate(request))
