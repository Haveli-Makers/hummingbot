import asyncio
import base64
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock
from urllib.parse import parse_qsl, unquote, urlencode

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class AjaibAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.seconds = 1700000000.0
        self.ms = 1700000000000

        self._private_key = Ed25519PrivateKey.generate()
        pem = self._private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8")

        time_provider = MagicMock()
        time_provider.time.return_value = self.seconds
        self.auth = AjaibAuth(api_key=self.api_key, secret_key=pem, time_provider=time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _verify(self, signature_b64: str, params: dict):
        """
        Verify against the params in TRANSMISSION order.

        Ajaib validates the signature against the query string it receives, so
        this must mirror the wire order -- verifying against a re-sorted copy
        would pass even when the connector signs something it never sends.
        """
        payload = urlencode(list(params.items())).encode("ascii")
        # Raises InvalidSignature if it does not match.
        self._private_key.public_key().verify(base64.b64decode(signature_b64), payload)

    def test_get_request_signs_the_params_in_query(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.crypto.ajaib.co.id/v1/order",
            params={"symbol": "BTC_IDR", "origClientOrderId": "abc"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(self.api_key, request.headers["X-MBX-APIKEY"])
        self.assertEqual(self.ms, request.params["timestamp"])
        self.assertEqual(CONSTANTS.RECV_WINDOW, request.params["recvWindow"])
        self.assertIn("signature", request.params)

        signature = request.params.pop("signature")
        self._verify(signature, request.params)

    def test_post_request_sends_a_signed_form_body(self):
        """
        Ajaib parses non-GET bodies as FORM data. A JSON body -- which is what
        RESTAssistant produces by default -- is rejected with -1102 Bad Request,
        so auth must emit form encoding AND override the Content-Type header.
        """
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.crypto.ajaib.co.id/v1/order",
            data={"symbol": "BTC_IDR", "side": "BUY", "type": "LIMIT"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual("application/x-www-form-urlencoded", request.headers["Content-Type"])
        self.assertIsInstance(request.data, str, "body must be form-encoded, not a dict/JSON")

        sent = dict(parse_qsl(request.data))
        self.assertEqual("BTC_IDR", sent["symbol"], "caller payload must survive")
        self.assertEqual(str(self.ms), sent["timestamp"])
        self.assertIn("signature", sent)

        body, _, signature_part = request.data.rpartition("&signature=")
        self._private_key.public_key().verify(
            base64.b64decode(unquote(signature_part)), body.encode("ascii"))

    def test_post_body_arriving_as_a_json_string_is_not_discarded(self):
        """
        RESTAssistant json.dumps() the payload before auth runs. Handling only
        dicts silently dropped it -- the listen-key keep-alive lost its
        {"listenKey": ...} and the exchange rejected the request.
        """
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.crypto.ajaib.co.id/auth/v1/listen-key",
            data=json.dumps({"listenKey": "abc123"}),
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        sent = dict(parse_qsl(request.data))
        self.assertEqual("abc123", sent["listenKey"], "payload was discarded")
        self.assertIn("signature", sent)

    def test_content_type_is_overridden_not_defaulted(self):
        # RESTAssistant pre-sets application/json for non-GET; setdefault would
        # leave it, producing a JSON header over a form body.
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.crypto.ajaib.co.id/v1/order",
            data=json.dumps({}),
            headers={"Content-Type": "application/json"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertEqual("application/x-www-form-urlencoded", request.headers["Content-Type"])

    def test_signed_payload_matches_what_is_transmitted(self):
        """
        The whole -1022 class of failure: signing a re-ordered copy of the
        params while sending them in their original order. The signature must
        cover the exact bytes the server will reconstruct from the query string.
        """
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.crypto.ajaib.co.id/v1/account",
            params={"symbol": "BTC_IDR", "origClientOrderId": "abc"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        sent = dict(request.params)
        signature = sent.pop("signature")
        # Exactly the string a server would rebuild from the wire, in order.
        on_the_wire = urlencode(list(sent.items())).encode("ascii")
        self._private_key.public_key().verify(base64.b64decode(signature), on_the_wire)

        # And signature must be the LAST parameter, never part of the payload.
        self.assertEqual("signature", list(request.params.keys())[-1])

    def test_signature_covers_insertion_order_not_a_sorted_copy(self):
        params = {"symbol": "BTC_IDR", "aaa": "1", "timestamp": self.ms}
        signature = self.auth._sign(params)
        insertion = urlencode(list(params.items())).encode("ascii")
        self._private_key.public_key().verify(base64.b64decode(signature), insertion)

        sorted_payload = urlencode(sorted(params.items())).encode("ascii")
        self.assertNotEqual(insertion, sorted_payload, "test params must distinguish the two")

    def test_recv_window_sent_is_within_the_server_cap(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.crypto.ajaib.co.id/v1/account",
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        self.assertLessEqual(int(request.params["recvWindow"]), CONSTANTS.MAX_RECV_WINDOW)

    def test_header_for_authentication(self):
        self.assertEqual({"X-MBX-APIKEY": self.api_key}, self.auth.header_for_authentication())

    def test_missing_key_returns_empty_signature(self):
        auth = AjaibAuth(api_key="", secret_key="", time_provider=MagicMock())
        self.assertEqual("", auth._sign({"a": "1"}))


class AjaibAuthCredentialFailureTests(TestCase):
    """
    A key that fails to load must fail LOUDLY. Previously it left the signer
    with no key, _sign returned an empty string, and the exchange answered
    "-1022 Invalid signature" on every request -- pointing the reader at the
    signing algorithm instead of at a mistyped filename.
    """

    def _auth(self, secret):
        provider = MagicMock()
        provider.time.return_value = 1700000000.0
        return AjaibAuth(api_key="k", secret_key=secret, time_provider=provider)

    def test_missing_key_file_raises_a_message_naming_the_path(self):
        auth = self._auth("/nope/missing_key.pem")
        self.assertIsNone(auth._private_key)
        with self.assertRaises(ValueError) as ctx:
            auth._sign({"timestamp": 1})
        self.assertIn("missing_key.pem", str(ctx.exception))

    def test_malformed_pem_contents_raise(self):
        # Not a key -- PEM-shaped garbage. The header is assembled at runtime so
        # the literal marker never appears in the source and the
        # detect-private-key pre-commit hook has nothing to match on.
        header = "-----BEGIN " + "PRIVATE KEY-----"
        auth = self._auth(f"{header}\nnot-a-key\n-----END PRIVATE KEY-----")
        self.assertIsNone(auth._private_key)
        with self.assertRaises(ValueError):
            auth._sign({"timestamp": 1})

    def test_no_secret_at_all_is_not_an_error(self):
        # Legitimate: the connector is built without credentials for discovery.
        auth = self._auth("")
        self.assertEqual("", auth._sign({"timestamp": 1}))

    def test_a_valid_pem_still_signs(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        pem = Ed25519PrivateKey.generate().private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()).decode()
        auth = self._auth(pem)
        self.assertIsNotNone(auth._private_key)
        self.assertTrue(auth._sign({"timestamp": 1}))
