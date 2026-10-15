import os
from unittest import TestCase
from unittest.mock import patch

from hummingbot.core.rate_oracle.sources.ajaib_rate_source import AjaibRateSource

SAVED_KEYS = {"ajaib_api_key": "saved-key", "ajaib_api_secret": "saved-secret", "ajaib_proxy_url": "socks5://saved:1080"}


class AjaibRateSourceCredentialsTest(TestCase):
    def _build(self, env: dict, saved: dict = SAVED_KEYS) -> dict:
        env = {"AJAIB_API_KEY": "", "AJAIB_API_SECRET": "", "AJAIB_PROXY_URL": "", **env}
        with patch.dict(os.environ, env), \
                patch("hummingbot.client.config.security.Security.api_keys", return_value=saved), \
                patch("hummingbot.connector.exchange.ajaib.ajaib_exchange.AjaibExchange") as exchange:
            AjaibRateSource._build_ajaib_connector()
        return exchange.call_args.kwargs

    def test_saved_config_used_without_env(self):
        kwargs = self._build({})
        self.assertEqual(("saved-key", "saved-secret"), (kwargs["ajaib_api_key"], kwargs["ajaib_api_secret"]))

    def test_env_pair_overrides_saved_pair(self):
        kwargs = self._build({"AJAIB_API_KEY": "env-key", "AJAIB_API_SECRET": "env-secret"})
        self.assertEqual(("env-key", "env-secret"), (kwargs["ajaib_api_key"], kwargs["ajaib_api_secret"]))

    def test_env_key_alone_does_not_mix_with_saved_secret(self):
        for env in ({"AJAIB_API_KEY": "env-key"}, {"AJAIB_API_SECRET": "env-secret"}):
            with self.subTest(env=env):
                kwargs = self._build(env)
                self.assertEqual(("saved-key", "saved-secret"), (kwargs["ajaib_api_key"], kwargs["ajaib_api_secret"]))

    def test_env_proxy_overrides_on_its_own(self):
        kwargs = self._build({"AJAIB_PROXY_URL": "socks5://env:1080"})
        self.assertEqual("socks5://env:1080", kwargs["ajaib_proxy_url"])
        self.assertEqual("saved-key", kwargs["ajaib_api_key"])
