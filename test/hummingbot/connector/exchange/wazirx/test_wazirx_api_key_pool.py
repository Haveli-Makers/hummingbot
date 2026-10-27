from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, patch

from hummingbot.connector.exchange.wazirx.wazirx_api_key_pool import WazirxApiKeyPool
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.core.api_throttler.data_types import RateLimit


class WazirxApiKeyPoolTests(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.now = 100.0
        self.sleeps = []
        self.auths = [
            WazirxAuth(api_key=f"key{i}", secret_key=f"secret{i}", time_provider=MagicMock()) for i in range(3)
        ]
        self.pool = self.create_pool(rate_limits=[
            RateLimit(limit_id="FAST", limit=10, time_interval=1.5),
            RateLimit(limit_id="SLOW", limit=1, time_interval=1.5),
        ])

    def create_pool(self, rate_limits, auths=None) -> WazirxApiKeyPool:
        return WazirxApiKeyPool(auths=auths or self.auths, rate_limits=rate_limits, time_provider=lambda: self.now)

    async def fake_sleep(self, seconds: float):
        self.sleeps.append(seconds)
        self.now += seconds

    async def acquire_many(self, limit_id: str, count: int):
        return [await self.pool.acquire(limit_id) for _ in range(count)]

    def test_needs_at_least_one_key(self):
        with self.assertRaises(ValueError):
            WazirxApiKeyPool(auths=[], rate_limits=[])

    def test_size_auth_and_label(self):
        self.assertEqual(3, self.pool.size)
        self.assertIs(self.auths[1], self.pool.auth(1))
        self.assertEqual("key 2 of 3", self.pool.label(1))
        self.assertEqual([0, 1, 2], self.pool.active_indexes)

    async def test_requests_rotate_across_keys(self):
        self.assertEqual([0, 1, 2, 0, 1, 2], await self.acquire_many("FAST", 6))

    async def test_rotation_is_tracked_per_endpoint(self):
        self.assertEqual(0, await self.pool.acquire("FAST"))
        self.assertEqual(0, await self.pool.acquire("SLOW"))
        self.assertEqual(1, await self.pool.acquire("FAST"))
        self.assertEqual(1, await self.pool.acquire("SLOW"))

    async def test_key_at_its_limit_is_skipped(self):
        self.assertEqual(0, await self.pool.acquire("SLOW"))
        # key 0 is full on SLOW
        self.pool.set_active([0, 1])
        self.assertEqual(1, await self.pool.acquire("SLOW"))

    async def test_waits_for_first_free_slot_when_every_key_is_full(self):
        self.pool.set_active([0, 1])
        self.assertEqual(0, await self.pool.acquire("SLOW"))
        self.now += 0.5
        self.assertEqual(1, await self.pool.acquire("SLOW"))

        with patch("hummingbot.connector.exchange.wazirx.wazirx_api_key_pool.asyncio.sleep", self.fake_sleep):
            index = await self.pool.acquire("SLOW")

        # key 0 frees first
        self.assertEqual(0, index)
        self.assertEqual([1.0], self.sleeps)

    async def test_window_slides(self):
        self.pool.set_active([0])
        self.assertEqual(0, await self.pool.acquire("SLOW"))
        self.now += 1.5
        with patch("hummingbot.connector.exchange.wazirx.wazirx_api_key_pool.asyncio.sleep", self.fake_sleep):
            self.assertEqual(0, await self.pool.acquire("SLOW"))
        self.assertEqual([], self.sleeps)

    async def test_exclude_leaves_keys_out(self):
        self.assertEqual(1, await self.pool.acquire("FAST", exclude=[0]))
        self.assertEqual(2, await self.pool.acquire("FAST", exclude=[0]))
        self.assertEqual(1, await self.pool.acquire("FAST", exclude=[0]))

    async def test_index_pins_one_key_even_outside_rotation(self):
        self.pool.set_active([0, 1])
        self.assertEqual(2, await self.pool.acquire("FAST", index=2))
        self.assertEqual(2, await self.pool.acquire("FAST", index=2, exclude=[2]))

    async def test_set_active_limits_rotation(self):
        self.pool.set_active([2, 0])
        self.assertEqual([0, 2], self.pool.active_indexes)
        self.assertEqual([0, 2, 0], await self.acquire_many("FAST", 3))

    def test_set_active_needs_a_key(self):
        with self.assertRaises(ValueError):
            self.pool.set_active([])

    async def test_no_key_left_to_try_raises(self):
        with self.assertRaises(ValueError):
            await self.pool.acquire("FAST", exclude=[0, 1, 2])

    async def test_rested_key_is_skipped_until_its_rest_ends(self):
        self.pool.set_active([0, 1])
        self.pool.rest(0, "FAST", 2.0)
        self.assertEqual([1, 1], await self.acquire_many("FAST", 2))

        self.now += 2.0
        self.assertEqual(0, await self.pool.acquire("FAST"))

    async def test_rest_applies_to_one_endpoint(self):
        self.pool.rest(0, "FAST", 2.0)
        self.assertEqual(0, await self.pool.acquire("SLOW"))

    async def test_every_key_resting_raises_without_waiting(self):
        for index in range(3):
            self.pool.rest(index, "FAST", 2.0 + index)

        with self.assertRaisesRegex(IOError, "rate limited on FAST; retry in 2.0s"):
            await self.pool.acquire("FAST")

    async def test_pause_blocks_every_key_until_it_ends(self):
        self.pool.pause(120)
        with self.assertRaisesRegex(IOError, "paused for 120s"):
            await self.pool.acquire("FAST")
        with self.assertRaises(IOError):
            await self.pool.acquire("FAST", index=1)

        self.now += 120
        self.assertEqual(0, await self.pool.acquire("FAST"))

    def test_shorter_pause_does_not_cut_a_longer_one(self):
        self.pool.pause(120)
        self.pool.pause(10)
        self.assertEqual(220.0, self.pool._paused_until)

    async def test_endpoint_without_a_limit_rotates_without_counting(self):
        self.assertEqual([0, 1, 2, 0], await self.acquire_many("UNLISTED", 4))
        self.assertEqual({}, self.pool._sent)
