import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from aioresponses import aioresponses

from hummingbot.notifier.gchat_notifier import GChatNotifier

WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAAA/messages"


class GChatNotifierTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.notifier = GChatNotifier(webhook_url=WEBHOOK_URL)
        self.notifier._sleep = AsyncMock()

    async def asyncTearDown(self):
        if self.notifier._session is not None and not self.notifier._session.closed:
            await self.notifier._session.close()
        await super().asyncTearDown()

    def _recorded_requests(self, mock_api: aioresponses):
        return [call for calls in mock_api.requests.values() for call in calls]

    def test_requires_webhook_url(self):
        with self.assertRaises(ValueError):
            GChatNotifier(webhook_url="")

    @aioresponses()
    async def test_send_message_posts_text_payload(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, status=200)

        await self.notifier._send_message("hello team")

        requests = self._recorded_requests(mock_api)
        self.assertEqual(1, len(requests))
        self.assertEqual({"text": "hello team"}, requests[0].kwargs["json"])

    @aioresponses()
    async def test_retries_once_on_server_error_then_succeeds(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, status=500)
        mock_api.post(WEBHOOK_URL, status=200)

        await self.notifier._send_message("retry me")

        self.assertEqual(2, len(self._recorded_requests(mock_api)))
        self.notifier._sleep.assert_awaited_once()

    @aioresponses()
    async def test_gives_up_after_second_failure_without_raising(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, status=500)
        mock_api.post(WEBHOOK_URL, status=500)

        await self.notifier._send_message("never delivered")

        self.assertEqual(2, len(self._recorded_requests(mock_api)))

    @aioresponses()
    async def test_client_error_is_not_retried(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, status=404)

        await self.notifier._send_message("bad webhook")

        self.assertEqual(1, len(self._recorded_requests(mock_api)))
        self.notifier._sleep.assert_not_awaited()

    @aioresponses()
    async def test_connection_error_is_retried_without_raising(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, exception=ConnectionError("network down"))
        mock_api.post(WEBHOOK_URL, exception=ConnectionError("network down"))

        await self.notifier._send_message("unreachable")

        self.assertEqual(2, len(self._recorded_requests(mock_api)))

    @aioresponses()
    async def test_stop_closes_session(self, mock_api: aioresponses):
        mock_api.post(WEBHOOK_URL, status=200)
        await self.notifier._send_message("open a session")
        session = self.notifier._session
        self.assertFalse(session.closed)

        self.notifier.stop()
        await asyncio.sleep(0)

        self.assertTrue(session.closed)
        self.assertIsNone(self.notifier._session)
