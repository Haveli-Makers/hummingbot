from unittest import TestCase

from hummingbot.monitoring.alert import Alert, AlertStatus, Severity
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher


class FakeNotifier:
    def __init__(self):
        self.messages = []

    def add_message_to_queue(self, msg: str):
        self.messages.append(msg)


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


def make_alert(check: str = "depth_below_min",
               severity: Severity = Severity.WARNING,
               status: AlertStatus = AlertStatus.FIRING,
               **kwargs) -> Alert:
    return Alert(
        source="pmm.wazirx.USDT-INR",
        check=check,
        severity=severity,
        title="Depth below minimum",
        message="Ask depth within 1.5% = ₹6,400 (need ₹20,000).",
        status=status,
        **kwargs,
    )


class AlertDispatcherTests(TestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.notifier = FakeNotifier()
        self.dispatcher = AlertDispatcher(
            notifiers=[self.notifier],
            min_severity=Severity.WARNING,
            rate_limit_per_min=5,
            renotify_interval=300.0,
            time_fn=self.clock.time,
        )

    def test_firing_alert_is_delivered_and_formatted(self):
        sent = self.dispatcher.dispatch(make_alert(metrics={"mid": "102.50", "bid_depth": "21300"}))

        self.assertTrue(sent)
        self.assertEqual(1, len(self.notifier.messages))
        message = self.notifier.messages[0]
        self.assertIn("🟠 *Depth below minimum*", message)
        self.assertIn("wazirx · USDT-INR (pmm)", message)
        self.assertIn("Ask depth within 1.5%", message)
        self.assertIn("mid: 102.50", message)
        self.assertIn("bid_depth: 21300", message)

    def test_critical_alert_uses_red_marker(self):
        self.dispatcher.dispatch(make_alert(severity=Severity.CRITICAL))
        self.assertIn("🔴", self.notifier.messages[0])

    def test_below_min_severity_is_dropped(self):
        sent = self.dispatcher.dispatch(make_alert(severity=Severity.INFO))

        self.assertFalse(sent)
        self.assertEqual(0, len(self.notifier.messages))

    def test_duplicate_firing_deduped_until_renotify_interval(self):
        self.assertTrue(self.dispatcher.dispatch(make_alert()))
        self.clock.advance(299.0)
        self.assertFalse(self.dispatcher.dispatch(make_alert()))
        self.clock.advance(2.0)
        self.assertTrue(self.dispatcher.dispatch(make_alert()))
        self.assertEqual(2, len(self.notifier.messages))

    def test_resolved_sends_all_clear_and_clears_state(self):
        self.dispatcher.dispatch(make_alert())
        sent = self.dispatcher.dispatch(make_alert(status=AlertStatus.RESOLVED))

        self.assertTrue(sent)
        self.assertIn("✅ *Resolved: Depth below minimum*", self.notifier.messages[-1])
        # After resolution a fresh firing alert is delivered immediately (state cleared)
        self.assertTrue(self.dispatcher.dispatch(make_alert()))
        self.assertEqual(3, len(self.notifier.messages))

    def test_resolved_without_prior_firing_is_ignored(self):
        sent = self.dispatcher.dispatch(make_alert(status=AlertStatus.RESOLVED))

        self.assertFalse(sent)
        self.assertEqual(0, len(self.notifier.messages))

    def test_rate_limit_blocks_then_refills_over_time(self):
        for i in range(5):
            self.assertTrue(self.dispatcher.dispatch(make_alert(check=f"check_{i}")))
        self.assertFalse(self.dispatcher.dispatch(make_alert(check="check_over_limit")))
        # Tokens refill at rate_limit_per_min / 60 per second: 5/min -> 1 token every 12s
        self.clock.advance(13.0)
        self.assertTrue(self.dispatcher.dispatch(make_alert(check="check_over_limit")))

    def test_dispatch_never_raises(self):
        class BrokenNotifier:
            def add_message_to_queue(self, msg: str):
                raise RuntimeError("boom")

        dispatcher = AlertDispatcher(notifiers=[BrokenNotifier()], time_fn=self.clock.time)
        sent = dispatcher.dispatch(make_alert())
        self.assertFalse(sent)

    def test_add_notifier_after_construction(self):
        extra = FakeNotifier()
        self.dispatcher.add_notifier(extra)
        self.dispatcher.dispatch(make_alert())
        self.assertEqual(1, len(self.notifier.messages))
        self.assertEqual(1, len(extra.messages))
