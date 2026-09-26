from unittest import TestCase

from hummingbot.monitoring.alert import AlertStatus, Severity
from hummingbot.monitoring.breach_fsm import BreachState, BreachStateMachine


class FakeDispatcher:
    def __init__(self):
        self.alerts = []

    def dispatch(self, alert) -> bool:
        self.alerts.append(alert)
        return True


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


class BreachStateMachineTests(TestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.dispatcher = FakeDispatcher()
        self.fsm = BreachStateMachine(
            source="pmm.wazirx.USDT-INR",
            check="depth_below_min",
            severity=Severity.WARNING,
            title="Depth below the SLA minimum",
            dispatcher=self.dispatcher,
            grace_period_sec=2.0,
            time_fn=self.clock.time,
        )

    def tick(self, breached: bool, seconds: float = 1.0):
        self.fsm.update(breached, message="bid 100, ask 100.")
        self.clock.advance(seconds)

    def test_blip_shorter_than_grace_never_fires(self):
        self.tick(True)   # t=0: PENDING
        self.tick(True)   # t=1: still within grace
        self.tick(False)  # t=2: recovered before firing

        self.assertEqual([], self.dispatcher.alerts)
        self.assertEqual(BreachState.OK, self.fsm.state)

    def test_sustained_breach_fires_after_grace(self):
        self.tick(True)   # t=0: PENDING
        self.assertEqual(BreachState.PENDING, self.fsm.state)
        self.tick(True)   # t=1: 1s elapsed < 2s grace
        self.assertEqual(BreachState.PENDING, self.fsm.state)
        self.tick(True)   # t=2: 2s elapsed >= grace -> FIRING

        self.assertEqual(BreachState.FIRING, self.fsm.state)
        self.assertEqual(1, len(self.dispatcher.alerts))
        alert = self.dispatcher.alerts[0]
        self.assertEqual(AlertStatus.FIRING, alert.status)
        self.assertEqual("depth_below_min", alert.check)
        self.assertIn("Down for 2s", alert.message)
        self.assertIn("bid 100", alert.message)

    def test_firing_redispatches_each_update_for_renotify(self):
        for _ in range(5):
            self.tick(True)

        # Fired at t=2, then re-dispatched at t=3 and t=4; the AlertDispatcher's dedup
        # decides which of these actually reach the channel.
        self.assertEqual(3, len(self.dispatcher.alerts))

    def test_recovery_from_firing_sends_resolved(self):
        for _ in range(3):
            self.tick(True)
        self.tick(False)

        self.assertEqual(BreachState.OK, self.fsm.state)
        resolved = self.dispatcher.alerts[-1]
        self.assertEqual(AlertStatus.RESOLVED, resolved.status)
        self.assertIn("Recovered after 3s", resolved.message)

    def test_flapping_resets_pending_without_alerts(self):
        for _ in range(4):
            self.tick(True)
            self.tick(False)

        self.assertEqual([], self.dispatcher.alerts)
        self.assertEqual(BreachState.OK, self.fsm.state)

    def test_zero_grace_fires_immediately(self):
        fsm = BreachStateMachine(
            source="s", check="c", severity=Severity.CRITICAL, title="t",
            dispatcher=self.dispatcher, grace_period_sec=0.0, time_fn=self.clock.time,
        )
        fsm.update(True)

        self.assertEqual(BreachState.FIRING, fsm.state)
        self.assertEqual(1, len(self.dispatcher.alerts))

    def test_recovery_while_pending_is_silent(self):
        self.tick(True)
        self.tick(False)
        self.tick(False)

        self.assertEqual([], self.dispatcher.alerts)
