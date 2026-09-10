import logging
from unittest import TestCase

from hummingbot.monitoring.alert import AlertStatus, Severity
from hummingbot.monitoring.gchat_log_handler import MESSAGE_MAX_LEN, TITLE_MAX_LEN, GChatLogHandler


class FakeDispatcher:
    def __init__(self):
        self.alerts = []

    def dispatch(self, alert) -> bool:
        self.alerts.append(alert)
        return True


class GChatLogHandlerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.dispatcher = FakeDispatcher()
        self.handler = GChatLogHandler(self.dispatcher, source="test-bot")
        self.logger = logging.getLogger("hummingbot.strategy.test_pmm")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)
        super().tearDown()

    def test_error_record_dispatched_as_warning_alert(self):
        self.logger.error("Order placement failed for USDT-INR")

        self.assertEqual(1, len(self.dispatcher.alerts))
        alert = self.dispatcher.alerts[0]
        self.assertEqual(Severity.WARNING, alert.severity)
        self.assertEqual(AlertStatus.FIRING, alert.status)
        self.assertEqual("test-bot.log", alert.source)
        self.assertEqual("log.hummingbot.strategy.test_pmm", alert.check)
        self.assertEqual("Order placement failed for USDT-INR", alert.title)
        self.assertEqual("ERROR", alert.metrics["level"])

    def test_critical_record_dispatched_as_critical_alert(self):
        self.logger.critical("Bot is going down")

        self.assertEqual(Severity.CRITICAL, self.dispatcher.alerts[0].severity)

    def test_error_with_exception_info_is_critical_and_includes_exception(self):
        try:
            raise ValueError("boom")
        except ValueError:
            self.logger.error("Control loop crashed", exc_info=True)

        alert = self.dispatcher.alerts[0]
        self.assertEqual(Severity.CRITICAL, alert.severity)
        self.assertIn("ValueError: boom", alert.message)
        self.assertEqual("Control loop crashed", alert.title)

    def test_info_and_warning_records_are_ignored(self):
        self.logger.info("all fine")
        self.logger.warning("minor hiccup")

        self.assertEqual(0, len(self.dispatcher.alerts))

    def test_own_framework_loggers_are_excluded(self):
        for name in ("hummingbot.notifier.gchat_notifier", "hummingbot.monitoring.alert_dispatcher"):
            framework_logger = logging.getLogger(name)
            framework_logger.propagate = False
            framework_logger.addHandler(self.handler)
            try:
                framework_logger.error("this must not loop back")
            finally:
                framework_logger.removeHandler(self.handler)

        self.assertEqual(0, len(self.dispatcher.alerts))

    def test_long_message_truncated(self):
        self.logger.error("x" * 5000)

        alert = self.dispatcher.alerts[0]
        self.assertLessEqual(len(alert.title), TITLE_MAX_LEN)
        self.assertLessEqual(len(alert.message), MESSAGE_MAX_LEN)

    def test_dispatch_failure_does_not_raise(self):
        class BrokenDispatcher:
            def dispatch(self, alert):
                raise RuntimeError("dispatcher down")

        handler = GChatLogHandler(BrokenDispatcher(), source="test-bot")
        self.logger.addHandler(handler)
        try:
            self.logger.error("should be swallowed")  # must not raise
        finally:
            self.logger.removeHandler(handler)
