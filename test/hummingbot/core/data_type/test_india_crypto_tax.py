import logging
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.india_crypto_tax import (
    IndiaCryptoTaxConfig,
    IndiaCryptoTaxTracker,
    MarketType,
    calculate_tds,
    get_market_type,
)

TDS_RATE = Decimal("0.01")


class TestCalculateTds(unittest.TestCase):
    """Unit tests for calculate_tds() (Section 194S)."""

    def test_inr_market_buyer_exempt(self):
        result = calculate_tds(
            fill_value_quote=Decimal("100000"),
            is_buyer=True,
            market_type=MarketType.INR,
        )
        self.assertFalse(result.is_applicable)
        self.assertEqual(result.tds_amount_quote, Decimal("0"))

    def test_inr_market_seller_pays(self):
        result = calculate_tds(
            fill_value_quote=Decimal("100000"),
            is_buyer=False,
            market_type=MarketType.INR,
        )
        self.assertTrue(result.is_applicable)
        self.assertEqual(result.tds_amount_quote, Decimal("1000"))  # 1% of 100 000

    def test_crypto_crypto_buyer_pays(self):
        result = calculate_tds(
            fill_value_quote=Decimal("50000"),
            is_buyer=True,
            market_type=MarketType.CRYPTO_CRYPTO,
        )
        self.assertTrue(result.is_applicable)
        self.assertEqual(result.tds_amount_quote, Decimal("500"))  # 1% of 50 000

    def test_crypto_crypto_seller_pays(self):
        result = calculate_tds(
            fill_value_quote=Decimal("50000"),
            is_buyer=False,
            market_type=MarketType.CRYPTO_CRYPTO,
        )
        self.assertTrue(result.is_applicable)
        self.assertEqual(result.tds_amount_quote, Decimal("500"))

    def test_zero_fill_value(self):
        result = calculate_tds(
            fill_value_quote=Decimal("0"),
            is_buyer=False,
            market_type=MarketType.INR,
        )
        self.assertEqual(result.tds_amount_quote, Decimal("0"))

    def test_custom_tds_rate(self):
        config = IndiaCryptoTaxConfig(tds_rate=Decimal("0.05"))
        result = calculate_tds(
            fill_value_quote=Decimal("10000"),
            is_buyer=False,
            market_type=MarketType.INR,
            config=config,
        )
        self.assertEqual(result.tds_amount_quote, Decimal("500"))  # 5% of 10 000


class TestGetMarketType(unittest.TestCase):
    """Unit tests for get_market_type()."""

    def test_inr_pair(self):
        self.assertEqual(get_market_type("BTC-INR"), MarketType.INR)

    def test_inr_pair_lowercase(self):
        self.assertEqual(get_market_type("eth-inr"), MarketType.INR)

    def test_usdt_pair(self):
        self.assertEqual(get_market_type("BTC-USDT"), MarketType.CRYPTO_CRYPTO)

    def test_btc_pair(self):
        self.assertEqual(get_market_type("ETH-BTC"), MarketType.CRYPTO_CRYPTO)

    def test_single_segment_defaults_to_crypto_crypto(self):
        self.assertEqual(get_market_type("BTCUSDT"), MarketType.CRYPTO_CRYPTO)


class TestIndiaCryptoTaxTrackerTdsStore(unittest.TestCase):
    """Tests for record_tds / pop_tds memory management."""

    def setUp(self):
        self.tracker = IndiaCryptoTaxTracker()

    def test_record_and_pop_returns_amount(self):
        self.tracker.record_tds("trade-1", Decimal("42"))
        self.assertEqual(self.tracker.pop_tds("trade-1"), Decimal("42"))

    def test_pop_removes_entry(self):
        self.tracker.record_tds("trade-1", Decimal("42"))
        self.tracker.pop_tds("trade-1")
        self.assertEqual(self.tracker.pop_tds("trade-1"), Decimal("0"))

    def test_pop_unknown_trade_returns_zero(self):
        self.assertEqual(self.tracker.pop_tds("nonexistent"), Decimal("0"))

    def test_get_tds_does_not_remove_entry(self):
        self.tracker.record_tds("trade-2", Decimal("10"))
        self.tracker.get_tds("trade-2")
        self.assertEqual(self.tracker.get_tds("trade-2"), Decimal("10"))

    def test_calc_and_record_tds_inr_seller(self):
        amount = self.tracker.calc_and_record_tds(
            trade_id="t1",
            fill_value_quote=Decimal("200000"),
            is_buyer=False,
            trading_pair="BTC-INR",
        )
        self.assertEqual(amount, Decimal("2000"))  # 1% of 200 000
        self.assertEqual(self.tracker.pop_tds("t1"), Decimal("2000"))

    def test_calc_and_record_tds_inr_buyer_exempt(self):
        amount = self.tracker.calc_and_record_tds(
            trade_id="t2",
            fill_value_quote=Decimal("200000"),
            is_buyer=True,
            trading_pair="BTC-INR",
        )
        self.assertEqual(amount, Decimal("0"))

    def test_calc_and_record_tds_crypto_crypto_buyer(self):
        amount = self.tracker.calc_and_record_tds(
            trade_id="t3",
            fill_value_quote=Decimal("100"),
            is_buyer=True,
            trading_pair="ETH-USDT",
        )
        self.assertEqual(amount, Decimal("1"))


class TestIndiaCryptoTaxTrackerTrackAndLog(unittest.TestCase):
    """Tests for track_and_log() FIFO matching and log output."""

    def setUp(self):
        self.tracker = IndiaCryptoTaxTracker()
        self.logger = MagicMock(spec=logging.Logger)

    def test_buy_fill_queued_and_logged(self):
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3000000"),
            fee_amount=Decimal("3000"),
            tds_amount=Decimal("0"),
            quote="INR",
            logger=self.logger,
        )
        queue = self.tracker._pending_buy_fills.get("BTC-INR")
        self.assertIsNotNone(queue)
        self.assertEqual(len(queue), 1)
        self.logger.info.assert_called_once()
        log_msg = self.logger.info.call_args[0][0]
        self.assertIn("Buy Fill", log_msg)

    def test_multiple_buy_fills_queued_in_order(self):
        for i in range(3):
            self.tracker.track_and_log(
                trade_type=TradeType.BUY,
                trading_pair="BTC-USDT",
                fill_base=Decimal("1"),
                fill_value=Decimal(str(30000 + i * 1000)),
                fee_amount=Decimal("10"),
                tds_amount=Decimal("300"),
                quote="USDT",
                logger=self.logger,
            )
        queue = self.tracker._pending_buy_fills["BTC-USDT"]
        self.assertEqual(len(queue), 3)

    def test_sell_with_no_tracked_buys_logs_advisory(self):
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-INR",
            fill_base=Decimal("0.5"),
            fill_value=Decimal("1500000"),
            fee_amount=Decimal("1500"),
            tds_amount=Decimal("15000"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.info.assert_called_once()
        log_msg = self.logger.info.call_args[0][0]
        self.assertIn("No matching buy", log_msg)

    def test_full_match_sell_produces_profit_report(self):
        # Queue a buy
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3000000"),
            fee_amount=Decimal("3000"),
            tds_amount=Decimal("0"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.reset_mock()
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3200000"),
            fee_amount=Decimal("3200"),
            tds_amount=Decimal("32000"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.info.assert_called_once()
        log_msg = self.logger.info.call_args[0][0]
        self.assertIn("Tax & Profit Report", log_msg)
        self.assertEqual(len(self.tracker._pending_buy_fills["BTC-INR"]), 0)

    def test_fifo_consumes_multiple_buys_for_one_sell(self):
        for _ in range(3):
            self.tracker.track_and_log(
                trade_type=TradeType.BUY,
                trading_pair="ETH-USDT",
                fill_base=Decimal("1"),
                fill_value=Decimal("2000"),
                fee_amount=Decimal("2"),
                tds_amount=Decimal("20"),
                quote="USDT",
                logger=self.logger,
            )
        self.logger.reset_mock()
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="ETH-USDT",
            fill_base=Decimal("3"),
            fill_value=Decimal("6300"),
            fee_amount=Decimal("6"),
            tds_amount=Decimal("63"),
            quote="USDT",
            logger=self.logger,
        )
        self.assertEqual(len(self.tracker._pending_buy_fills["ETH-USDT"]), 0)
        self.logger.info.assert_called_once()

    def test_partial_sell_leaves_remainder_in_queue(self):
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-INR",
            fill_base=Decimal("2"),
            fill_value=Decimal("6000000"),
            fee_amount=Decimal("6000"),
            tds_amount=Decimal("0"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.reset_mock()
        # Sell only half
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3200000"),
            fee_amount=Decimal("3200"),
            tds_amount=Decimal("32000"),
            quote="INR",
            logger=self.logger,
        )
        queue = self.tracker._pending_buy_fills["BTC-INR"]
        self.assertEqual(len(queue), 1)
        remaining_base, remaining_val, _, _ = queue[0]
        self.assertEqual(remaining_base, Decimal("1"))
        self.assertAlmostEqual(float(remaining_val), 3000000.0)

    def test_partial_match_warns_with_both_tds_amounts(self):
        """Actual and matched-portion TDS must both appear in the warning."""
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3000000"),
            fee_amount=Decimal("0"),
            tds_amount=Decimal("0"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.reset_mock()
        actual_tds = Decimal("40000")
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-INR",
            fill_base=Decimal("2"),
            fill_value=Decimal("4000000"),
            fee_amount=Decimal("0"),
            tds_amount=actual_tds,
            quote="INR",
            logger=self.logger,
        )
        warning_call = self.logger.warning.call_args[0][0]
        self.assertIn("40000", warning_call)
        self.assertIn("20000", warning_call)

    def test_approximate_note_appended_to_report(self):
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-USDT",
            fill_base=Decimal("1"),
            fill_value=Decimal("50000"),
            fee_amount=Decimal("0"),
            tds_amount=Decimal("500"),
            quote="USDT",
            logger=self.logger,
        )
        self.logger.reset_mock()
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-USDT",
            fill_base=Decimal("1"),
            fill_value=Decimal("52000"),
            fee_amount=Decimal("0"),
            tds_amount=Decimal("520"),
            quote="USDT",
            logger=self.logger,
            approximate_note="APPROXIMATE: fee charged in WRX; excluded",
        )
        report_msg = self.logger.info.call_args[0][0]
        self.assertIn("APPROXIMATE", report_msg)
        self.assertIn("WRX", report_msg)

    def test_no_approximate_note_by_default(self):
        self.tracker.track_and_log(
            trade_type=TradeType.BUY,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3000000"),
            fee_amount=Decimal("0"),
            tds_amount=Decimal("0"),
            quote="INR",
            logger=self.logger,
        )
        self.logger.reset_mock()
        self.tracker.track_and_log(
            trade_type=TradeType.SELL,
            trading_pair="BTC-INR",
            fill_base=Decimal("1"),
            fill_value=Decimal("3200000"),
            fee_amount=Decimal("0"),
            tds_amount=Decimal("32000"),
            quote="INR",
            logger=self.logger,
        )
        report_msg = self.logger.info.call_args[0][0]
        self.assertNotIn("APPROXIMATE", report_msg)


if __name__ == "__main__":
    unittest.main()
