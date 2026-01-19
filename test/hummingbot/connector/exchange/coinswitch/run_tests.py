"""
CoinSwitch Connector Test Suite Runner

This module provides utilities for running the CoinSwitch connector test suite.
"""

import sys
import unittest
from pathlib import Path

from test_coinswitch_api_order_book_data_source import CoinswitchAPIOrderBookDataSourceTests
from test_coinswitch_auth import CoinswitchAuthTests
from test_coinswitch_constants import CoinswitchConstantsTests
from test_coinswitch_exchange import CoinswitchExchangeTests
from test_coinswitch_user_stream_data_source import CoinswitchUserStreamDataSourceTests
from test_coinswitch_utils import CoinswitchUtilsTests
from test_coinswitch_web_utils import CoinswitchWebUtilsTests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent


def create_test_suite():
    """Create and return the complete CoinSwitch test suite."""
    suite = unittest.TestSuite()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchAuthTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchConstantsTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchUtilsTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchWebUtilsTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchExchangeTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchAPIOrderBookDataSourceTests))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchUserStreamDataSourceTests))

    return suite


def run_all_tests(verbosity=2):
    """Run all CoinSwitch connector tests."""
    runner = unittest.TextTestRunner(verbosity=verbosity)
    suite = create_test_suite()
    result = runner.run(suite)
    return result


def run_auth_tests(verbosity=2):
    """Run only authentication tests."""
    runner = unittest.TextTestRunner(verbosity=verbosity)
    suite = unittest.TestLoader().loadTestsFromTestCase(CoinswitchAuthTests)
    result = runner.run(suite)
    return result


def run_exchange_tests(verbosity=2):
    """Run only exchange connector tests."""
    runner = unittest.TextTestRunner(verbosity=verbosity)
    suite = unittest.TestLoader().loadTestsFromTestCase(CoinswitchExchangeTests)
    result = runner.run(suite)
    return result


def run_market_data_tests(verbosity=2):
    """Run only market data source tests."""
    runner = unittest.TextTestRunner(verbosity=verbosity)
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchAPIOrderBookDataSourceTests))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchUserStreamDataSourceTests))
    result = runner.run(suite)
    return result


def run_utility_tests(verbosity=2):
    """Run only utility function tests."""
    runner = unittest.TextTestRunner(verbosity=verbosity)
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchConstantsTests))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchUtilsTests))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CoinswitchWebUtilsTests))
    result = runner.run(suite)
    return result


def get_test_statistics():
    """Get statistics about the test suite."""
    suite = create_test_suite()

    stats = {
        "total_tests": suite.countTestCases(),
        "test_files": 7,
        "total_lines": 1478,
        "modules": [
            {"name": "test_coinswitch_auth", "tests": 8},
            {"name": "test_coinswitch_constants", "tests": 12},
            {"name": "test_coinswitch_utils", "tests": 18},
            {"name": "test_coinswitch_web_utils", "tests": 16},
            {"name": "test_coinswitch_exchange", "tests": 24},
            {"name": "test_coinswitch_api_order_book_data_source", "tests": 16},
            {"name": "test_coinswitch_user_stream_data_source", "tests": 20},
        ]
    }

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CoinSwitch Connector Test Suite Runner")
    parser.add_argument(
        "--test-group",
        choices=["all", "auth", "exchange", "market-data", "utils"],
        default="all",
        help="Which test group to run (default: all)"
    )
    parser.add_argument(
        "--verbosity",
        type=int,
        choices=[0, 1, 2],
        default=2,
        help="Test output verbosity (default: 2)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print test suite statistics"
    )

    args = parser.parse_args()

    if args.stats:
        stats = get_test_statistics()
        print("=" * 60)
        print("CoinSwitch Connector Test Suite Statistics")
        print("=" * 60)
        print(f"Total Test Files: {stats['test_files']}")
        print(f"Total Tests: {stats['total_tests']}")
        print(f"Total Lines: {stats['total_lines']}")
        print("\nTest Modules:")
        for module in stats["modules"]:
            print(f"  - {module['name']}: {module['tests']} tests")
        print("=" * 60)
    else:
        print("Running CoinSwitch Connector Test Suite...")
        print("=" * 60)

        if args.test_group == "all":
            result = run_all_tests(args.verbosity)
        elif args.test_group == "auth":
            result = run_auth_tests(args.verbosity)
        elif args.test_group == "exchange":
            result = run_exchange_tests(args.verbosity)
        elif args.test_group == "market-data":
            result = run_market_data_tests(args.verbosity)
        elif args.test_group == "utils":
            result = run_utility_tests(args.verbosity)

        print("=" * 60)
        print(f"Tests Run: {result.testsRun}")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
        print(f"Skipped: {len(result.skipped)}")

        sys.exit(0 if result.wasSuccessful() else 1)
