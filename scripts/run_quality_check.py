#!/usr/bin/env python
"""
Run a quality check comparing our data against screener.in.

This script performs a verification check to ensure our computed
financial data matches the reference data from screener.in within
acceptable thresholds.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.logging_config import setup_logging, get_logger
from db import get_async_session
from quality.checker import QualityChecker

logger = get_logger(__name__)


async def run_quality_check(sample_size: int = 100):
    """Run a quality check against screener.in."""
    setup_logging()
    logger.info(f"Starting quality check with sample size {sample_size}")

    async with get_async_session() as session:
        checker = QualityChecker(session)
        results = await checker.run_quality_check(sample_size=sample_size)

    # Print summary
    print("\n" + "=" * 60)
    print("QUALITY CHECK REPORT")
    print("=" * 60)

    print(f"\nOverall Accuracy: {results.get('accuracy', 0):.1f}%")
    print(f"Total Checks: {results.get('total_checks', 0)}")
    print(f"Within Threshold: {results.get('within_threshold', 0)}")
    print(f"Outside Threshold: {results.get('outside_threshold', 0)}")
    print(f"Errors: {results.get('errors', 0)}")

    print("\n" + "-" * 60)
    print("ACCURACY BY FIELD")
    print("-" * 60)

    by_field = results.get("by_field", {})
    for field, stats in sorted(by_field.items()):
        acc = (stats["ok"] / max(stats["total"], 1)) * 100
        print(f"  {field:25s}: {acc:5.1f}% ({stats['ok']}/{stats['total']})")

    print("\n" + "-" * 60)
    print("WORST DEVIATIONS")
    print("-" * 60)

    worst = results.get("worst_deviations", [])[:10]
    for item in worst:
        print(
            f"  {item.get('symbol', 'N/A'):10s} | "
            f"{item.get('field_name', 'N/A'):20s} | "
            f"Ours: {item.get('our_value', 'N/A'):>12} | "
            f"Ref: {item.get('ref_value', 'N/A'):>12} | "
            f"Dev: {item.get('pct_deviation', 0):>6.1f}%"
        )

    print("\n" + "=" * 60)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run quality check")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Number of companies to check",
    )

    args = parser.parse_args()

    asyncio.run(run_quality_check(sample_size=args.sample_size))
