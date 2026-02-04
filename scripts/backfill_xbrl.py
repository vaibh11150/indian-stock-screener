#!/usr/bin/env python
"""
Backfill XBRL financial data from NSE and BSE.

This script fetches historical financial results for all companies
from both exchanges. It uses the NSE results-comparison API and
BSE financial results API to get pre-parsed financial data.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select

from config.logging_config import setup_logging, get_logger
from db import get_async_session, Company
from scrapers.nse_xbrl import NSEFinancialScraper
from scrapers.bse_xbrl import BSEFinancialScraper

logger = get_logger(__name__)


async def backfill_company(
    company_id: int,
    nse_symbol: Optional[str],
    bse_code: Optional[str],
    session,
) -> int:
    """Backfill financial data for a single company."""
    count = 0

    # Try NSE first
    if nse_symbol:
        try:
            nse_scraper = NSEFinancialScraper(session)
            result = await nse_scraper.run(
                symbol=nse_symbol,
                company_id=company_id,
            )
            count += result.get("records_inserted", 0)
            logger.debug(f"NSE {nse_symbol}: {result.get('records_inserted', 0)} records")
        except Exception as e:
            logger.warning(f"NSE scrape failed for {nse_symbol}: {e}")

    # Try BSE
    if bse_code:
        try:
            bse_scraper = BSEFinancialScraper(session)
            result = await bse_scraper.run(
                scrip_code=bse_code,
                company_id=company_id,
            )
            count += result.get("records_inserted", 0)
            logger.debug(f"BSE {bse_code}: {result.get('records_inserted', 0)} records")
        except Exception as e:
            logger.warning(f"BSE scrape failed for {bse_code}: {e}")

    return count


async def backfill_xbrl(
    limit: Optional[int] = None,
    offset: int = 0,
    symbols: Optional[list[str]] = None,
):
    """
    Run the XBRL backfill for all or selected companies.

    Args:
        limit: Maximum number of companies to process
        offset: Starting offset
        symbols: Optional list of specific symbols to process
    """
    setup_logging()
    logger.info("Starting XBRL backfill")

    async with get_async_session() as session:
        # Get companies to process
        stmt = select(Company).where(Company.is_active == True)

        if symbols:
            stmt = stmt.where(
                Company.nse_symbol.in_(symbols) | Company.bse_scrip_code.in_(symbols)
            )
        else:
            stmt = stmt.offset(offset)
            if limit:
                stmt = stmt.limit(limit)

        stmt = stmt.order_by(Company.id)

        result = await session.execute(stmt)
        companies = result.scalars().all()

        total = len(companies)
        success = 0
        failed = 0
        total_records = 0

        for i, company in enumerate(companies):
            try:
                count = await backfill_company(
                    company.id,
                    company.nse_symbol,
                    company.bse_scrip_code,
                    session,
                )

                if count > 0:
                    success += 1
                    total_records += count
                    logger.info(
                        f"[{i+1}/{total}] {company.company_name}: {count} filings"
                    )
                else:
                    logger.info(f"[{i+1}/{total}] {company.company_name}: no new data")

            except Exception as e:
                failed += 1
                logger.error(f"[{i+1}/{total}] {company.company_name} FAILED: {e}")

            # Rate limiting
            await asyncio.sleep(1.0)

        logger.info(
            f"Backfill complete: {success} success, {failed} failed, "
            f"{total_records} total records"
        )

    return {
        "total_companies": total,
        "success": success,
        "failed": failed,
        "total_records": total_records,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill XBRL financial data")
    parser.add_argument("--limit", type=int, help="Max companies to process")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to process")

    args = parser.parse_args()

    asyncio.run(
        backfill_xbrl(
            limit=args.limit,
            offset=args.offset,
            symbols=args.symbols,
        )
    )
