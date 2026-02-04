#!/usr/bin/env python
"""
Seed the company master data from NSE and BSE.

This script fetches the list of all listed companies from both exchanges
and merges them into a unified company master table using ISIN as the key.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.logging_config import setup_logging, get_logger
from db import get_async_session
from scrapers.nse_company_master import NSECompanyMasterScraper
from scrapers.bse_company_master import BSECompanyMasterScraper

logger = get_logger(__name__)


async def seed_companies():
    """Seed company master data from NSE and BSE."""
    setup_logging()
    logger.info("Starting company master seed")

    async with get_async_session() as session:
        # Seed from NSE
        logger.info("Fetching NSE company list...")
        nse_scraper = NSECompanyMasterScraper(session)
        nse_result = await nse_scraper.run()
        logger.info(
            f"NSE: scraped={nse_result['records_scraped']}, "
            f"inserted={nse_result['records_inserted']}"
        )

        # Seed from BSE
        logger.info("Fetching BSE company list...")
        bse_scraper = BSECompanyMasterScraper(session)
        bse_result = await bse_scraper.run()
        logger.info(
            f"BSE: scraped={bse_result['records_scraped']}, "
            f"inserted={bse_result['records_inserted']}"
        )

    logger.info("Company master seed completed")

    return {
        "nse": nse_result,
        "bse": bse_result,
    }


if __name__ == "__main__":
    asyncio.run(seed_companies())
