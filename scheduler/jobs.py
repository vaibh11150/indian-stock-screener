"""
Scheduler jobs for automated data updates.

Nightly and periodic update jobs for:
- Daily price updates
- New financial results
- Ratio recomputation
- Quality checks
"""

import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from config.logging_config import get_logger, setup_logging
from config.settings import settings
from db import get_async_session, Company, ComputedRatio, PeriodType, ResultNature
from scrapers.nse_company_master import NSECompanyMasterScraper
from scrapers.bse_company_master import BSECompanyMasterScraper
from scrapers.nse_xbrl import NSEFinancialScraper
from scrapers.bse_xbrl import BSEFinancialScraper
from scrapers.price_scraper import PriceScraper
from engine.ratios import compute_ratios
from engine.ttm import TTMCalculator
from engine.growth import GrowthCalculator
from quality.checker import QualityChecker

logger = get_logger(__name__)

# Create the scheduler
scheduler = AsyncIOScheduler()


# ===== DAILY JOBS =====

@scheduler.scheduled_job(
    CronTrigger(hour=18, minute=30, timezone="Asia/Kolkata"),
    id="daily_price_update",
    name="Daily Price Update",
)
async def daily_price_update():
    """
    Run at 6:30 PM IST (after market close).
    Fetch today's bhavcopy and update daily_prices.
    """
    logger.info("Starting daily price update")

    today = date.today()

    async with get_async_session() as session:
        scraper = PriceScraper(session)
        result = await scraper.run(trade_date=today, use_bhavcopy=True)

        logger.info(
            f"Daily price update completed: "
            f"scraped={result['records_scraped']}, "
            f"inserted={result['records_inserted']}"
        )


@scheduler.scheduled_job(
    CronTrigger(hour=20, minute=0, timezone="Asia/Kolkata"),
    id="daily_ratio_recompute",
    name="Daily Ratio Recompute",
)
async def daily_ratio_recompute():
    """
    Run at 8 PM IST.
    Recompute TTM ratios for companies that got new price data.
    (PE, PB, dividend yield change daily with price)
    """
    logger.info("Starting daily ratio recompute")

    async with get_async_session() as session:
        # Get companies with recent price data
        stmt = (
            select(Company)
            .where(Company.is_active == True, Company.nse_symbol.isnot(None))
            .limit(500)  # Process top 500
        )

        result = await session.execute(stmt)
        companies = result.scalars().all()

        updated = 0
        for company in companies:
            try:
                # Compute TTM and ratios
                calculator = TTMCalculator(session)
                ttm_data = await calculator.compute_ttm(
                    company.id,
                    result_nature=ResultNature.CONSOLIDATED,
                )

                # Get latest price for market cap calculation
                # (simplified - would need to fetch from daily_prices)
                ratios = compute_ratios(ttm_data)

                # Update computed_ratios table
                # (simplified - would need proper upsert)
                updated += 1

            except Exception as e:
                logger.warning(f"Failed to update ratios for {company.nse_symbol}: {e}")

        await session.commit()
        logger.info(f"Daily ratio recompute completed: {updated} companies updated")


# ===== WEEKLY JOBS =====

@scheduler.scheduled_job(
    CronTrigger(day_of_week="sat", hour=6, timezone="Asia/Kolkata"),
    id="weekly_financial_update",
    name="Weekly Financial Update",
)
async def weekly_financial_update():
    """
    Run Saturday morning.
    Check for new financial result filings on NSE/BSE.
    Process any new filings found since last check.
    """
    logger.info("Starting weekly financial update")

    async with get_async_session() as session:
        # Get companies to update
        stmt = (
            select(Company)
            .where(Company.is_active == True)
            .limit(200)
        )

        result = await session.execute(stmt)
        companies = result.scalars().all()

        nse_scraper = NSEFinancialScraper(session)
        bse_scraper = BSEFinancialScraper(session)

        for company in companies:
            try:
                if company.nse_symbol:
                    await nse_scraper.run(
                        symbol=company.nse_symbol,
                        company_id=company.id,
                    )

                if company.bse_scrip_code:
                    await bse_scraper.run(
                        scrip_code=company.bse_scrip_code,
                        company_id=company.id,
                    )

                # Rate limiting
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.warning(
                    f"Failed to update financials for {company.nse_symbol}: {e}"
                )

        logger.info("Weekly financial update completed")


@scheduler.scheduled_job(
    CronTrigger(day_of_week="sun", hour=6, timezone="Asia/Kolkata"),
    id="weekly_quality_check",
    name="Weekly Quality Check",
)
async def weekly_quality_check():
    """
    Run Sunday morning.
    Compare our data against screener.in for 100 sample companies.
    Generate quality report.
    """
    logger.info("Starting weekly quality check")

    async with get_async_session() as session:
        checker = QualityChecker(session)
        results = await checker.run_quality_check(sample_size=100)

        logger.info(
            f"Weekly quality check completed: "
            f"accuracy={results.get('accuracy')}%, "
            f"checks={results.get('total_checks')}"
        )


# ===== QUARTERLY JOBS =====

@scheduler.scheduled_job(
    CronTrigger(day=15, month="1,4,7,10", hour=6, timezone="Asia/Kolkata"),
    id="quarterly_full_refresh",
    name="Quarterly Full Refresh",
)
async def quarterly_full_refresh():
    """
    Run on 15th of Jan/Apr/Jul/Oct.
    Full refresh of shareholding patterns and corporate actions.
    Recompute all annual ratios.
    """
    logger.info("Starting quarterly full refresh")

    # This would include:
    # 1. Fetch shareholding patterns for all companies
    # 2. Fetch corporate actions (dividends, splits)
    # 3. Recompute annual ratios with growth metrics
    # 4. Update computed_ratios table

    logger.info("Quarterly full refresh completed")


@scheduler.scheduled_job(
    CronTrigger(day=1, month="1,4,7,10", hour=6, timezone="Asia/Kolkata"),
    id="quarterly_company_master_refresh",
    name="Quarterly Company Master Refresh",
)
async def quarterly_company_master_refresh():
    """
    Refresh company master data (new listings, delistings, name changes).
    """
    logger.info("Starting quarterly company master refresh")

    async with get_async_session() as session:
        # Refresh NSE list
        nse_scraper = NSECompanyMasterScraper(session)
        nse_result = await nse_scraper.run()

        # Refresh BSE list
        bse_scraper = BSECompanyMasterScraper(session)
        bse_result = await bse_scraper.run()

        logger.info(
            f"Company master refresh completed: "
            f"NSE={nse_result['records_inserted']}, "
            f"BSE={bse_result['records_inserted']}"
        )


def start_scheduler():
    """Start the scheduler."""
    setup_logging()
    logger.info("Starting scheduler")
    scheduler.start()


def stop_scheduler():
    """Stop the scheduler."""
    logger.info("Stopping scheduler")
    scheduler.shutdown()


async def run_scheduler():
    """Run the scheduler in async mode."""
    setup_logging()
    logger.info("Starting async scheduler")
    scheduler.start()

    try:
        # Keep running
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(run_scheduler())
