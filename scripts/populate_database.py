#!/usr/bin/env python
"""
Master script to populate the Indian Stock Screener database.

This script orchestrates the full data population pipeline:
1. Seeds companies from NSE/BSE master lists
2. Scrapes financial results (XBRL data from 2015+)
3. Scrapes daily prices
4. Computes financial ratios
5. Runs quality checks

Usage:
    python scripts/populate_database.py --full          # Full population
    python scripts/populate_database.py --companies     # Only seed companies
    python scripts/populate_database.py --financials    # Only scrape financials
    python scripts/populate_database.py --prices        # Only scrape prices
    python scripts/populate_database.py --ratios        # Only compute ratios
    python scripts/populate_database.py --sample        # Quick sample (10 companies)
"""

import argparse
import asyncio
import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

# Add project root to path
sys.path.insert(0, "/Users/vaibhavrungta/NSE BSE SCRAPER/indian-screener")

from config.logging_config import setup_logging, get_logger
from db import get_async_session, init_db
from db.models import (
    Company,
    FinancialStatement,
    FinancialLineItem,
    DailyPrice,
    ComputedRatio,
    StatementType,
    ResultNature,
    PeriodType,
)
from scrapers.nse_company_master import NSECompanyMasterScraper
from scrapers.bse_company_master import BSECompanyMasterScraper
from scrapers.nse_xbrl import NSEFinancialScraper
from scrapers.bse_xbrl import BSEFinancialScraper
from scrapers.price_scraper import PriceScraper
from engine.ratios import compute_ratios
from engine.ttm import TTMCalculator

logger = get_logger(__name__)


class DatabasePopulator:
    """Orchestrates database population."""

    def __init__(self):
        self.stats = {
            "companies_seeded": 0,
            "financials_scraped": 0,
            "prices_scraped": 0,
            "ratios_computed": 0,
            "errors": 0,
        }

    async def run_full_population(self, limit: Optional[int] = None):
        """Run complete database population pipeline."""
        logger.info("=== Starting Full Database Population ===")

        # Step 1: Seed companies
        await self.seed_companies()

        # Step 2: Scrape financials
        await self.scrape_financials(limit=limit)

        # Step 3: Scrape prices
        await self.scrape_prices(limit=limit)

        # Step 4: Compute ratios
        await self.compute_all_ratios(limit=limit)

        logger.info("=== Database Population Complete ===")
        self._print_stats()

    async def seed_companies(self):
        """Seed companies from NSE and BSE master lists."""
        logger.info("Step 1: Seeding companies from NSE/BSE...")

        async with get_async_session() as session:
            # Seed NSE companies
            try:
                nse_scraper = NSECompanyMasterScraper(session)
                nse_result = await nse_scraper.run()
                self.stats["companies_seeded"] += nse_result.get("records_inserted", 0)
                logger.info(f"NSE: Seeded {nse_result.get('records_inserted', 0)} companies")
            except Exception as e:
                logger.error(f"NSE seeding failed: {e}")
                self.stats["errors"] += 1

            # Seed BSE companies
            try:
                bse_scraper = BSECompanyMasterScraper(session)
                bse_result = await bse_scraper.run()
                self.stats["companies_seeded"] += bse_result.get("records_inserted", 0)
                logger.info(f"BSE: Seeded {bse_result.get('records_inserted', 0)} companies")
            except Exception as e:
                logger.error(f"BSE seeding failed: {e}")
                self.stats["errors"] += 1

            # Get total count
            count_stmt = select(func.count(Company.id))
            result = await session.execute(count_stmt)
            total = result.scalar()
            logger.info(f"Total companies in database: {total}")

    async def scrape_financials(self, limit: Optional[int] = None):
        """Scrape financial results for all companies."""
        logger.info("Step 2: Scraping financial results...")

        async with get_async_session() as session:
            # Get companies to scrape
            stmt = (
                select(Company)
                .where(Company.is_active == True)
                .order_by(Company.id)
            )
            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            companies = result.scalars().all()

            logger.info(f"Scraping financials for {len(companies)} companies...")

            nse_scraper = NSEFinancialScraper(session)
            bse_scraper = BSEFinancialScraper(session)

            for i, company in enumerate(companies):
                try:
                    # Try NSE first
                    if company.nse_symbol:
                        await nse_scraper.run(
                            symbol=company.nse_symbol,
                            company_id=company.id,
                        )
                        self.stats["financials_scraped"] += 1

                    # Also try BSE
                    elif company.bse_scrip_code:
                        await bse_scraper.run(
                            scrip_code=company.bse_scrip_code,
                            company_id=company.id,
                        )
                        self.stats["financials_scraped"] += 1

                    if (i + 1) % 10 == 0:
                        logger.info(f"Progress: {i + 1}/{len(companies)} companies")

                    # Rate limiting
                    await asyncio.sleep(1.5)

                except Exception as e:
                    logger.warning(f"Failed to scrape {company.nse_symbol or company.bse_scrip_code}: {e}")
                    self.stats["errors"] += 1

    async def scrape_prices(self, limit: Optional[int] = None, days: int = 30):
        """Scrape daily prices."""
        logger.info(f"Step 3: Scraping prices for last {days} days...")

        async with get_async_session() as session:
            scraper = PriceScraper(session)

            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            current_date = start_date
            while current_date <= end_date:
                # Skip weekends
                if current_date.weekday() < 5:
                    try:
                        result = await scraper.run(trade_date=current_date, use_bhavcopy=True)
                        self.stats["prices_scraped"] += result.get("records_inserted", 0)
                        logger.info(f"Prices for {current_date}: {result.get('records_inserted', 0)} records")
                    except Exception as e:
                        logger.warning(f"Failed to scrape prices for {current_date}: {e}")

                    await asyncio.sleep(1.0)

                current_date += timedelta(days=1)

    async def compute_all_ratios(self, limit: Optional[int] = None):
        """Compute financial ratios for all companies."""
        logger.info("Step 4: Computing financial ratios...")

        async with get_async_session() as session:
            # Get companies with financial data
            stmt = (
                select(Company)
                .where(Company.is_active == True)
                .order_by(Company.id)
            )
            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            companies = result.scalars().all()

            calculator = TTMCalculator(session)

            for company in companies:
                try:
                    # Compute TTM data
                    ttm_data = await calculator.compute_ttm(
                        company.id,
                        result_nature=ResultNature.CONSOLIDATED,
                    )

                    # Compute ratios
                    ratios = compute_ratios(ttm_data)

                    # Store in computed_ratios table
                    if ratios:
                        ratio_data = {
                            "company_id": company.id,
                            "period_end": date.today(),
                            "period_type": PeriodType.QUARTERLY,
                            "is_ttm": True,
                            "result_nature": ResultNature.CONSOLIDATED,
                            **{k: Decimal(str(v)) if v is not None else None for k, v in ratios.items()},
                        }

                        stmt = insert(ComputedRatio).values(**ratio_data)
                        stmt = stmt.on_conflict_do_update(
                            constraint="uq_computed_ratio",
                            set_=ratio_data,
                        )
                        await session.execute(stmt)
                        self.stats["ratios_computed"] += 1

                except Exception as e:
                    logger.debug(f"No ratio data for {company.nse_symbol}: {e}")

            await session.commit()
            logger.info(f"Computed ratios for {self.stats['ratios_computed']} companies")

    async def run_sample_population(self, sample_size: int = 10):
        """Run a quick sample population with a few top companies."""
        logger.info(f"=== Running Sample Population ({sample_size} companies) ===")

        # Popular NSE symbols for testing
        sample_symbols = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK",
            "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "SUNPHARMA",
        ][:sample_size]

        async with get_async_session() as session:
            # First, seed all companies to get the mapping
            await self.seed_companies()

            # Get the specific companies
            stmt = select(Company).where(Company.nse_symbol.in_(sample_symbols))
            result = await session.execute(stmt)
            companies = result.scalars().all()

            logger.info(f"Found {len(companies)} of {len(sample_symbols)} sample companies")

            # Scrape financials for these companies
            nse_scraper = NSEFinancialScraper(session)

            for company in companies:
                try:
                    logger.info(f"Scraping {company.nse_symbol}...")
                    await nse_scraper.run(
                        symbol=company.nse_symbol,
                        company_id=company.id,
                    )
                    self.stats["financials_scraped"] += 1
                    await asyncio.sleep(2.0)
                except Exception as e:
                    logger.warning(f"Failed: {e}")
                    self.stats["errors"] += 1

        # Compute ratios
        await self.compute_all_ratios(limit=sample_size)

        logger.info("=== Sample Population Complete ===")
        self._print_stats()

    def _print_stats(self):
        """Print population statistics."""
        print("\n" + "=" * 50)
        print("DATABASE POPULATION STATISTICS")
        print("=" * 50)
        print(f"Companies seeded:    {self.stats['companies_seeded']}")
        print(f"Financials scraped:  {self.stats['financials_scraped']}")
        print(f"Prices scraped:      {self.stats['prices_scraped']}")
        print(f"Ratios computed:     {self.stats['ratios_computed']}")
        print(f"Errors:              {self.stats['errors']}")
        print("=" * 50)


async def check_database_connection():
    """Verify database connection is working."""
    try:
        async with get_async_session() as session:
            result = await session.execute(select(1))
            result.scalar()
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure PostgreSQL is running and DATABASE_URL is set correctly")
        logger.error("Run: docker compose up -d db")
        return False


async def show_database_stats():
    """Show current database statistics."""
    async with get_async_session() as session:
        stats = {}

        # Count companies
        result = await session.execute(select(func.count(Company.id)))
        stats["companies"] = result.scalar()

        # Count financial statements
        result = await session.execute(select(func.count(FinancialStatement.id)))
        stats["financial_statements"] = result.scalar()

        # Count line items
        result = await session.execute(select(func.count(FinancialLineItem.id)))
        stats["line_items"] = result.scalar()

        # Count daily prices
        result = await session.execute(select(func.count(DailyPrice.id)))
        stats["daily_prices"] = result.scalar()

        # Count computed ratios
        result = await session.execute(select(func.count(ComputedRatio.id)))
        stats["computed_ratios"] = result.scalar()

        print("\n" + "=" * 50)
        print("CURRENT DATABASE STATISTICS")
        print("=" * 50)
        for table, count in stats.items():
            print(f"{table:25} {count:>10,}")
        print("=" * 50)

        return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Populate the Indian Stock Screener database"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full population pipeline",
    )
    parser.add_argument(
        "--companies",
        action="store_true",
        help="Only seed companies from NSE/BSE",
    )
    parser.add_argument(
        "--financials",
        action="store_true",
        help="Only scrape financial results",
    )
    parser.add_argument(
        "--prices",
        action="store_true",
        help="Only scrape daily prices",
    )
    parser.add_argument(
        "--ratios",
        action="store_true",
        help="Only compute financial ratios",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Run quick sample population (10 companies)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show current database statistics",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of companies to process",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database tables (run migrations)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    # Check database connection
    if not await check_database_connection():
        sys.exit(1)

    # Initialize DB if requested
    if args.init_db:
        logger.info("Initializing database...")
        await init_db()

    # Show stats if requested
    if args.stats:
        await show_database_stats()
        return

    # Create populator
    populator = DatabasePopulator()

    # Run requested operation
    if args.sample:
        await populator.run_sample_population(sample_size=10)
    elif args.full:
        await populator.run_full_population(limit=args.limit)
    elif args.companies:
        await populator.seed_companies()
    elif args.financials:
        await populator.scrape_financials(limit=args.limit)
    elif args.prices:
        await populator.scrape_prices(limit=args.limit)
    elif args.ratios:
        await populator.compute_all_ratios(limit=args.limit)
    else:
        # Default: show help
        parser.print_help()
        print("\n--- Current Database Status ---")
        await show_database_stats()


if __name__ == "__main__":
    asyncio.run(main())
