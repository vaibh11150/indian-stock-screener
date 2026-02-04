#!/usr/bin/env python
"""
Backfill HTML financial data for pre-2015 historical results.

Uses Playwright to fetch and parse HTML tables from NSE/BSE archives
where XBRL filings are not available.

Pre-2015 financial results were published as HTML tables on exchange websites.
This script scrapes those historical tables and normalizes them into our schema.
"""

import asyncio
import argparse
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from playwright.async_api import async_playwright, Browser, Page
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger, setup_logging
from config.settings import settings
from db import get_async_session
from db.models import (
    Company,
    FinancialLineItem,
    FinancialStatement,
    PeriodType,
    ResultNature,
    StatementType,
)
from scrapers.utils.html_table_parser import (
    parse_financial_html_table,
    extract_tables_from_html,
)
from scrapers.utils.normalizer import normalize_field, PL_FIELD_MAP, BS_FIELD_MAP

logger = get_logger(__name__)

# NSE historical results archive URLs
NSE_ARCHIVE_BASE = "https://archives.nseindia.com"
NSE_RESULTS_URL = f"{NSE_ARCHIVE_BASE}/content/corporates/Results/"

# BSE historical results archive
BSE_ARCHIVE_BASE = "https://www.bseindia.com"
BSE_RESULTS_URL = f"{BSE_ARCHIVE_BASE}/corporates/ann.aspx"

# Date range for HTML scraping (pre-XBRL era)
HTML_ERA_START = date(2000, 1, 1)
HTML_ERA_END = date(2015, 3, 31)


class HTMLBackfillScraper:
    """Scraper for pre-2015 HTML financial results."""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.browser: Optional[Browser] = None
        self.stats = {
            "companies_processed": 0,
            "statements_inserted": 0,
            "statements_skipped": 0,
            "errors": 0,
        }

    async def __aenter__(self):
        """Set up Playwright browser."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up browser resources."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def run(
        self,
        company_ids: Optional[list[int]] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        Run the HTML backfill process.

        Args:
            company_ids: Specific companies to backfill (or None for all)
            from_date: Start date for backfill (default: 2000-01-01)
            to_date: End date for backfill (default: 2015-03-31)
            limit: Maximum number of companies to process

        Returns:
            Statistics dict
        """
        from_date = from_date or HTML_ERA_START
        to_date = to_date or HTML_ERA_END

        logger.info(f"Starting HTML backfill from {from_date} to {to_date}")

        # Get companies to process
        companies = await self._get_companies(company_ids, limit)
        logger.info(f"Found {len(companies)} companies to process")

        for company in companies:
            try:
                await self._process_company(company, from_date, to_date)
                self.stats["companies_processed"] += 1
            except Exception as e:
                logger.error(f"Failed to process {company.nse_symbol}: {e}")
                self.stats["errors"] += 1

            # Rate limiting
            await asyncio.sleep(2.0)

        logger.info(f"HTML backfill complete: {self.stats}")
        return self.stats

    async def _get_companies(
        self,
        company_ids: Optional[list[int]],
        limit: int,
    ) -> list[Company]:
        """Get companies to process."""
        stmt = select(Company).where(Company.is_active == True)

        if company_ids:
            stmt = stmt.where(Company.id.in_(company_ids))
        else:
            # Prioritize companies with NSE symbols (better archive coverage)
            stmt = stmt.where(Company.nse_symbol.isnot(None))

        stmt = stmt.order_by(Company.id).limit(limit)

        result = await self.db_session.execute(stmt)
        return list(result.scalars().all())

    async def _process_company(
        self,
        company: Company,
        from_date: date,
        to_date: date,
    ) -> None:
        """Process a single company's historical results."""
        symbol = company.nse_symbol or company.bse_scrip_code
        logger.info(f"Processing {symbol} (ID: {company.id})")

        # Create browser page
        page = await self.browser.new_page()

        try:
            # Try NSE archive first
            if company.nse_symbol:
                await self._scrape_nse_archive(page, company, from_date, to_date)

            # Try BSE archive as fallback
            if company.bse_scrip_code:
                await self._scrape_bse_archive(page, company, from_date, to_date)

        finally:
            await page.close()

    async def _scrape_nse_archive(
        self,
        page: Page,
        company: Company,
        from_date: date,
        to_date: date,
    ) -> None:
        """Scrape NSE archive for historical results."""
        symbol = company.nse_symbol

        # NSE archive structure: /content/corporates/Results/SYMBOL/
        archive_url = f"{NSE_RESULTS_URL}{symbol}/"

        try:
            response = await page.goto(archive_url, wait_until="networkidle", timeout=30000)

            if not response or response.status != 200:
                logger.debug(f"NSE archive not found for {symbol}")
                return

            # Get all links to result files
            links = await page.query_selector_all("a[href*='.htm'], a[href*='.html']")

            for link in links:
                href = await link.get_attribute("href")
                if not href:
                    continue

                # Extract period info from filename (e.g., "Q1_2010.html")
                period_info = self._parse_filename_period(href)
                if not period_info:
                    continue

                period_end = period_info.get("end")
                if not period_end or not (from_date <= period_end <= to_date):
                    continue

                # Check if we already have this data
                if await self._statement_exists(company.id, period_end):
                    self.stats["statements_skipped"] += 1
                    continue

                # Fetch and parse the HTML file
                try:
                    file_url = f"{archive_url}{href}" if not href.startswith("http") else href
                    await page.goto(file_url, wait_until="networkidle", timeout=30000)

                    html_content = await page.content()
                    await self._parse_and_insert(company.id, html_content, period_info, "nse_html")

                except Exception as e:
                    logger.warning(f"Failed to fetch {href}: {e}")

                await asyncio.sleep(1.0)  # Rate limit

        except Exception as e:
            logger.warning(f"NSE archive scrape failed for {symbol}: {e}")

    async def _scrape_bse_archive(
        self,
        page: Page,
        company: Company,
        from_date: date,
        to_date: date,
    ) -> None:
        """Scrape BSE archive for historical results."""
        scrip_code = company.bse_scrip_code

        # BSE uses a different archive structure
        # Navigate to corporate announcements and filter by scrip code
        try:
            # BSE historical results URL pattern
            search_url = (
                f"{BSE_ARCHIVE_BASE}/corporates/ann.aspx?"
                f"scrip={scrip_code}&dur=3&dtefrom={from_date.strftime('%d/%m/%Y')}"
                f"&dteto={to_date.strftime('%d/%m/%Y')}&cat=results"
            )

            await page.goto(search_url, wait_until="networkidle", timeout=30000)

            # Find result announcement links
            result_links = await page.query_selector_all(
                "a[href*='Result'], a[href*='result'], a[href*='RESULT']"
            )

            for link in result_links:
                href = await link.get_attribute("href")
                link_text = await link.inner_text()

                if not href:
                    continue

                # Try to extract period from link text
                period_info = self._parse_period_from_text(link_text)
                if not period_info:
                    continue

                period_end = period_info.get("end")
                if not period_end or not (from_date <= period_end <= to_date):
                    continue

                # Check if we already have this data
                if await self._statement_exists(company.id, period_end):
                    self.stats["statements_skipped"] += 1
                    continue

                # Fetch and parse
                try:
                    file_url = href if href.startswith("http") else f"{BSE_ARCHIVE_BASE}{href}"
                    await page.goto(file_url, wait_until="networkidle", timeout=30000)

                    html_content = await page.content()
                    await self._parse_and_insert(company.id, html_content, period_info, "bse_html")

                except Exception as e:
                    logger.warning(f"Failed to fetch BSE result {href}: {e}")

                await asyncio.sleep(1.0)

        except Exception as e:
            logger.warning(f"BSE archive scrape failed for {scrip_code}: {e}")

    def _parse_filename_period(self, filename: str) -> Optional[dict]:
        """Parse period information from archive filename."""
        import re

        filename = filename.lower()

        # Pattern: Q1_2010.html, Q2FY11.htm, etc.
        quarter_match = re.search(r"q([1-4])[\s_]*(fy)?(\d{2,4})", filename)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year_str = quarter_match.group(3)
            if len(year_str) == 2:
                year_str = "20" + year_str if int(year_str) < 50 else "19" + year_str
            fy_year = int(year_str)

            # Calculate period end based on Indian fiscal year quarters
            quarter_end_month = {1: 6, 2: 9, 3: 12, 4: 3}
            quarter_end_year = {1: fy_year - 1, 2: fy_year - 1, 3: fy_year - 1, 4: fy_year}
            quarter_end_day = {1: 30, 2: 30, 3: 31, 4: 31}

            return {
                "end": date(
                    quarter_end_year[quarter],
                    quarter_end_month[quarter],
                    quarter_end_day[quarter],
                ),
                "type": "quarterly",
                "quarter": quarter,
                "fiscal_year": f"FY{fy_year}",
            }

        # Pattern: Annual_2010.html, FY2010.htm
        annual_match = re.search(r"(annual|fy)[\s_]*(\d{2,4})", filename)
        if annual_match:
            year_str = annual_match.group(2)
            if len(year_str) == 2:
                year_str = "20" + year_str if int(year_str) < 50 else "19" + year_str
            fy_year = int(year_str)

            return {
                "end": date(fy_year, 3, 31),
                "type": "annual",
                "fiscal_year": f"FY{fy_year}",
            }

        return None

    def _parse_period_from_text(self, text: str) -> Optional[dict]:
        """Parse period information from link text or announcement title."""
        from scrapers.utils.html_table_parser import parse_period_from_header
        return parse_period_from_header(text)

    async def _statement_exists(self, company_id: int, period_end: date) -> bool:
        """Check if we already have a statement for this period."""
        stmt = select(FinancialStatement.id).where(
            FinancialStatement.company_id == company_id,
            FinancialStatement.period_end == period_end,
        ).limit(1)

        result = await self.db_session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def _parse_and_insert(
        self,
        company_id: int,
        html_content: str,
        period_info: dict,
        source: str,
    ) -> None:
        """Parse HTML content and insert into database."""
        # Parse the HTML tables
        parsed = parse_financial_html_table(html_content)

        if not parsed or not parsed.get("data"):
            logger.debug("No financial data found in HTML")
            return

        # Get the first period's data (most recent in the table)
        data = parsed["data"][0] if parsed["data"] else {}

        if not data:
            return

        # Determine statement type from the data
        has_pl = any(k in data for k in ["revenue", "net_profit", "operating_profit"])
        has_bs = any(k in data for k in ["total_assets", "total_equity"])

        # Calculate fiscal year
        period_end = period_info["end"]
        fiscal_year = period_info.get("fiscal_year")
        if not fiscal_year:
            fiscal_year = f"FY{period_end.year}" if period_end.month >= 4 else f"FY{period_end.year}"

        # Calculate period start
        period_type_str = period_info.get("type", "quarterly")
        if period_type_str == "annual":
            period_start = date(period_end.year - 1, 4, 1)
            period_type = PeriodType.ANNUAL
        else:
            from dateutil.relativedelta import relativedelta
            period_start = period_end - relativedelta(months=3) + timedelta(days=1)
            period_type = PeriodType.QUARTERLY

        # Insert P&L statement if we have P&L data
        if has_pl:
            await self._insert_statement(
                company_id=company_id,
                statement_type=StatementType.PROFIT_LOSS,
                period_start=period_start,
                period_end=period_end,
                period_type=period_type,
                fiscal_year=fiscal_year,
                fiscal_quarter=period_info.get("quarter"),
                source=source,
                items={k: v for k, v in data.items() if k in PL_FIELD_MAP or normalize_field(k, PL_FIELD_MAP)},
            )

        # Insert BS statement if we have BS data
        if has_bs:
            await self._insert_statement(
                company_id=company_id,
                statement_type=StatementType.BALANCE_SHEET,
                period_start=period_start,
                period_end=period_end,
                period_type=period_type,
                fiscal_year=fiscal_year,
                fiscal_quarter=period_info.get("quarter"),
                source=source,
                items={k: v for k, v in data.items() if k in BS_FIELD_MAP or normalize_field(k, BS_FIELD_MAP)},
            )

    async def _insert_statement(
        self,
        company_id: int,
        statement_type: StatementType,
        period_start: date,
        period_end: date,
        period_type: PeriodType,
        fiscal_year: str,
        fiscal_quarter: Optional[int],
        source: str,
        items: dict[str, float],
    ) -> None:
        """Insert a financial statement and its line items."""
        try:
            # Upsert the statement
            stmt_data = {
                "company_id": company_id,
                "statement_type": statement_type,
                "result_nature": ResultNature.STANDALONE,  # Pre-2015 was mostly standalone
                "period_type": period_type,
                "period_start": period_start,
                "period_end": period_end,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "is_audited": period_type == PeriodType.ANNUAL,
                "source": source,
            }

            stmt = insert(FinancialStatement).values(**stmt_data)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_financial_statement",
                set_={
                    "source": stmt.excluded.source,
                },
            )
            stmt = stmt.returning(FinancialStatement.id)
            result = await self.db_session.execute(stmt)
            statement_id = result.scalar_one()

            # Insert line items
            for field_name, field_value in items.items():
                if field_value is None:
                    continue

                # Normalize field name if needed
                canonical = normalize_field(field_name) or field_name

                line_item_stmt = insert(FinancialLineItem).values(
                    statement_id=statement_id,
                    field_name=canonical,
                    field_value=Decimal(str(field_value)),
                )
                line_item_stmt = line_item_stmt.on_conflict_do_update(
                    constraint="uq_line_item",
                    set_={"field_value": line_item_stmt.excluded.field_value},
                )
                await self.db_session.execute(line_item_stmt)

            await self.db_session.commit()
            self.stats["statements_inserted"] += 1
            logger.info(f"Inserted {statement_type.value} for company {company_id}, period {period_end}")

        except Exception as e:
            await self.db_session.rollback()
            logger.error(f"Failed to insert statement: {e}")
            self.stats["errors"] += 1


async def main():
    """Main entry point for HTML backfill script."""
    parser = argparse.ArgumentParser(
        description="Backfill pre-2015 HTML financial data"
    )
    parser.add_argument(
        "--company-ids",
        type=str,
        help="Comma-separated list of company IDs to process",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default="2000-01-01",
        help="Start date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default="2015-03-31",
        help="End date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of companies to process",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    # Parse arguments
    company_ids = None
    if args.company_ids:
        company_ids = [int(x.strip()) for x in args.company_ids.split(",")]

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)

    # Run the backfill
    async with get_async_session() as session:
        async with HTMLBackfillScraper(session) as scraper:
            stats = await scraper.run(
                company_ids=company_ids,
                from_date=from_date,
                to_date=to_date,
                limit=args.limit,
            )

    print("\n=== HTML Backfill Complete ===")
    print(f"Companies processed: {stats['companies_processed']}")
    print(f"Statements inserted: {stats['statements_inserted']}")
    print(f"Statements skipped (already exist): {stats['statements_skipped']}")
    print(f"Errors: {stats['errors']}")


if __name__ == "__main__":
    asyncio.run(main())
