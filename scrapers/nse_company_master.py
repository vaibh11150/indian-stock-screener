"""
NSE Company Master Data Scraper.

Fetches the list of all NSE-listed equities from the official NSE equity list CSV.
"""

from io import StringIO
from typing import Any, Optional

import pandas as pd
from dateutil.parser import parse as parse_date
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import Company
from scrapers.base import BaseScraper
from scrapers.utils.session_manager import NSESession

logger = get_logger(__name__)

# NSE publishes a CSV of all listed equities
NSE_EQUITY_LIST_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"


class NSECompanyMasterScraper(BaseScraper):
    """Scraper for NSE company master data."""

    SCRAPER_NAME = "nse_company_master"

    def __init__(self, session: Optional[AsyncSession] = None):
        super().__init__(session)

    async def _scrape(self, **kwargs) -> list[dict[str, Any]]:
        """
        Fetch and process NSE equity list.

        Returns:
            List of company data dicts
        """
        companies = []

        async with NSESession() as nse_session:
            # Fetch the CSV
            csv_content = await nse_session.get(
                NSE_EQUITY_LIST_URL,
                raw_response=True,
            )

            # Parse CSV
            df = pd.read_csv(StringIO(csv_content))
            self.increment_scraped(len(df))

            # Process each row
            for _, row in df.iterrows():
                try:
                    company = self._parse_row(row)
                    if company:
                        companies.append(company)
                except Exception as e:
                    self.log_error(f"Failed to parse row: {e}")

        # Insert/update in database
        if self.db_session and companies:
            await self._upsert_companies(companies)

        return companies

    def _parse_row(self, row: pd.Series) -> Optional[dict[str, Any]]:
        """Parse a row from the NSE equity list CSV."""
        # CSV columns: SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING,
        #              PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE

        symbol = str(row.get("SYMBOL", "")).strip()
        company_name = str(row.get(" NAME OF COMPANY", row.get("NAME OF COMPANY", ""))).strip()
        isin = str(row.get("ISIN NUMBER", row.get(" ISIN NUMBER", ""))).strip()
        series = str(row.get(" SERIES", row.get("SERIES", ""))).strip()

        # Skip non-equity series
        if series not in ["EQ", "BE", "SM", "ST", "BZ"]:
            return None

        if not symbol or not isin:
            return None

        # Parse listing date
        listing_date = None
        date_str = str(row.get(" DATE OF LISTING", row.get("DATE OF LISTING", ""))).strip()
        if date_str and date_str != "nan":
            try:
                listing_date = parse_date(date_str, dayfirst=True).date()
            except Exception:
                pass

        # Parse face value
        face_value = None
        fv_str = str(row.get(" FACE VALUE", row.get("FACE VALUE", ""))).strip()
        if fv_str and fv_str != "nan":
            try:
                face_value = float(fv_str)
            except Exception:
                pass

        return {
            "nse_symbol": symbol,
            "isin": isin,
            "company_name": company_name,
            "listing_date": listing_date,
            "face_value": face_value,
        }

    async def _upsert_companies(self, companies: list[dict[str, Any]]) -> None:
        """Insert or update companies in the database."""
        for company_data in companies:
            try:
                # Use PostgreSQL upsert
                stmt = insert(Company).values(**company_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["isin"],
                    set_={
                        "nse_symbol": stmt.excluded.nse_symbol,
                        "company_name": stmt.excluded.company_name,
                        "listing_date": stmt.excluded.listing_date,
                        "face_value": stmt.excluded.face_value,
                    },
                )
                await self.db_session.execute(stmt)
                self.increment_inserted()
            except Exception as e:
                self.log_error(f"Failed to upsert {company_data.get('nse_symbol')}: {e}")

        await self.db_session.commit()


async def fetch_nse_company_list() -> pd.DataFrame:
    """
    Fetch the full NSE equity list as a DataFrame.

    Standalone function for quick access without DB integration.
    """
    async with NSESession() as session:
        csv_content = await session.get(NSE_EQUITY_LIST_URL, raw_response=True)
        return pd.read_csv(StringIO(csv_content))
