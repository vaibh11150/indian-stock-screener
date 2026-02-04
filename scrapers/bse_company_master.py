"""
BSE Company Master Data Scraper.

Fetches the list of all BSE-listed equities from the BSE API.
"""

from typing import Any, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import Company
from scrapers.base import BaseScraper
from scrapers.utils.session_manager import BSESession

logger = get_logger(__name__)

# BSE API endpoint for scrip list
BSE_SCRIP_LIST_ENDPOINT = "ListofScripData/w"


class BSECompanyMasterScraper(BaseScraper):
    """Scraper for BSE company master data."""

    SCRAPER_NAME = "bse_company_master"

    def __init__(self, session: Optional[AsyncSession] = None):
        super().__init__(session)

    async def _scrape(self, **kwargs) -> list[dict[str, Any]]:
        """
        Fetch and process BSE scrip list.

        Returns:
            List of company data dicts
        """
        companies = []

        async with BSESession() as bse_session:
            # Fetch the scrip list
            # Parameters: Group=&Atea=&Flag= (empty values return all)
            data = await bse_session.get(
                BSE_SCRIP_LIST_ENDPOINT,
                params={"Group": "", "Atea": "", "Flag": ""},
            )

            if not isinstance(data, list):
                self.log_error(f"Unexpected BSE response format: {type(data)}")
                return []

            self.increment_scraped(len(data))

            # Process each item
            for item in data:
                try:
                    company = self._parse_item(item)
                    if company:
                        companies.append(company)
                except Exception as e:
                    self.log_error(f"Failed to parse item: {e}")

        # Insert/update in database
        if self.db_session and companies:
            await self._upsert_companies(companies)

        return companies

    def _parse_item(self, item: dict) -> Optional[dict[str, Any]]:
        """Parse an item from the BSE scrip list JSON."""
        # BSE API returns items like:
        # {
        #   "SCRIP_CD": "500325",
        #   "Scrip_Name": "RELIANCE",
        #   "Status": "Active",
        #   "GROUP": "A",
        #   "FACE_VALUE": "10.00",
        #   "ISIN_NUMBER": "INE002A01018",
        #   "INDUSTRY": "Refineries",
        #   "scrip_id": "500325",
        #   "Scrip_Name_1": "RELIANCE INDUSTRIES LTD."
        # }

        scrip_code = str(item.get("SCRIP_CD", item.get("scrip_id", ""))).strip()
        isin = str(item.get("ISIN_NUMBER", item.get("Isin_Number", ""))).strip()
        company_name = str(
            item.get("Scrip_Name_1", item.get("Scrip_Name", item.get("Long_Name", "")))
        ).strip()
        status = str(item.get("Status", "")).strip()
        group = str(item.get("GROUP", item.get("Group", ""))).strip()
        industry = str(item.get("INDUSTRY", item.get("Industry", ""))).strip()

        # Skip inactive or non-standard scrips
        if status.lower() not in ["active", ""]:
            return None

        if not scrip_code or not isin:
            return None

        # Validate ISIN format (should start with INE for Indian securities)
        if not isin.startswith("INE") and not isin.startswith("IN"):
            return None

        # Parse face value
        face_value = None
        fv_str = str(item.get("FACE_VALUE", item.get("Face_Value", ""))).strip()
        if fv_str and fv_str != "nan":
            try:
                face_value = float(fv_str)
            except Exception:
                pass

        return {
            "bse_scrip_code": scrip_code,
            "isin": isin,
            "company_name": company_name,
            "bse_group": group if group else None,
            "industry": industry if industry and industry != "nan" else None,
            "face_value": face_value,
            "is_active": True,
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
                        "bse_scrip_code": stmt.excluded.bse_scrip_code,
                        "company_name": stmt.excluded.company_name,
                        "bse_group": stmt.excluded.bse_group,
                        "industry": stmt.excluded.industry,
                        "face_value": stmt.excluded.face_value,
                        "is_active": stmt.excluded.is_active,
                    },
                )
                await self.db_session.execute(stmt)
                self.increment_inserted()
            except Exception as e:
                self.log_error(f"Failed to upsert {company_data.get('bse_scrip_code')}: {e}")

        await self.db_session.commit()


async def fetch_bse_company_list() -> list[dict]:
    """
    Fetch the full BSE scrip list.

    Standalone function for quick access without DB integration.
    """
    async with BSESession() as session:
        data = await session.get(
            BSE_SCRIP_LIST_ENDPOINT,
            params={"Group": "", "Atea": "", "Flag": ""},
        )
        return data if isinstance(data, list) else []
