"""
BSE Financial Results Scraper.

Fetches financial results from BSE's API and XBRL files.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from dateutil.parser import parse as parse_date
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import (
    FinancialLineItem,
    FinancialStatement,
    PeriodType,
    RawFiling,
    ResultNature,
    StatementType,
)
from scrapers.base import BaseScraper
from scrapers.utils.normalizer import normalize_field
from scrapers.utils.session_manager import BSESession
from scrapers.utils.xbrl_parser import parse_xbrl_financial_result

logger = get_logger(__name__)


class BSEFinancialScraper(BaseScraper):
    """Scraper for BSE financial results."""

    SCRAPER_NAME = "bse_financial"

    def __init__(self, session: Optional[AsyncSession] = None):
        super().__init__(session)

    async def _scrape(
        self,
        scrip_code: Optional[str] = None,
        company_id: Optional[int] = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Fetch financial results for a BSE scrip.

        Args:
            scrip_code: BSE scrip code
            company_id: Company ID in database

        Returns:
            List of financial result records
        """
        if not scrip_code:
            self.log_error("Scrip code is required")
            return []

        results = []

        async with BSESession() as bse_session:
            # Fetch financial results listing
            try:
                data = await bse_session.get(
                    "FinancialResult/w",
                    params={"Atea": "", "Flag": "0", "scripcode": scrip_code},
                )

                if data and isinstance(data, list):
                    for item in data:
                        parsed = self._parse_financial_result(item)
                        if parsed:
                            results.append(parsed)
                            self.increment_scraped()
            except Exception as e:
                self.log_error(f"Failed to fetch financial results: {e}")

            # Try to fetch additional details from annual reports endpoint
            try:
                annual_data = await bse_session.get(
                    "AnnualReport/w",
                    params={"scripcode": scrip_code, "flag": "0"},
                )

                if annual_data and isinstance(annual_data, list):
                    for item in annual_data:
                        parsed = self._parse_annual_report(item)
                        if parsed:
                            # Merge with existing or add new
                            self._merge_result(results, parsed)
                            self.increment_scraped()
            except Exception as e:
                logger.debug(f"Annual reports fetch failed (may be expected): {e}")

        # Insert into database
        if self.db_session and results and company_id:
            await self._insert_financial_data(company_id, results)

        return results

    def _parse_financial_result(self, item: dict) -> Optional[dict[str, Any]]:
        """Parse a financial result item from BSE API."""
        # BSE API response structure:
        # {
        #   "DATE": "30-06-2024",
        #   "QTR_TYPE": "Q1",
        #   "TYPE_OF_MEETING": "BOARD MEETING",
        #   "ATTACHMENT": "...",
        #   "ATTACHMENT_NAME": "...",
        #   "XBRL": "...",
        #   "AUDITED_STATUS": "Unaudited",
        #   "CONSOLIDATED_STANDALONE": "Consolidated",
        #   ...
        # }

        # Parse period end date
        date_str = item.get("DATE", item.get("TO_DATE", ""))
        period_end = None
        if date_str:
            try:
                period_end = parse_date(date_str, dayfirst=True).date()
            except Exception:
                pass

        if not period_end:
            return None

        # Determine period type
        qtr_type = str(item.get("QTR_TYPE", "")).upper()
        period_type = "quarterly"
        fiscal_quarter = None

        if qtr_type in ["Q1", "Q2", "Q3", "Q4"]:
            fiscal_quarter = int(qtr_type[1])
        elif "ANNUAL" in qtr_type or "YEARLY" in qtr_type:
            period_type = "annual"
        elif "HALF" in qtr_type:
            period_type = "half_yearly"

        # Determine result nature
        nature_str = str(item.get("CONSOLIDATED_STANDALONE", "")).lower()
        result_nature = "consolidated" if "consolidated" in nature_str else "standalone"

        # Audited status
        is_audited = "audited" in str(item.get("AUDITED_STATUS", "")).lower()
        if "unaudited" in str(item.get("AUDITED_STATUS", "")).lower():
            is_audited = False

        # Extract XBRL URL if available
        xbrl_url = item.get("XBRL", item.get("XBRLFILE", ""))

        return {
            "period_end": period_end,
            "period_type": period_type,
            "fiscal_quarter": fiscal_quarter,
            "result_nature": result_nature,
            "is_audited": is_audited,
            "source": "bse_api",
            "xbrl_url": xbrl_url if xbrl_url else None,
            "filing_date": None,  # Could be parsed from item if available
            "items": {},  # Financial items would come from XBRL parsing
        }

    def _parse_annual_report(self, item: dict) -> Optional[dict[str, Any]]:
        """Parse an annual report item from BSE API."""
        # Similar structure to financial results
        date_str = item.get("FROM_DATE", "")
        to_date_str = item.get("TO_DATE", "")

        period_end = None
        if to_date_str:
            try:
                period_end = parse_date(to_date_str, dayfirst=True).date()
            except Exception:
                pass

        if not period_end:
            return None

        return {
            "period_end": period_end,
            "period_type": "annual",
            "result_nature": "consolidated",
            "is_audited": True,
            "source": "bse_annual",
            "items": {},
        }

    def _merge_result(
        self,
        results: list[dict[str, Any]],
        new_result: dict[str, Any],
    ) -> None:
        """Merge a new result with existing results."""
        for existing in results:
            if (
                existing["period_end"] == new_result["period_end"]
                and existing["result_nature"] == new_result["result_nature"]
            ):
                # Merge items
                existing["items"].update(new_result.get("items", {}))
                return

        # No match found, add new
        results.append(new_result)

    async def _insert_financial_data(
        self,
        company_id: int,
        results: list[dict[str, Any]],
    ) -> None:
        """Insert financial data into the database."""
        for result in results:
            try:
                # Determine enums
                stmt_type = StatementType.PROFIT_LOSS  # Default, could be determined from content
                nature = ResultNature(result.get("result_nature", "consolidated"))
                period_type = self._get_period_type(result.get("period_type", "quarterly"))

                # Calculate fiscal year
                fiscal_year = self._calculate_fiscal_year(result["period_end"])

                # Calculate period start
                period_start = self._calculate_period_start(
                    result["period_end"],
                    result.get("period_type", "quarterly"),
                )

                # Upsert financial statement
                stmt_data = {
                    "company_id": company_id,
                    "statement_type": stmt_type,
                    "result_nature": nature,
                    "period_type": period_type,
                    "period_start": period_start,
                    "period_end": result["period_end"],
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": result.get("fiscal_quarter"),
                    "is_audited": result.get("is_audited", False),
                    "source": result.get("source", "bse_api"),
                    "source_url": result.get("xbrl_url"),
                    "filing_date": result.get("filing_date"),
                }

                stmt = insert(FinancialStatement).values(**stmt_data)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_financial_statement",
                    set_={
                        "is_audited": stmt.excluded.is_audited,
                        "source": stmt.excluded.source,
                        "source_url": stmt.excluded.source_url,
                    },
                )
                stmt = stmt.returning(FinancialStatement.id)
                result_row = await self.db_session.execute(stmt)
                statement_id = result_row.scalar_one()

                # Insert line items
                items = result.get("items", {})
                for field_name, field_value in items.items():
                    if field_value is None:
                        continue

                    line_item_stmt = insert(FinancialLineItem).values(
                        statement_id=statement_id,
                        field_name=field_name,
                        field_value=Decimal(str(field_value)),
                    )
                    line_item_stmt = line_item_stmt.on_conflict_do_update(
                        constraint="uq_line_item",
                        set_={"field_value": line_item_stmt.excluded.field_value},
                    )
                    await self.db_session.execute(line_item_stmt)

                self.increment_inserted()

            except Exception as e:
                self.log_error(f"Failed to insert financial data: {e}")

        await self.db_session.commit()

    def _get_period_type(self, period_type_str: str) -> PeriodType:
        """Convert string period type to enum."""
        mapping = {
            "quarterly": PeriodType.QUARTERLY,
            "half_yearly": PeriodType.HALF_YEARLY,
            "nine_months": PeriodType.NINE_MONTHS,
            "annual": PeriodType.ANNUAL,
        }
        return mapping.get(period_type_str, PeriodType.QUARTERLY)

    def _calculate_fiscal_year(self, period_end: date) -> str:
        """Calculate fiscal year string from period end date."""
        if period_end.month >= 4:
            fy = period_end.year + 1
        else:
            fy = period_end.year
        return f"FY{fy}"

    def _calculate_period_start(self, period_end: date, period_type: str) -> date:
        """Calculate period start from end date."""
        from dateutil.relativedelta import relativedelta

        if period_type == "quarterly":
            return period_end - relativedelta(months=3) + relativedelta(days=1)
        elif period_type == "half_yearly":
            return period_end - relativedelta(months=6) + relativedelta(days=1)
        elif period_type == "nine_months":
            return period_end - relativedelta(months=9) + relativedelta(days=1)
        else:  # annual
            return period_end - relativedelta(years=1) + relativedelta(days=1)


async def fetch_bse_financial_results(scrip_code: str) -> list[dict]:
    """
    Fetch financial results for a BSE scrip.

    Standalone function for quick access.
    """
    async with BSESession() as session:
        data = await session.get(
            "FinancialResult/w",
            params={"Atea": "", "Flag": "0", "scripcode": scrip_code},
        )
        return data if isinstance(data, list) else []
