"""
NSE Financial Results Scraper.

Fetches financial results from NSE's API endpoints.
The primary endpoint is /api/results-comparision which returns pre-parsed financial data.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from dateutil.parser import parse as parse_date
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import (
    Company,
    FinancialLineItem,
    FinancialStatement,
    PeriodType,
    ResultNature,
    StatementType,
)
from scrapers.base import BaseScraper
from scrapers.utils.normalizer import normalize_field, PL_FIELD_MAP, BS_FIELD_MAP
from scrapers.utils.session_manager import NSESession

logger = get_logger(__name__)


class NSEFinancialScraper(BaseScraper):
    """Scraper for NSE financial results."""

    SCRAPER_NAME = "nse_financial"

    def __init__(self, session: Optional[AsyncSession] = None):
        super().__init__(session)

    async def _scrape(
        self,
        symbol: Optional[str] = None,
        company_id: Optional[int] = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Fetch financial results for a company.

        Args:
            symbol: NSE symbol to scrape
            company_id: Company ID in database

        Returns:
            List of financial result records
        """
        if not symbol:
            self.log_error("Symbol is required")
            return []

        results = []

        async with NSESession() as nse_session:
            # Fetch from results-comparison endpoint (best structured data)
            try:
                data = await nse_session.get(
                    "results-comparision",
                    params={"symbol": symbol},
                )

                if data and isinstance(data, dict):
                    parsed = self._parse_results_comparison(data)
                    results.extend(parsed)
                    self.increment_scraped(len(parsed))
            except Exception as e:
                self.log_error(f"Failed to fetch results-comparision: {e}")

            # Also try corporate filings for additional data
            try:
                filings = await nse_session.get(
                    "corporates-financial-results",
                    params={"index": "equities", "symbol": symbol},
                )

                if filings and isinstance(filings, list):
                    for filing in filings:
                        parsed = self._parse_corporate_filing(filing)
                        if parsed and parsed not in results:
                            results.append(parsed)
                            self.increment_scraped()
            except Exception as e:
                logger.debug(f"Corporate filings fetch failed (may be expected): {e}")

        # Insert into database
        if self.db_session and results and company_id:
            await self._insert_financial_data(company_id, results)

        return results

    def _parse_results_comparison(self, data: dict) -> list[dict[str, Any]]:
        """
        Parse the results-comparison API response.

        The response contains arrays for each financial metric across periods.
        Structure:
        {
            "symbol": "RELIANCE",
            "companyName": "Reliance Industries Limited",
            "periodDates": ["Jun 2024", "Mar 2024", ...],
            "revenue": [152345, 148765, ...],
            "operatingProfit": [...],
            ...
        }
        """
        results = []

        period_dates = data.get("periodDates", data.get("dates", []))
        if not period_dates:
            return results

        # Extract all available metrics
        metric_mappings = {
            # NSE field name -> canonical field name
            "revenue": "revenue",
            "revenueFromOperations": "revenue",
            "otherIncome": "other_income",
            "totalExpenses": "total_expenses",
            "operatingProfit": "operating_profit",
            "ebitda": "operating_profit",
            "depreciationAndAmortisation": "depreciation",
            "financeCost": "interest_expense",
            "profitBeforeTax": "profit_before_tax",
            "taxExpense": "tax_expense",
            "profitAfterTax": "net_profit",
            "netProfit": "net_profit",
            "basicEPS": "eps_basic",
            "dilutedEPS": "eps_diluted",
            # Balance sheet items (if available)
            "totalAssets": "total_assets",
            "totalEquity": "total_equity",
            "totalBorrowings": "total_borrowings",
            "cashAndEquivalents": "cash_and_equivalents",
        }

        for i, period_str in enumerate(period_dates):
            period_info = self._parse_period_string(period_str)
            if not period_info:
                continue

            items = {}
            for nse_field, canonical in metric_mappings.items():
                values = data.get(nse_field, [])
                if i < len(values):
                    try:
                        value = values[i]
                        if value is not None and value != "" and value != "-":
                            items[canonical] = float(value)
                    except (ValueError, TypeError):
                        pass

            if items:
                results.append({
                    "period_end": period_info.get("end"),
                    "period_start": period_info.get("start"),
                    "period_type": period_info.get("type", "quarterly"),
                    "fiscal_year": period_info.get("fiscal_year"),
                    "fiscal_quarter": period_info.get("quarter"),
                    "is_audited": period_info.get("type") == "annual",
                    "source": "nse_api",
                    "statement_type": "profit_loss",
                    "result_nature": "consolidated",  # NSE usually shows consolidated
                    "items": items,
                })

        return results

    def _parse_corporate_filing(self, filing: dict) -> Optional[dict[str, Any]]:
        """Parse a corporate filing record."""
        # Extract basic info
        period_end = None
        if filing.get("toDate"):
            try:
                period_end = parse_date(filing["toDate"], dayfirst=True).date()
            except Exception:
                pass

        if not period_end:
            return None

        return {
            "period_end": period_end,
            "filing_date": filing.get("submittedDate"),
            "source": "nse_filing",
            "xbrl_url": filing.get("xbrlFile"),
            "is_audited": "audited" in str(filing.get("auditStatus", "")).lower(),
        }

    def _parse_period_string(self, period_str: str) -> Optional[dict]:
        """
        Parse a period string like 'Jun 2024' or 'Mar 2024'.
        """
        from scrapers.utils.html_table_parser import parse_period_from_header
        return parse_period_from_header(period_str)

    async def _insert_financial_data(
        self,
        company_id: int,
        results: list[dict[str, Any]],
    ) -> None:
        """Insert financial data into the database."""
        for result in results:
            try:
                # Determine statement type and result nature
                stmt_type = StatementType(result.get("statement_type", "profit_loss"))
                nature = ResultNature(result.get("result_nature", "consolidated"))
                period_type = self._get_period_type(result.get("period_type", "quarterly"))

                # Calculate fiscal year if not provided
                fiscal_year = result.get("fiscal_year")
                if not fiscal_year and result.get("period_end"):
                    fiscal_year = self._calculate_fiscal_year(result["period_end"])

                # Upsert financial statement
                stmt_data = {
                    "company_id": company_id,
                    "statement_type": stmt_type,
                    "result_nature": nature,
                    "period_type": period_type,
                    "period_start": result.get("period_start"),
                    "period_end": result["period_end"],
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": result.get("fiscal_quarter"),
                    "is_audited": result.get("is_audited", False),
                    "source": result.get("source", "nse_api"),
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
                        "filing_date": stmt.excluded.filing_date,
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
        # Indian fiscal year: Apr 1 - Mar 31
        # FY2024 = Apr 1, 2023 to Mar 31, 2024
        if period_end.month >= 4:
            fy = period_end.year + 1
        else:
            fy = period_end.year
        return f"FY{fy}"


async def fetch_nse_financial_results(symbol: str) -> dict[str, Any]:
    """
    Fetch financial results for an NSE symbol.

    Standalone function for quick access.
    """
    async with NSESession() as session:
        return await session.get("results-comparision", params={"symbol": symbol})
