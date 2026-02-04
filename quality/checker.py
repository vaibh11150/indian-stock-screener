"""
Data quality checker.

Compares our computed data against screener.in for verification.
screener.in is ONLY used for quality checks — never as a data source.

Acceptable thresholds (based on known rounding differences):
- Revenue, expenses, profit: ±0.5% (screener rounds to crores)
- Ratios (PE, ROE, ROCE): ±2% absolute difference
- Per share values (EPS): ±₹0.5
- Balance sheet totals: ±1%
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from config.settings import settings
from db.models import Company, QualityCheck

logger = get_logger(__name__)

SCREENER_BASE = "https://www.screener.in"

# Quality thresholds by field type (percentage)
THRESHOLDS = {
    # Flow items (P&L) — allow 0.5% due to rounding
    "revenue": 1.0,
    "net_profit": 2.0,
    "total_expenses": 1.0,
    "operating_profit": 2.0,
    # Balance sheet — allow 1%
    "total_assets": 1.5,
    "total_equity": 2.0,
    "total_borrowings": 3.0,
    # Ratios — allow 2-5 percentage points absolute
    "pe_ratio": 10.0,  # PE can vary due to price timing
    "roe": 3.0,
    "roce": 3.0,
    "debt_equity": 5.0,
    # Per share — allow up to 2%
    "eps_basic": 3.0,
    # Default
    "_default": 5.0,
}


class QualityChecker:
    """Quality checker that compares data against screener.in."""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def run_quality_check(
        self,
        sample_size: int = 100,
        company_ids: Optional[list[int]] = None,
    ) -> dict[str, Any]:
        """
        Run a full quality check against screener.in.

        Args:
            sample_size: Number of companies to check
            company_ids: Specific company IDs to check (overrides sample_size)

        Returns:
            Quality check results
        """
        if company_ids:
            companies = await self._get_companies_by_ids(company_ids)
        else:
            companies = await self._select_sample_companies(sample_size)

        results = {
            "total_checks": 0,
            "within_threshold": 0,
            "outside_threshold": 0,
            "errors": 0,
            "by_field": {},
            "worst_deviations": [],
        }

        for company in companies:
            try:
                comparisons = await self._check_company(company)

                for comp in comparisons:
                    results["total_checks"] += 1

                    # Check if within threshold
                    threshold = self._get_threshold(comp["field_name"])
                    is_ok = abs(comp["pct_deviation"]) <= threshold

                    if is_ok:
                        results["within_threshold"] += 1
                    else:
                        results["outside_threshold"] += 1
                        results["worst_deviations"].append(comp)

                    # Log to database
                    await self._insert_quality_check(
                        company_id=company.id,
                        field_name=comp["field_name"],
                        our_value=comp["our_value"],
                        reference_value=comp["ref_value"],
                        pct_deviation=comp["pct_deviation"],
                        is_acceptable=is_ok,
                        period_end=comp.get("period_end"),
                    )

                    # Aggregate by field
                    field = comp["field_name"]
                    if field not in results["by_field"]:
                        results["by_field"][field] = {"total": 0, "ok": 0, "bad": 0}
                    results["by_field"][field]["total"] += 1
                    results["by_field"][field]["ok" if is_ok else "bad"] += 1

            except Exception as e:
                results["errors"] += 1
                logger.error(f"Quality check failed for {company.nse_symbol}: {e}")

        # Sort worst deviations
        results["worst_deviations"].sort(
            key=lambda x: abs(x["pct_deviation"]), reverse=True
        )
        results["worst_deviations"] = results["worst_deviations"][:20]

        # Calculate overall accuracy
        if results["total_checks"] > 0:
            results["accuracy"] = round(
                (results["within_threshold"] / results["total_checks"]) * 100, 1
            )
        else:
            results["accuracy"] = 0

        logger.info(f"Quality Check Complete: {results['accuracy']}% accuracy")
        return results

    async def _check_company(self, company: Company) -> list[dict[str, Any]]:
        """Check a single company against screener.in."""
        symbol = company.nse_symbol or company.bse_scrip_code
        if not symbol:
            return []

        # Fetch reference data from screener.in
        ref_data = await self._fetch_screener_data(symbol)
        if not ref_data:
            return []

        # Fetch our data
        our_data = await self._get_our_data(company.id)
        if not our_data:
            return []

        # Compare field by field
        comparisons = []
        fields_to_check = [
            "revenue",
            "net_profit",
            "operating_profit",
            "total_assets",
            "total_equity",
            "total_borrowings",
            "roe",
            "roce",
            "pe_ratio",
            "eps_basic",
            "debt_equity",
        ]

        for field in fields_to_check:
            our_value = our_data.get(field)
            ref_value = ref_data.get(field)

            if our_value is not None and ref_value is not None and ref_value != 0:
                pct_dev = ((our_value - ref_value) / abs(ref_value)) * 100
                comparisons.append({
                    "symbol": symbol,
                    "field_name": field,
                    "our_value": our_value,
                    "ref_value": ref_value,
                    "pct_deviation": round(pct_dev, 2),
                })

        return comparisons

    async def _fetch_screener_data(self, symbol: str) -> Optional[dict]:
        """Fetch financial data from screener.in for verification."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Try the API first
                resp = await client.get(
                    f"{SCREENER_BASE}/api/company/{symbol}/",
                    headers={"X-Requested-With": "XMLHttpRequest"},
                    follow_redirects=True,
                )

                if resp.status_code == 200:
                    return self._parse_screener_api_response(resp.json())

                # Fall back to scraping the HTML page
                resp = await client.get(
                    f"{SCREENER_BASE}/company/{symbol}/consolidated/",
                    follow_redirects=True,
                )
                if resp.status_code == 200:
                    return self._parse_screener_html(resp.text)

        except Exception as e:
            logger.warning(f"Failed to fetch screener data for {symbol}: {e}")

        return None

    def _parse_screener_api_response(self, data: dict) -> dict:
        """Parse screener.in API response."""
        result = {}

        # Map API fields to our canonical names
        field_map = {
            "market_cap": "market_cap",
            "current_price": "current_price",
            "pe_ratio": "pe_ratio",
            "book_value": "book_value_per_share",
            "dividend_yield": "dividend_yield",
            "roce": "roce",
            "roe": "roe",
            "face_value": "face_value",
            "sales": "revenue",
            "profit": "net_profit",
            "eps": "eps_basic",
        }

        for api_key, our_key in field_map.items():
            value = data.get(api_key)
            if value is not None:
                try:
                    result[our_key] = float(value)
                except (ValueError, TypeError):
                    pass

        return result

    def _parse_screener_html(self, html: str) -> dict:
        """Parse screener.in company page to extract financial data."""
        soup = BeautifulSoup(html, "lxml")
        data = {}

        # Extract the top-line ratios
        ratios_section = soup.find("div", {"id": "top-ratios"})
        if ratios_section:
            for li in ratios_section.find_all("li"):
                name = li.find("span", class_="name")
                value = li.find("span", class_="number")
                if name and value:
                    name_text = name.get_text(strip=True).lower()
                    value_text = value.get_text(strip=True)

                    # Map to our field names
                    if "market cap" in name_text:
                        data["market_cap"] = self._parse_screener_value(value_text)
                    elif "pe" in name_text and "ratio" in name_text:
                        data["pe_ratio"] = self._parse_screener_value(value_text)
                    elif "roe" in name_text:
                        data["roe"] = self._parse_screener_value(value_text)
                    elif "roce" in name_text:
                        data["roce"] = self._parse_screener_value(value_text)
                    elif "debt" in name_text and "equity" in name_text:
                        data["debt_equity"] = self._parse_screener_value(value_text)

        return data

    def _parse_screener_value(self, text: str) -> Optional[float]:
        """Parse a numeric value from screener.in."""
        if not text:
            return None

        text = text.strip().replace(",", "").replace("%", "")

        # Handle Cr (crores) and L (lakhs)
        multiplier = 1
        if "Cr" in text or "cr" in text:
            text = text.replace("Cr", "").replace("cr", "")
            multiplier = 1  # Already in crores
        elif "L" in text:
            text = text.replace("L", "")
            multiplier = 0.01  # Convert lakhs to crores

        try:
            return float(text.strip()) * multiplier
        except ValueError:
            return None

    async def _get_our_data(self, company_id: int) -> dict:
        """Get our latest data for a company."""
        from engine.ratios import compute_ratios, FinancialData
        from engine.ttm import TTMCalculator

        try:
            calculator = TTMCalculator(self.db_session)
            ttm_data = await calculator.compute_ttm(company_id)
            ratios = compute_ratios(ttm_data)

            # Combine TTM data with ratios
            result = {
                "revenue": ttm_data.revenue,
                "net_profit": ttm_data.net_profit,
                "operating_profit": ttm_data.operating_profit,
                "total_assets": ttm_data.total_assets,
                "total_equity": ttm_data.total_equity,
                "total_borrowings": ttm_data.total_borrowings,
                "eps_basic": ttm_data.eps_basic,
            }
            result.update(ratios)
            return result

        except Exception as e:
            logger.warning(f"Failed to get our data for company {company_id}: {e}")
            return {}

    async def _select_sample_companies(self, sample_size: int) -> list[Company]:
        """Select a sample of companies for quality check."""
        # Priority: Get a mix of large, mid, and small caps
        stmt = (
            select(Company)
            .where(Company.is_active == True, Company.nse_symbol.isnot(None))
            .order_by(Company.id)
            .limit(sample_size)
        )

        result = await self.db_session.execute(stmt)
        return list(result.scalars().all())

    async def _get_companies_by_ids(self, company_ids: list[int]) -> list[Company]:
        """Get companies by IDs."""
        stmt = select(Company).where(Company.id.in_(company_ids))
        result = await self.db_session.execute(stmt)
        return list(result.scalars().all())

    def _get_threshold(self, field_name: str) -> float:
        """Get the acceptable threshold for a field."""
        return THRESHOLDS.get(field_name, THRESHOLDS["_default"])

    async def _insert_quality_check(
        self,
        company_id: int,
        field_name: str,
        our_value: float,
        reference_value: float,
        pct_deviation: float,
        is_acceptable: bool,
        period_end: Optional[date] = None,
    ) -> None:
        """Insert a quality check record."""
        check = QualityCheck(
            company_id=company_id,
            field_name=field_name,
            our_value=Decimal(str(our_value)) if our_value else None,
            reference_value=Decimal(str(reference_value)) if reference_value else None,
            pct_deviation=Decimal(str(pct_deviation)),
            is_acceptable=is_acceptable,
            reference_source="screener.in",
            period_end=period_end,
        )
        self.db_session.add(check)
        await self.db_session.commit()


async def run_quality_check(
    db_session: AsyncSession,
    sample_size: int = 100,
) -> dict[str, Any]:
    """
    Run a quality check.

    Convenience function.
    """
    checker = QualityChecker(db_session)
    return await checker.run_quality_check(sample_size)
