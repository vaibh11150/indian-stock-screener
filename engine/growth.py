"""
Growth computation engine.

Computes YoY, QoQ, and CAGR growth rates for financial metrics.
"""

from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import (
    FinancialLineItem,
    FinancialStatement,
    PeriodType,
    ResultNature,
    StatementType,
)

logger = get_logger(__name__)


class GrowthCalculator:
    """Calculator for growth metrics."""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def compute_yoy_growth(
        self,
        company_id: int,
        field_name: str,
        period_end: date,
        period_type: PeriodType = PeriodType.ANNUAL,
        result_nature: ResultNature = ResultNature.CONSOLIDATED,
    ) -> Optional[float]:
        """
        Compute year-over-year growth for a field.

        Args:
            company_id: Company ID
            field_name: Field to compute growth for (e.g., 'revenue', 'net_profit')
            period_end: Current period end date
            period_type: Period type
            result_nature: Standalone or consolidated

        Returns:
            YoY growth percentage or None
        """
        # Get current period value
        current_value = await self._get_field_value(
            company_id, field_name, period_end, period_type, result_nature
        )

        if current_value is None:
            return None

        # Get previous year value
        from dateutil.relativedelta import relativedelta

        previous_period_end = period_end - relativedelta(years=1)
        previous_value = await self._get_field_value(
            company_id, field_name, previous_period_end, period_type, result_nature
        )

        if previous_value is None or previous_value == 0:
            return None

        # Calculate growth
        growth = ((current_value - previous_value) / abs(previous_value)) * 100
        return round(growth, 2)

    async def compute_qoq_growth(
        self,
        company_id: int,
        field_name: str,
        period_end: date,
        result_nature: ResultNature = ResultNature.CONSOLIDATED,
    ) -> Optional[float]:
        """
        Compute quarter-over-quarter growth for a field.

        Args:
            company_id: Company ID
            field_name: Field to compute growth for
            period_end: Current quarter end date
            result_nature: Standalone or consolidated

        Returns:
            QoQ growth percentage or None
        """
        # Get current quarter value
        current_value = await self._get_field_value(
            company_id, field_name, period_end, PeriodType.QUARTERLY, result_nature
        )

        if current_value is None:
            return None

        # Get previous quarter value
        from dateutil.relativedelta import relativedelta

        previous_period_end = period_end - relativedelta(months=3)
        previous_value = await self._get_field_value(
            company_id, field_name, previous_period_end, PeriodType.QUARTERLY, result_nature
        )

        if previous_value is None or previous_value == 0:
            return None

        # Calculate growth
        growth = ((current_value - previous_value) / abs(previous_value)) * 100
        return round(growth, 2)

    async def compute_cagr(
        self,
        company_id: int,
        field_name: str,
        start_date: date,
        end_date: date,
        result_nature: ResultNature = ResultNature.CONSOLIDATED,
    ) -> Optional[float]:
        """
        Compute Compound Annual Growth Rate (CAGR).

        CAGR = (End Value / Start Value)^(1/years) - 1

        Args:
            company_id: Company ID
            field_name: Field to compute CAGR for
            start_date: Start period end date
            end_date: End period end date
            result_nature: Standalone or consolidated

        Returns:
            CAGR percentage or None
        """
        # Get values
        start_value = await self._get_field_value(
            company_id, field_name, start_date, PeriodType.ANNUAL, result_nature
        )
        end_value = await self._get_field_value(
            company_id, field_name, end_date, PeriodType.ANNUAL, result_nature
        )

        if start_value is None or end_value is None:
            return None

        if start_value <= 0 or end_value <= 0:
            return None

        # Calculate years
        years = (end_date - start_date).days / 365.25

        if years <= 0:
            return None

        # Calculate CAGR
        cagr = (pow(end_value / start_value, 1 / years) - 1) * 100
        return round(cagr, 2)

    async def compute_all_growth_metrics(
        self,
        company_id: int,
        period_end: date,
        period_type: PeriodType = PeriodType.ANNUAL,
        result_nature: ResultNature = ResultNature.CONSOLIDATED,
    ) -> dict[str, Optional[float]]:
        """
        Compute all growth metrics for a company.

        Args:
            company_id: Company ID
            period_end: Current period end date
            period_type: Period type
            result_nature: Standalone or consolidated

        Returns:
            Dict of growth metrics
        """
        from dateutil.relativedelta import relativedelta

        metrics = {}

        # Revenue growth
        metrics["revenue_growth"] = await self.compute_yoy_growth(
            company_id, "revenue", period_end, period_type, result_nature
        )

        # Profit growth
        metrics["profit_growth"] = await self.compute_yoy_growth(
            company_id, "net_profit", period_end, period_type, result_nature
        )

        # EPS growth
        metrics["eps_growth"] = await self.compute_yoy_growth(
            company_id, "eps_basic", period_end, period_type, result_nature
        )

        # EBITDA growth
        metrics["ebitda_growth"] = await self.compute_yoy_growth(
            company_id, "operating_profit", period_end, period_type, result_nature
        )

        # 3-year and 5-year CAGR for revenue and profit
        three_years_ago = period_end - relativedelta(years=3)
        five_years_ago = period_end - relativedelta(years=5)

        metrics["revenue_cagr_3yr"] = await self.compute_cagr(
            company_id, "revenue", three_years_ago, period_end, result_nature
        )
        metrics["revenue_cagr_5yr"] = await self.compute_cagr(
            company_id, "revenue", five_years_ago, period_end, result_nature
        )
        metrics["profit_cagr_3yr"] = await self.compute_cagr(
            company_id, "net_profit", three_years_ago, period_end, result_nature
        )
        metrics["profit_cagr_5yr"] = await self.compute_cagr(
            company_id, "net_profit", five_years_ago, period_end, result_nature
        )

        return metrics

    async def _get_field_value(
        self,
        company_id: int,
        field_name: str,
        period_end: date,
        period_type: PeriodType,
        result_nature: ResultNature,
    ) -> Optional[float]:
        """Get a specific field value from the database."""
        # Allow some date tolerance (± 45 days) to handle fiscal year variations
        from datetime import timedelta

        date_lower = period_end - timedelta(days=45)
        date_upper = period_end + timedelta(days=45)

        stmt = (
            select(FinancialLineItem.field_value)
            .join(FinancialStatement)
            .where(
                FinancialStatement.company_id == company_id,
                FinancialStatement.period_type == period_type,
                FinancialStatement.result_nature == result_nature,
                FinancialStatement.period_end.between(date_lower, date_upper),
                FinancialLineItem.field_name == field_name,
            )
            .order_by(FinancialStatement.period_end.desc())
            .limit(1)
        )

        result = await self.db_session.execute(stmt)
        value = result.scalar_one_or_none()

        return float(value) if value is not None else None


async def compute_growth(
    db_session: AsyncSession,
    company_id: int,
    period_end: date,
    period_type: PeriodType = PeriodType.ANNUAL,
    result_nature: ResultNature = ResultNature.CONSOLIDATED,
) -> dict[str, Optional[float]]:
    """
    Convenience function to compute all growth metrics.

    Args:
        db_session: Database session
        company_id: Company ID
        period_end: Current period end date
        period_type: Period type
        result_nature: Standalone or consolidated

    Returns:
        Dict of growth metrics
    """
    calculator = GrowthCalculator(db_session)
    return await calculator.compute_all_growth_metrics(
        company_id, period_end, period_type, result_nature
    )
