"""
Trailing Twelve Months (TTM) computation engine.

Computes TTM values by summing the last 4 quarters.
Screener.in uses TTM for PE, EPS, and other flow-based ratios.

For balance sheet items (stock items), use the latest quarter's values.
For P&L and cash flow items (flow items), sum the last 4 quarters.
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
from engine.ratios import FinancialData

logger = get_logger(__name__)

# Fields that are flow items (sum over periods)
FLOW_FIELDS = {
    # P&L
    "revenue",
    "other_income",
    "total_income",
    "raw_material_cost",
    "employee_cost",
    "total_expenses",
    "operating_profit",
    "depreciation",
    "interest_expense",
    "profit_before_exceptional",
    "exceptional_items",
    "profit_before_tax",
    "tax_expense",
    "net_profit",
    # Cash Flow
    "cfo",
    "cfi",
    "cff",
    "net_cash_flow",
    "capex",
}

# Fields that are stock items (use latest value)
STOCK_FIELDS = {
    # Balance Sheet
    "share_capital",
    "reserves_surplus",
    "total_equity",
    "minority_interest",
    "long_term_borrowings",
    "short_term_borrowings",
    "total_borrowings",
    "total_current_liabilities",
    "total_non_current_liabilities",
    "total_liabilities",
    "total_current_assets",
    "total_non_current_assets",
    "total_assets",
    "fixed_assets",
    "cwip",
    "intangible_assets",
    "investments",
    "non_current_investments",
    "current_investments",
    "inventory",
    "trade_receivables",
    "trade_payables",
    "cash_and_equivalents",
    "goodwill",
}


class TTMCalculator:
    """Calculator for trailing twelve months financial data."""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def compute_ttm(
        self,
        company_id: int,
        as_of_date: Optional[date] = None,
        result_nature: ResultNature = ResultNature.CONSOLIDATED,
    ) -> FinancialData:
        """
        Compute TTM financials by summing last 4 quarterly P&L/CF results
        and using the latest balance sheet.

        Args:
            company_id: Company ID
            as_of_date: Reference date (default: today)
            result_nature: Standalone or consolidated

        Returns:
            FinancialData with TTM values
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Fetch last 4 quarterly P&L results
        quarters = await self._get_last_n_quarters(
            company_id,
            n=4,
            before=as_of_date,
            statement_type=StatementType.PROFIT_LOSS,
            result_nature=result_nature,
        )

        if len(quarters) < 4:
            logger.warning(
                f"Only {len(quarters)} quarters available for TTM (company_id={company_id})"
            )

        # Initialize TTM data
        ttm_data = {}

        # Sum flow items from all quarters
        for quarter in quarters:
            for field_name, field_value in quarter.items():
                if field_name in FLOW_FIELDS and field_value is not None:
                    ttm_data[field_name] = ttm_data.get(field_name, 0) + float(field_value)

        # Get latest balance sheet for stock items
        latest_bs = await self._get_latest_statement(
            company_id,
            before=as_of_date,
            statement_type=StatementType.BALANCE_SHEET,
            result_nature=result_nature,
        )

        if latest_bs:
            for field_name, field_value in latest_bs.items():
                if field_name in STOCK_FIELDS and field_value is not None:
                    ttm_data[field_name] = float(field_value)

        # Get latest cash flow for any missing CF items
        latest_cf = await self._get_latest_statement(
            company_id,
            before=as_of_date,
            statement_type=StatementType.CASH_FLOW,
            result_nature=result_nature,
        )

        # Sum CF from quarters if available, else use annual
        if latest_cf:
            for field_name in ["cfo", "cfi", "cff", "net_cash_flow", "capex"]:
                if field_name not in ttm_data or ttm_data.get(field_name, 0) == 0:
                    if field_name in latest_cf and latest_cf[field_name] is not None:
                        ttm_data[field_name] = float(latest_cf[field_name])

        # Calculate EPS for TTM = sum of quarterly EPS
        # (Note: This is an approximation; true TTM EPS should be recalculated)
        ttm_eps = sum(
            float(q.get("eps_basic", 0) or 0)
            for q in quarters
        )
        if ttm_eps != 0:
            ttm_data["eps_basic"] = ttm_eps

        ttm_eps_diluted = sum(
            float(q.get("eps_diluted", 0) or 0)
            for q in quarters
        )
        if ttm_eps_diluted != 0:
            ttm_data["eps_diluted"] = ttm_eps_diluted

        return FinancialData.from_dict(ttm_data)

    async def _get_last_n_quarters(
        self,
        company_id: int,
        n: int,
        before: date,
        statement_type: StatementType,
        result_nature: ResultNature,
    ) -> list[dict]:
        """Get the last N quarterly statements."""
        stmt = (
            select(FinancialStatement)
            .where(
                FinancialStatement.company_id == company_id,
                FinancialStatement.statement_type == statement_type,
                FinancialStatement.result_nature == result_nature,
                FinancialStatement.period_type == PeriodType.QUARTERLY,
                FinancialStatement.period_end <= before,
            )
            .order_by(FinancialStatement.period_end.desc())
            .limit(n)
        )

        result = await self.db_session.execute(stmt)
        statements = result.scalars().all()

        quarters = []
        for statement in statements:
            items = await self._get_statement_items(statement.id)
            quarters.append(items)

        return quarters

    async def _get_latest_statement(
        self,
        company_id: int,
        before: date,
        statement_type: StatementType,
        result_nature: ResultNature,
    ) -> Optional[dict]:
        """Get the latest statement of a given type."""
        stmt = (
            select(FinancialStatement)
            .where(
                FinancialStatement.company_id == company_id,
                FinancialStatement.statement_type == statement_type,
                FinancialStatement.result_nature == result_nature,
                FinancialStatement.period_end <= before,
            )
            .order_by(FinancialStatement.period_end.desc())
            .limit(1)
        )

        result = await self.db_session.execute(stmt)
        statement = result.scalar_one_or_none()

        if statement:
            return await self._get_statement_items(statement.id)
        return None

    async def _get_statement_items(self, statement_id: int) -> dict:
        """Get all line items for a statement."""
        stmt = select(FinancialLineItem).where(
            FinancialLineItem.statement_id == statement_id
        )
        result = await self.db_session.execute(stmt)
        items = result.scalars().all()

        return {item.field_name: item.field_value for item in items}


async def compute_ttm(
    db_session: AsyncSession,
    company_id: int,
    as_of_date: Optional[date] = None,
    result_nature: ResultNature = ResultNature.CONSOLIDATED,
) -> FinancialData:
    """
    Convenience function to compute TTM.

    Args:
        db_session: Database session
        company_id: Company ID
        as_of_date: Reference date
        result_nature: Standalone or consolidated

    Returns:
        FinancialData with TTM values
    """
    calculator = TTMCalculator(db_session)
    return await calculator.compute_ttm(company_id, as_of_date, result_nature)
