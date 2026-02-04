"""
Financial statements API endpoints.
"""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, get_company_by_symbol
from api.schemas import FinancialResponse, FinancialPeriod, TTMResponse
from db.models import (
    Company,
    FinancialStatement,
    FinancialLineItem,
    StatementType,
    ResultNature,
    PeriodType,
)
from engine.ttm import TTMCalculator

router = APIRouter()


@router.get("/{symbol}", response_model=FinancialResponse)
async def get_financials(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    statement_type: str = Query("profit_loss", enum=["profit_loss", "balance_sheet", "cash_flow"]),
    nature: str = Query("consolidated", enum=["standalone", "consolidated"]),
    period_type: str = Query("annual", enum=["quarterly", "annual"]),
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    limit: int = Query(40, ge=1, le=100),
):
    """
    Get financial statements for a company.

    Returns timestamped data with source provenance.
    """
    company = await get_company_by_symbol(symbol, db)

    # Build query
    stmt = (
        select(FinancialStatement)
        .where(
            FinancialStatement.company_id == company.id,
            FinancialStatement.statement_type == StatementType(statement_type),
            FinancialStatement.result_nature == ResultNature(nature),
            FinancialStatement.period_type == PeriodType(period_type),
        )
        .order_by(FinancialStatement.period_end.desc())
        .limit(limit)
    )

    if from_date:
        stmt = stmt.where(FinancialStatement.period_end >= from_date)
    if to_date:
        stmt = stmt.where(FinancialStatement.period_end <= to_date)

    result = await db.execute(stmt)
    statements = result.scalars().all()

    # Fetch line items for each statement
    periods = []
    for statement in statements:
        items_stmt = select(FinancialLineItem).where(
            FinancialLineItem.statement_id == statement.id
        )
        items_result = await db.execute(items_stmt)
        line_items = items_result.scalars().all()

        items_dict = {
            item.field_name: float(item.field_value) if item.field_value else None
            for item in line_items
        }

        periods.append(
            FinancialPeriod(
                period_end=statement.period_end,
                period_start=statement.period_start,
                fiscal_year=statement.fiscal_year,
                fiscal_quarter=statement.fiscal_quarter,
                is_audited=statement.is_audited,
                source=statement.source,
                filing_date=statement.filing_date,
                items=items_dict,
            )
        )

    return FinancialResponse(
        symbol=company.nse_symbol or company.bse_scrip_code,
        company_name=company.company_name,
        statement_type=statement_type,
        nature=nature,
        periods=periods,
        data_timestamp=datetime.now(),
    )


@router.get("/{symbol}/ttm", response_model=TTMResponse)
async def get_ttm_financials(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    nature: str = Query("consolidated", enum=["standalone", "consolidated"]),
):
    """
    Get trailing twelve months (TTM) financial data.

    Sums last 4 quarters for flow items, uses latest for stock items.
    """
    company = await get_company_by_symbol(symbol, db)

    try:
        calculator = TTMCalculator(db)
        ttm_data = await calculator.compute_ttm(
            company.id,
            result_nature=ResultNature(nature),
        )

        # Convert FinancialData to dict
        ttm_dict = {
            "revenue": ttm_data.revenue,
            "other_income": ttm_data.other_income,
            "total_expenses": ttm_data.total_expenses,
            "operating_profit": ttm_data.operating_profit,
            "depreciation": ttm_data.depreciation,
            "interest_expense": ttm_data.interest_expense,
            "profit_before_tax": ttm_data.profit_before_tax,
            "tax_expense": ttm_data.tax_expense,
            "net_profit": ttm_data.net_profit,
            "eps_basic": ttm_data.eps_basic,
            "eps_diluted": ttm_data.eps_diluted,
            "total_assets": ttm_data.total_assets,
            "total_equity": ttm_data.total_equity,
            "total_borrowings": ttm_data.total_borrowings,
            "cash_and_equivalents": ttm_data.cash_and_equivalents,
            "cfo": ttm_data.cfo,
            "cfi": ttm_data.cfi,
            "cff": ttm_data.cff,
        }

        # Filter out None/zero values
        ttm_dict = {k: v for k, v in ttm_dict.items() if v is not None and v != 0}

        return TTMResponse(
            symbol=company.nse_symbol or company.bse_scrip_code,
            company_name=company.company_name,
            nature=nature,
            ttm_data=ttm_dict,
            data_timestamp=datetime.now(),
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not compute TTM: {str(e)}",
        )


@router.get("/{symbol}/comparison")
async def compare_periods(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    periods: str = Query(..., description="Comma-separated period_ends: 2024-03-31,2023-03-31"),
    statement_type: str = Query("profit_loss"),
    nature: str = Query("consolidated", enum=["standalone", "consolidated"]),
):
    """
    Side-by-side comparison of multiple periods.

    Useful for YoY analysis.
    """
    company = await get_company_by_symbol(symbol, db)

    # Parse period dates
    try:
        period_dates = [
            datetime.strptime(p.strip(), "%Y-%m-%d").date()
            for p in periods.split(",")
        ]
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD",
        )

    comparison = {}

    for period_date in period_dates:
        stmt = (
            select(FinancialStatement)
            .where(
                FinancialStatement.company_id == company.id,
                FinancialStatement.statement_type == StatementType(statement_type),
                FinancialStatement.result_nature == ResultNature(nature),
                FinancialStatement.period_end == period_date,
            )
            .limit(1)
        )

        result = await db.execute(stmt)
        statement = result.scalar_one_or_none()

        if statement:
            items_stmt = select(FinancialLineItem).where(
                FinancialLineItem.statement_id == statement.id
            )
            items_result = await db.execute(items_stmt)
            line_items = items_result.scalars().all()

            comparison[str(period_date)] = {
                item.field_name: float(item.field_value) if item.field_value else None
                for item in line_items
            }
        else:
            comparison[str(period_date)] = None

    return {
        "symbol": company.nse_symbol or company.bse_scrip_code,
        "company_name": company.company_name,
        "statement_type": statement_type,
        "nature": nature,
        "comparison": comparison,
        "data_timestamp": datetime.now().isoformat(),
    }
