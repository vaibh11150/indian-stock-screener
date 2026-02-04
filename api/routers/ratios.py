"""
Financial ratios API endpoints.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, get_company_by_symbol
from api.schemas import RatiosResponse, RatioPeriod
from db.models import Company, ComputedRatio, PeriodType, ResultNature
from engine.ratios import compute_ratios, FinancialData
from engine.ttm import TTMCalculator

router = APIRouter()


@router.get("/{symbol}", response_model=RatiosResponse)
async def get_ratios(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    period_type: str = Query("annual", enum=["quarterly", "annual", "ttm"]),
    nature: str = Query("consolidated", enum=["standalone", "consolidated"]),
    limit: int = Query(10, ge=1, le=40),
):
    """
    Get computed financial ratios for a company.

    Returns valuation, profitability, efficiency, and leverage ratios.
    """
    company = await get_company_by_symbol(symbol, db)

    ratios_list = []

    if period_type == "ttm":
        # Compute TTM ratios on the fly
        try:
            calculator = TTMCalculator(db)
            ttm_data = await calculator.compute_ttm(
                company.id,
                result_nature=ResultNature(nature),
            )
            ratios_dict = compute_ratios(ttm_data)

            ratios_list.append(
                RatioPeriod(
                    period_end=datetime.now().date(),
                    period_type="ttm",
                    is_ttm=True,
                    **ratios_dict,
                )
            )
        except Exception:
            pass  # Return empty if TTM can't be computed

    else:
        # Get from computed_ratios table
        stmt = (
            select(ComputedRatio)
            .where(
                ComputedRatio.company_id == company.id,
                ComputedRatio.period_type == PeriodType(period_type),
                ComputedRatio.result_nature == ResultNature(nature),
            )
            .order_by(ComputedRatio.period_end.desc())
            .limit(limit)
        )

        result = await db.execute(stmt)
        ratios = result.scalars().all()

        for ratio in ratios:
            ratios_list.append(
                RatioPeriod(
                    period_end=ratio.period_end,
                    period_type=period_type,
                    is_ttm=ratio.is_ttm,
                    market_cap=float(ratio.market_cap) if ratio.market_cap else None,
                    pe_ratio=float(ratio.pe_ratio) if ratio.pe_ratio else None,
                    pb_ratio=float(ratio.pb_ratio) if ratio.pb_ratio else None,
                    ev=float(ratio.ev) if ratio.ev else None,
                    ev_ebitda=float(ratio.ev_ebitda) if ratio.ev_ebitda else None,
                    dividend_yield=float(ratio.dividend_yield) if ratio.dividend_yield else None,
                    roe=float(ratio.roe) if ratio.roe else None,
                    roce=float(ratio.roce) if ratio.roce else None,
                    roa=float(ratio.roa) if ratio.roa else None,
                    operating_margin=float(ratio.operating_margin) if ratio.operating_margin else None,
                    net_margin=float(ratio.net_margin) if ratio.net_margin else None,
                    asset_turnover=float(ratio.asset_turnover) if ratio.asset_turnover else None,
                    inventory_days=float(ratio.inventory_days) if ratio.inventory_days else None,
                    receivable_days=float(ratio.receivable_days) if ratio.receivable_days else None,
                    payable_days=float(ratio.payable_days) if ratio.payable_days else None,
                    cash_conversion_cycle=float(ratio.cash_conversion_cycle) if ratio.cash_conversion_cycle else None,
                    debt_equity=float(ratio.debt_equity) if ratio.debt_equity else None,
                    current_ratio=float(ratio.current_ratio) if ratio.current_ratio else None,
                    interest_coverage=float(ratio.interest_coverage) if ratio.interest_coverage else None,
                    revenue_growth=float(ratio.revenue_growth) if ratio.revenue_growth else None,
                    profit_growth=float(ratio.profit_growth) if ratio.profit_growth else None,
                    eps=float(ratio.eps) if ratio.eps else None,
                    book_value_per_share=float(ratio.book_value_per_share) if ratio.book_value_per_share else None,
                )
            )

    return RatiosResponse(
        symbol=company.nse_symbol or company.bse_scrip_code,
        ratios=ratios_list,
        data_timestamp=datetime.now(),
    )


@router.get("/{symbol}/compute")
async def compute_ratios_endpoint(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    nature: str = Query("consolidated", enum=["standalone", "consolidated"]),
):
    """
    Compute ratios on-the-fly using TTM data.

    Useful for getting the most current ratios without relying on pre-computed values.
    """
    company = await get_company_by_symbol(symbol, db)

    try:
        calculator = TTMCalculator(db)
        ttm_data = await calculator.compute_ttm(
            company.id,
            result_nature=ResultNature(nature),
        )
        ratios = compute_ratios(ttm_data)

        return {
            "symbol": company.nse_symbol or company.bse_scrip_code,
            "company_name": company.company_name,
            "computation_type": "ttm",
            "nature": nature,
            "ratios": ratios,
            "data_timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        return {
            "symbol": company.nse_symbol or company.bse_scrip_code,
            "error": str(e),
            "data_timestamp": datetime.now().isoformat(),
        }
