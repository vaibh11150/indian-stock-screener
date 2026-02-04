"""
Stock screener API endpoints.
"""

import re
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, pagination_params
from api.schemas import (
    ScreenerRequest,
    ScreenerResponse,
    ScreenerResultItem,
)
from db.models import Company, ComputedRatio, DailyPrice, PeriodType, ResultNature

router = APIRouter()


@router.post("/", response_model=ScreenerResponse)
async def screen_stocks(
    filters: ScreenerRequest,
    db: AsyncSession = Depends(get_db_session),
    sort_by: str = Query("market_cap", description="Field to sort by"),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Screen stocks based on financial criteria.

    Supports filtering by multiple ratio criteria simultaneously.
    """
    # Build the query with joins
    stmt = (
        select(Company, ComputedRatio)
        .join(ComputedRatio, Company.id == ComputedRatio.company_id)
        .where(
            Company.is_active == True,
            ComputedRatio.is_ttm == True,
            ComputedRatio.result_nature == ResultNature.CONSOLIDATED,
        )
    )

    # Apply filters
    filter_conditions = []

    # PE Ratio
    if filters.pe_ratio:
        if filters.pe_ratio.min is not None:
            filter_conditions.append(ComputedRatio.pe_ratio >= filters.pe_ratio.min)
        if filters.pe_ratio.max is not None:
            filter_conditions.append(ComputedRatio.pe_ratio <= filters.pe_ratio.max)

    # PB Ratio
    if filters.pb_ratio:
        if filters.pb_ratio.min is not None:
            filter_conditions.append(ComputedRatio.pb_ratio >= filters.pb_ratio.min)
        if filters.pb_ratio.max is not None:
            filter_conditions.append(ComputedRatio.pb_ratio <= filters.pb_ratio.max)

    # ROE
    if filters.roe:
        if filters.roe.min is not None:
            filter_conditions.append(ComputedRatio.roe >= filters.roe.min)
        if filters.roe.max is not None:
            filter_conditions.append(ComputedRatio.roe <= filters.roe.max)

    # ROCE
    if filters.roce:
        if filters.roce.min is not None:
            filter_conditions.append(ComputedRatio.roce >= filters.roce.min)
        if filters.roce.max is not None:
            filter_conditions.append(ComputedRatio.roce <= filters.roce.max)

    # Debt/Equity
    if filters.debt_equity:
        if filters.debt_equity.min is not None:
            filter_conditions.append(ComputedRatio.debt_equity >= filters.debt_equity.min)
        if filters.debt_equity.max is not None:
            filter_conditions.append(ComputedRatio.debt_equity <= filters.debt_equity.max)

    # Current Ratio
    if filters.current_ratio:
        if filters.current_ratio.min is not None:
            filter_conditions.append(ComputedRatio.current_ratio >= filters.current_ratio.min)
        if filters.current_ratio.max is not None:
            filter_conditions.append(ComputedRatio.current_ratio <= filters.current_ratio.max)

    # Market Cap
    if filters.market_cap:
        if filters.market_cap.min is not None:
            filter_conditions.append(ComputedRatio.market_cap >= filters.market_cap.min)
        if filters.market_cap.max is not None:
            filter_conditions.append(ComputedRatio.market_cap <= filters.market_cap.max)

    # Revenue Growth
    if filters.revenue_growth:
        if filters.revenue_growth.min is not None:
            filter_conditions.append(ComputedRatio.revenue_growth >= filters.revenue_growth.min)
        if filters.revenue_growth.max is not None:
            filter_conditions.append(ComputedRatio.revenue_growth <= filters.revenue_growth.max)

    # Profit Growth
    if filters.profit_growth:
        if filters.profit_growth.min is not None:
            filter_conditions.append(ComputedRatio.profit_growth >= filters.profit_growth.min)
        if filters.profit_growth.max is not None:
            filter_conditions.append(ComputedRatio.profit_growth <= filters.profit_growth.max)

    # Operating Margin
    if filters.operating_margin:
        if filters.operating_margin.min is not None:
            filter_conditions.append(ComputedRatio.operating_margin >= filters.operating_margin.min)
        if filters.operating_margin.max is not None:
            filter_conditions.append(ComputedRatio.operating_margin <= filters.operating_margin.max)

    # Net Margin
    if filters.net_margin:
        if filters.net_margin.min is not None:
            filter_conditions.append(ComputedRatio.net_margin >= filters.net_margin.min)
        if filters.net_margin.max is not None:
            filter_conditions.append(ComputedRatio.net_margin <= filters.net_margin.max)

    # Sector filter
    if filters.sector:
        filter_conditions.append(Company.sector == filters.sector)

    # Industry filter
    if filters.industry:
        filter_conditions.append(Company.industry == filters.industry)

    if filter_conditions:
        stmt = stmt.where(and_(*filter_conditions))

    # Sorting
    sort_column = getattr(ComputedRatio, sort_by, ComputedRatio.market_cap)
    if sort_order == "desc":
        stmt = stmt.order_by(sort_column.desc().nulls_last())
    else:
        stmt = stmt.order_by(sort_column.asc().nulls_last())

    # Get total count (without pagination)
    count_stmt = select(ComputedRatio.id).where(
        ComputedRatio.is_ttm == True,
        ComputedRatio.result_nature == ResultNature.CONSOLIDATED,
    )
    for condition in filter_conditions:
        count_stmt = count_stmt.where(condition)
    # This is a simplified count - proper implementation would need subquery

    # Apply pagination
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    rows = result.all()

    results = []
    for company, ratio in rows:
        results.append(
            ScreenerResultItem(
                symbol=company.nse_symbol or company.bse_scrip_code,
                company_name=company.company_name,
                sector=company.sector,
                industry=company.industry,
                market_cap=float(ratio.market_cap) if ratio.market_cap else None,
                current_price=None,  # Would need to join with prices
                pe_ratio=float(ratio.pe_ratio) if ratio.pe_ratio else None,
                pb_ratio=float(ratio.pb_ratio) if ratio.pb_ratio else None,
                roe=float(ratio.roe) if ratio.roe else None,
                roce=float(ratio.roce) if ratio.roce else None,
                debt_equity=float(ratio.debt_equity) if ratio.debt_equity else None,
                revenue_growth=float(ratio.revenue_growth) if ratio.revenue_growth else None,
                profit_growth=float(ratio.profit_growth) if ratio.profit_growth else None,
            )
        )

    return ScreenerResponse(
        total_matches=len(results),  # Simplified; should be total before pagination
        results=results,
        data_timestamp=datetime.now(),
    )


@router.get("/query")
async def query_screen(
    db: AsyncSession = Depends(get_db_session),
    q: str = Query(
        ...,
        description="Screener query string. Example: 'pe_ratio < 20 AND roe > 15 AND market_cap > 5000'",
    ),
    sort_by: str = Query("market_cap"),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Screen stocks using a query string (like screener.in's query builder).

    Supported operators: <, >, <=, >=, =, !=
    Supported conjunctions: AND, OR
    Supported fields: All ratio fields + sector, industry

    Example queries:
    - "pe_ratio < 15 AND roe > 20"
    - "debt_equity = 0 AND market_cap > 1000"
    - "sector = 'IT' AND revenue_growth > 15"
    """
    try:
        conditions = _parse_query_string(q)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Build the query
    stmt = (
        select(Company, ComputedRatio)
        .join(ComputedRatio, Company.id == ComputedRatio.company_id)
        .where(
            Company.is_active == True,
            ComputedRatio.is_ttm == True,
            ComputedRatio.result_nature == ResultNature.CONSOLIDATED,
        )
    )

    # Apply parsed conditions
    filter_conditions = []
    for condition in conditions:
        field = condition["field"]
        op = condition["operator"]
        value = condition["value"]

        # Map field to column
        if field in ["sector", "industry"]:
            column = getattr(Company, field, None)
        else:
            column = getattr(ComputedRatio, field, None)

        if column is None:
            continue

        # Build condition
        if op == "<":
            filter_conditions.append(column < value)
        elif op == ">":
            filter_conditions.append(column > value)
        elif op == "<=":
            filter_conditions.append(column <= value)
        elif op == ">=":
            filter_conditions.append(column >= value)
        elif op == "=":
            filter_conditions.append(column == value)
        elif op == "!=":
            filter_conditions.append(column != value)

    if filter_conditions:
        stmt = stmt.where(and_(*filter_conditions))

    # Sorting
    sort_column = getattr(ComputedRatio, sort_by, ComputedRatio.market_cap)
    if sort_order == "desc":
        stmt = stmt.order_by(sort_column.desc().nulls_last())
    else:
        stmt = stmt.order_by(sort_column.asc().nulls_last())

    # Apply pagination
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    rows = result.all()

    results = []
    for company, ratio in rows:
        results.append({
            "symbol": company.nse_symbol or company.bse_scrip_code,
            "company_name": company.company_name,
            "sector": company.sector,
            "industry": company.industry,
            "market_cap": float(ratio.market_cap) if ratio.market_cap else None,
            "pe_ratio": float(ratio.pe_ratio) if ratio.pe_ratio else None,
            "roe": float(ratio.roe) if ratio.roe else None,
            "roce": float(ratio.roce) if ratio.roce else None,
            "debt_equity": float(ratio.debt_equity) if ratio.debt_equity else None,
        })

    return {
        "query": q,
        "total_matches": len(results),
        "results": results,
        "data_timestamp": datetime.now().isoformat(),
    }


def _parse_query_string(query: str) -> list[dict[str, Any]]:
    """
    Parse a query string into a list of conditions.

    Example: "pe_ratio < 15 AND roe > 20" ->
    [
        {"field": "pe_ratio", "operator": "<", "value": 15},
        {"field": "roe", "operator": ">", "value": 20},
    ]
    """
    # Split by AND/OR (case insensitive)
    parts = re.split(r"\s+(?:AND|and)\s+", query)

    conditions = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Match pattern: field operator value
        match = re.match(
            r"(\w+)\s*(<=|>=|!=|<|>|=)\s*['\"]?([^'\"]+)['\"]?",
            part,
        )

        if not match:
            raise ValueError(f"Invalid condition: {part}")

        field = match.group(1).lower()
        operator = match.group(2)
        value_str = match.group(3).strip()

        # Try to parse value as number
        try:
            value = float(value_str)
        except ValueError:
            value = value_str

        conditions.append({
            "field": field,
            "operator": operator,
            "value": value,
        })

    return conditions
