"""
Company API endpoints.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, get_company_by_symbol, pagination_params
from api.schemas import CompanyBase, CompanyResponse, CompanyListResponse
from db.models import Company

router = APIRouter()


@router.get("/", response_model=CompanyListResponse)
async def list_companies(
    db: AsyncSession = Depends(get_db_session),
    pagination: dict = Depends(pagination_params),
    search: Optional[str] = Query(None, description="Search by name or symbol"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    active_only: bool = Query(True, description="Only active companies"),
):
    """
    List all companies with optional filtering.

    Supports searching by name/symbol and filtering by sector/industry.
    """
    stmt = select(Company)

    # Apply filters
    if active_only:
        stmt = stmt.where(Company.is_active == True)

    if search:
        search_term = f"%{search}%"
        stmt = stmt.where(
            (Company.company_name.ilike(search_term))
            | (Company.nse_symbol.ilike(search_term))
            | (Company.bse_scrip_code.ilike(search_term))
        )

    if sector:
        stmt = stmt.where(Company.sector == sector)

    if industry:
        stmt = stmt.where(Company.industry == industry)

    # Get total count
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await db.execute(count_stmt)
    total = total_result.scalar()

    # Apply pagination
    stmt = stmt.offset(pagination["offset"]).limit(pagination["limit"])
    stmt = stmt.order_by(Company.company_name)

    result = await db.execute(stmt)
    companies = result.scalars().all()

    return CompanyListResponse(
        total=total,
        companies=[CompanyBase.model_validate(c) for c in companies],
        data_timestamp=datetime.now(),
    )


@router.get("/{symbol}", response_model=CompanyResponse)
async def get_company(
    company: Company = Depends(get_company_by_symbol),
):
    """
    Get company details by symbol.

    Accepts NSE symbol, BSE scrip code, or ISIN.
    """
    return CompanyResponse(
        company=CompanyBase.model_validate(company),
        data_timestamp=datetime.now(),
    )


@router.get("/sectors/list")
async def list_sectors(
    db: AsyncSession = Depends(get_db_session),
):
    """Get list of all unique sectors."""
    stmt = select(Company.sector).where(Company.sector.isnot(None)).distinct()
    result = await db.execute(stmt)
    sectors = [row[0] for row in result.all() if row[0]]
    return {"sectors": sorted(sectors)}


@router.get("/industries/list")
async def list_industries(
    db: AsyncSession = Depends(get_db_session),
    sector: Optional[str] = Query(None, description="Filter by sector"),
):
    """Get list of all unique industries."""
    stmt = select(Company.industry).where(Company.industry.isnot(None))
    if sector:
        stmt = stmt.where(Company.sector == sector)
    stmt = stmt.distinct()

    result = await db.execute(stmt)
    industries = [row[0] for row in result.all() if row[0]]
    return {"industries": sorted(industries)}
