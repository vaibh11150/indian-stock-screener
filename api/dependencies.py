"""
FastAPI dependencies for the API.
"""

from typing import AsyncGenerator

from fastapi import Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db, Company


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session."""
    async for session in get_db():
        yield session


async def get_company_by_symbol(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
) -> Company:
    """
    Get company by NSE symbol or BSE scrip code.

    Raises 404 if not found.
    """
    # Try NSE symbol first
    stmt = select(Company).where(Company.nse_symbol == symbol.upper())
    result = await db.execute(stmt)
    company = result.scalar_one_or_none()

    if not company:
        # Try BSE scrip code
        stmt = select(Company).where(Company.bse_scrip_code == symbol)
        result = await db.execute(stmt)
        company = result.scalar_one_or_none()

    if not company:
        # Try ISIN
        stmt = select(Company).where(Company.isin == symbol.upper())
        result = await db.execute(stmt)
        company = result.scalar_one_or_none()

    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {symbol}")

    return company


def pagination_params(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Common pagination parameters."""
    return {"limit": limit, "offset": offset}
