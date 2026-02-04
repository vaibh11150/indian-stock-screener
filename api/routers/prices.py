"""
Price data API endpoints.
"""

from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, get_company_by_symbol
from api.schemas import PriceResponse, PricePoint
from db.models import Company, DailyPrice

router = APIRouter()


@router.get("/{symbol}", response_model=PriceResponse)
async def get_prices(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    interval: str = Query("daily", enum=["daily", "weekly", "monthly"]),
    limit: int = Query(365, ge=1, le=5000),
):
    """
    Get historical price data.

    Returns OHLCV data for the specified period.
    """
    company = await get_company_by_symbol(symbol, db)

    # Default date range: last year
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = to_date - timedelta(days=365)

    # Build query
    stmt = (
        select(DailyPrice)
        .where(
            DailyPrice.company_id == company.id,
            DailyPrice.trade_date >= from_date,
            DailyPrice.trade_date <= to_date,
        )
        .order_by(DailyPrice.trade_date.desc())
        .limit(limit)
    )

    result = await db.execute(stmt)
    prices = result.scalars().all()

    # Convert to response format
    price_points = []
    for price in prices:
        price_points.append(
            PricePoint(
                date=price.trade_date,
                open=float(price.open_price) if price.open_price else 0,
                high=float(price.high_price) if price.high_price else 0,
                low=float(price.low_price) if price.low_price else 0,
                close=float(price.close_price) if price.close_price else 0,
                adj_close=float(price.adj_close) if price.adj_close else None,
                volume=price.volume or 0,
            )
        )

    # If interval is not daily, aggregate
    if interval == "weekly":
        price_points = _aggregate_to_weekly(price_points)
    elif interval == "monthly":
        price_points = _aggregate_to_monthly(price_points)

    return PriceResponse(
        symbol=company.nse_symbol or company.bse_scrip_code,
        prices=price_points,
        data_timestamp=datetime.now(),
    )


@router.get("/{symbol}/latest")
async def get_latest_price(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
):
    """
    Get the latest available price for a company.
    """
    company = await get_company_by_symbol(symbol, db)

    stmt = (
        select(DailyPrice)
        .where(DailyPrice.company_id == company.id)
        .order_by(DailyPrice.trade_date.desc())
        .limit(1)
    )

    result = await db.execute(stmt)
    price = result.scalar_one_or_none()

    if price:
        return {
            "symbol": company.nse_symbol or company.bse_scrip_code,
            "company_name": company.company_name,
            "trade_date": price.trade_date.isoformat(),
            "open": float(price.open_price) if price.open_price else None,
            "high": float(price.high_price) if price.high_price else None,
            "low": float(price.low_price) if price.low_price else None,
            "close": float(price.close_price) if price.close_price else None,
            "volume": price.volume,
            "data_timestamp": datetime.now().isoformat(),
        }
    else:
        return {
            "symbol": company.nse_symbol or company.bse_scrip_code,
            "company_name": company.company_name,
            "message": "No price data available",
            "data_timestamp": datetime.now().isoformat(),
        }


def _aggregate_to_weekly(prices: list[PricePoint]) -> list[PricePoint]:
    """Aggregate daily prices to weekly OHLCV."""
    if not prices:
        return []

    # Sort by date ascending
    prices = sorted(prices, key=lambda x: x.date)

    weekly = []
    week_prices = []
    current_week = None

    for price in prices:
        week_num = price.date.isocalendar()[1]
        year = price.date.year

        if current_week != (year, week_num):
            if week_prices:
                weekly.append(_aggregate_prices(week_prices))
            week_prices = []
            current_week = (year, week_num)

        week_prices.append(price)

    if week_prices:
        weekly.append(_aggregate_prices(week_prices))

    return sorted(weekly, key=lambda x: x.date, reverse=True)


def _aggregate_to_monthly(prices: list[PricePoint]) -> list[PricePoint]:
    """Aggregate daily prices to monthly OHLCV."""
    if not prices:
        return []

    # Sort by date ascending
    prices = sorted(prices, key=lambda x: x.date)

    monthly = []
    month_prices = []
    current_month = None

    for price in prices:
        month_key = (price.date.year, price.date.month)

        if current_month != month_key:
            if month_prices:
                monthly.append(_aggregate_prices(month_prices))
            month_prices = []
            current_month = month_key

        month_prices.append(price)

    if month_prices:
        monthly.append(_aggregate_prices(month_prices))

    return sorted(monthly, key=lambda x: x.date, reverse=True)


def _aggregate_prices(prices: list[PricePoint]) -> PricePoint:
    """Aggregate a list of prices into a single OHLCV point."""
    if not prices:
        raise ValueError("Cannot aggregate empty price list")

    # Sort by date
    prices = sorted(prices, key=lambda x: x.date)

    return PricePoint(
        date=prices[-1].date,  # Use last date in period
        open=prices[0].open,   # First open
        high=max(p.high for p in prices),
        low=min(p.low for p in prices),
        close=prices[-1].close,  # Last close
        adj_close=prices[-1].adj_close,
        volume=sum(p.volume for p in prices),
    )
