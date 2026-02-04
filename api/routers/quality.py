"""
Data quality API endpoints.
"""

from datetime import datetime, timedelta, date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db_session, get_company_by_symbol
from api.schemas import (
    QualityReportResponse,
    CompanyQualityResponse,
    FieldAccuracy,
)
from db.models import Company, QualityCheck
from quality.checker import QualityChecker

router = APIRouter()


@router.get("/report", response_model=QualityReportResponse)
async def get_quality_report(
    db: AsyncSession = Depends(get_db_session),
    days: int = Query(7, description="Report for last N days"),
):
    """
    Get the latest data quality report.

    Shows accuracy metrics comparing our data against screener.in reference.
    """
    cutoff_date = datetime.now() - timedelta(days=days)

    # Get aggregate stats
    stmt = (
        select(
            QualityCheck.field_name,
            func.count(QualityCheck.id).label("total"),
            func.sum(
                func.cast(QualityCheck.is_acceptable, Integer)
            ).label("acceptable"),
        )
        .where(QualityCheck.check_date >= cutoff_date)
        .group_by(QualityCheck.field_name)
    )

    # Use raw SQL for the cast since SQLAlchemy 2.0 syntax differs
    from sqlalchemy import Integer, case

    stmt = (
        select(
            QualityCheck.field_name,
            func.count(QualityCheck.id).label("total"),
            func.sum(
                case((QualityCheck.is_acceptable == True, 1), else_=0)
            ).label("acceptable"),
        )
        .where(QualityCheck.check_date >= cutoff_date)
        .group_by(QualityCheck.field_name)
    )

    result = await db.execute(stmt)
    field_stats = result.all()

    # Build by_field dict
    by_field = {}
    total_checks = 0
    total_acceptable = 0

    for field_name, total, acceptable in field_stats:
        if field_name:
            acceptable = acceptable or 0
            total_checks += total
            total_acceptable += acceptable
            by_field[field_name] = FieldAccuracy(
                accuracy=round((acceptable / total) * 100, 1) if total > 0 else 0,
                total=total,
                within_threshold=acceptable,
                outside_threshold=total - acceptable,
            )

    # Get worst deviations
    worst_stmt = (
        select(QualityCheck, Company.nse_symbol)
        .join(Company)
        .where(
            QualityCheck.check_date >= cutoff_date,
            QualityCheck.is_acceptable == False,
        )
        .order_by(func.abs(QualityCheck.pct_deviation).desc())
        .limit(20)
    )

    worst_result = await db.execute(worst_stmt)
    worst_rows = worst_result.all()

    worst_deviations = []
    for check, symbol in worst_rows:
        worst_deviations.append({
            "symbol": symbol,
            "field": check.field_name,
            "our_value": float(check.our_value) if check.our_value else None,
            "reference_value": float(check.reference_value) if check.reference_value else None,
            "pct_deviation": float(check.pct_deviation) if check.pct_deviation else None,
            "period_end": check.period_end.isoformat() if check.period_end else None,
        })

    overall_accuracy = (
        round((total_acceptable / total_checks) * 100, 1)
        if total_checks > 0
        else 0
    )

    return QualityReportResponse(
        report_date=date.today(),
        overall_accuracy=overall_accuracy,
        total_checks=total_checks,
        within_threshold=total_acceptable,
        outside_threshold=total_checks - total_acceptable,
        by_field=by_field,
        worst_deviations=worst_deviations,
        data_timestamp=datetime.now(),
    )


@router.get("/{symbol}", response_model=CompanyQualityResponse)
async def get_company_quality(
    symbol: str,
    db: AsyncSession = Depends(get_db_session),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Get quality check results for a specific company.
    """
    company = await get_company_by_symbol(symbol, db)

    stmt = (
        select(QualityCheck)
        .where(QualityCheck.company_id == company.id)
        .order_by(QualityCheck.check_date.desc())
        .limit(limit)
    )

    result = await db.execute(stmt)
    checks = result.scalars().all()

    check_list = []
    for check in checks:
        check_list.append({
            "check_date": check.check_date.isoformat() if check.check_date else None,
            "field_name": check.field_name,
            "our_value": float(check.our_value) if check.our_value else None,
            "reference_value": float(check.reference_value) if check.reference_value else None,
            "pct_deviation": float(check.pct_deviation) if check.pct_deviation else None,
            "is_acceptable": check.is_acceptable,
            "period_end": check.period_end.isoformat() if check.period_end else None,
            "reference_source": check.reference_source,
        })

    return CompanyQualityResponse(
        symbol=company.nse_symbol or company.bse_scrip_code,
        checks=check_list,
        data_timestamp=datetime.now(),
    )


@router.post("/run")
async def run_quality_check_endpoint(
    db: AsyncSession = Depends(get_db_session),
    sample_size: int = Query(50, ge=1, le=200),
):
    """
    Trigger a quality check run.

    This compares our data against screener.in for a sample of companies.
    """
    checker = QualityChecker(db)
    results = await checker.run_quality_check(sample_size=sample_size)

    return {
        "status": "completed",
        "accuracy": results.get("accuracy"),
        "total_checks": results.get("total_checks"),
        "within_threshold": results.get("within_threshold"),
        "outside_threshold": results.get("outside_threshold"),
        "errors": results.get("errors"),
        "data_timestamp": datetime.now().isoformat(),
    }
