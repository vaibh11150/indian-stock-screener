"""
Anomaly detector for financial data.

Detects statistical outliers and data quality issues in financial data.
"""

from datetime import date
from typing import Any, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import (
    Company,
    FinancialLineItem,
    FinancialStatement,
    ComputedRatio,
    PeriodType,
    ResultNature,
)

logger = get_logger(__name__)


class AnomalyDetector:
    """Detector for data quality anomalies."""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def detect_anomalies(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Detect anomalies in financial data.

        Args:
            company_id: Optional specific company to check

        Returns:
            List of detected anomalies
        """
        anomalies = []

        # Run various anomaly detection checks
        anomalies.extend(await self._check_balance_sheet_equation(company_id))
        anomalies.extend(await self._check_negative_values(company_id))
        anomalies.extend(await self._check_extreme_ratios(company_id))
        anomalies.extend(await self._check_missing_data(company_id))
        anomalies.extend(await self._check_sudden_changes(company_id))

        return anomalies

    async def _check_balance_sheet_equation(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Check that Assets = Liabilities + Equity.

        This is a fundamental accounting equation that must hold.
        """
        anomalies = []

        # Get balance sheet statements
        stmt = (
            select(FinancialStatement)
            .where(
                FinancialStatement.statement_type == "balance_sheet",
            )
            .order_by(FinancialStatement.period_end.desc())
            .limit(1000)
        )

        if company_id:
            stmt = stmt.where(FinancialStatement.company_id == company_id)

        result = await self.db_session.execute(stmt)
        statements = result.scalars().all()

        for statement in statements:
            items = await self._get_statement_items(statement.id)

            total_assets = float(items.get("total_assets", 0) or 0)
            total_equity = float(items.get("total_equity", 0) or 0)
            total_liabilities = float(items.get("total_liabilities", 0) or 0)

            if total_assets == 0:
                continue

            # Check equation: Assets = Equity + Liabilities
            expected = total_equity + total_liabilities
            if expected > 0:
                diff_pct = abs(total_assets - expected) / expected * 100

                if diff_pct > 5:  # More than 5% difference
                    anomalies.append({
                        "type": "balance_sheet_mismatch",
                        "company_id": statement.company_id,
                        "period_end": statement.period_end,
                        "message": f"Assets ({total_assets:.0f}) != Equity ({total_equity:.0f}) + Liabilities ({total_liabilities:.0f})",
                        "severity": "high" if diff_pct > 10 else "medium",
                        "deviation_pct": round(diff_pct, 2),
                    })

        return anomalies

    async def _check_negative_values(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Check for unexpected negative values.

        Some fields should never be negative (e.g., revenue, assets).
        """
        anomalies = []

        # Fields that should typically be positive
        positive_fields = [
            "revenue",
            "total_assets",
            "total_equity",
            "share_capital",
            "inventory",
            "trade_receivables",
        ]

        for field in positive_fields:
            stmt = (
                select(FinancialLineItem, FinancialStatement)
                .join(FinancialStatement)
                .where(
                    FinancialLineItem.field_name == field,
                    FinancialLineItem.field_value < 0,
                )
                .limit(100)
            )

            if company_id:
                stmt = stmt.where(FinancialStatement.company_id == company_id)

            result = await self.db_session.execute(stmt)
            rows = result.all()

            for line_item, statement in rows:
                anomalies.append({
                    "type": "negative_value",
                    "company_id": statement.company_id,
                    "period_end": statement.period_end,
                    "field_name": field,
                    "value": float(line_item.field_value),
                    "message": f"Unexpected negative {field}: {line_item.field_value}",
                    "severity": "medium",
                })

        return anomalies

    async def _check_extreme_ratios(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Check for extreme ratio values that may indicate data issues.
        """
        anomalies = []

        # Define reasonable bounds for ratios
        ratio_bounds = {
            "pe_ratio": (0, 500),
            "pb_ratio": (0, 100),
            "roe": (-100, 200),
            "roce": (-100, 200),
            "debt_equity": (0, 20),
            "current_ratio": (0, 50),
        }

        stmt = (
            select(ComputedRatio, Company.nse_symbol)
            .join(Company)
            .order_by(ComputedRatio.computed_at.desc())
            .limit(5000)
        )

        if company_id:
            stmt = stmt.where(ComputedRatio.company_id == company_id)

        result = await self.db_session.execute(stmt)
        rows = result.all()

        for ratio, symbol in rows:
            for field, (min_val, max_val) in ratio_bounds.items():
                value = getattr(ratio, field, None)
                if value is not None:
                    value = float(value)
                    if value < min_val or value > max_val:
                        anomalies.append({
                            "type": "extreme_ratio",
                            "company_id": ratio.company_id,
                            "symbol": symbol,
                            "period_end": ratio.period_end,
                            "field_name": field,
                            "value": value,
                            "bounds": (min_val, max_val),
                            "message": f"Extreme {field}: {value} (expected {min_val}-{max_val})",
                            "severity": "low",
                        })

        return anomalies

    async def _check_missing_data(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Check for missing critical data fields.
        """
        anomalies = []

        # Required fields for P&L
        required_pl_fields = ["revenue", "net_profit"]

        # Get recent P&L statements
        stmt = (
            select(FinancialStatement)
            .where(
                FinancialStatement.statement_type == "profit_loss",
                FinancialStatement.period_type == PeriodType.ANNUAL,
            )
            .order_by(FinancialStatement.period_end.desc())
            .limit(500)
        )

        if company_id:
            stmt = stmt.where(FinancialStatement.company_id == company_id)

        result = await self.db_session.execute(stmt)
        statements = result.scalars().all()

        for statement in statements:
            items = await self._get_statement_items(statement.id)

            for field in required_pl_fields:
                if field not in items or items[field] is None:
                    anomalies.append({
                        "type": "missing_data",
                        "company_id": statement.company_id,
                        "period_end": statement.period_end,
                        "field_name": field,
                        "message": f"Missing required field: {field}",
                        "severity": "high",
                    })

        return anomalies

    async def _check_sudden_changes(
        self,
        company_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Check for sudden large changes in key metrics.

        A sudden >100% change in revenue or profit may indicate data issues.
        """
        anomalies = []

        # This would require comparing consecutive periods
        # For now, just return empty - full implementation would need
        # more complex queries

        return anomalies

    async def _get_statement_items(self, statement_id: int) -> dict:
        """Get all line items for a statement."""
        stmt = select(FinancialLineItem).where(
            FinancialLineItem.statement_id == statement_id
        )
        result = await self.db_session.execute(stmt)
        items = result.scalars().all()

        return {item.field_name: item.field_value for item in items}


async def detect_anomalies(
    db_session: AsyncSession,
    company_id: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Detect anomalies in financial data.

    Convenience function.
    """
    detector = AnomalyDetector(db_session)
    return await detector.detect_anomalies(company_id)
