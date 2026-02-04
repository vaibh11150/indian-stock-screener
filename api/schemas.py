"""
Pydantic response models for the API.

All responses include a data_timestamp for freshness checking.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field


class TimestampedResponse(BaseModel):
    """Base response with timestamp."""
    data_timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When this data was last updated",
    )


# ============ Company Models ============

class CompanyBase(BaseModel):
    """Company basic information."""
    id: int
    nse_symbol: Optional[str] = None
    bse_scrip_code: Optional[str] = None
    isin: str
    company_name: str
    industry: Optional[str] = None
    sector: Optional[str] = None
    listing_date: Optional[date] = None
    face_value: Optional[float] = None

    class Config:
        from_attributes = True


class CompanyResponse(TimestampedResponse):
    """Single company response."""
    company: CompanyBase


class CompanyListResponse(TimestampedResponse):
    """List of companies response."""
    total: int
    companies: list[CompanyBase]


# ============ Financial Models ============

class FinancialPeriod(BaseModel):
    """Financial data for a single period."""
    period_end: date
    period_start: Optional[date] = None
    fiscal_year: str
    fiscal_quarter: Optional[int] = None
    is_audited: bool = False
    source: str
    filing_date: Optional[date] = None
    items: dict[str, Optional[float]] = Field(default_factory=dict)


class FinancialResponse(TimestampedResponse):
    """Financial statements response."""
    symbol: str
    company_name: str
    statement_type: str
    nature: str
    periods: list[FinancialPeriod]


class TTMResponse(TimestampedResponse):
    """TTM financial data response."""
    symbol: str
    company_name: str
    nature: str
    ttm_data: dict[str, Optional[float]]


# ============ Ratio Models ============

class RatioPeriod(BaseModel):
    """Ratios for a single period."""
    period_end: date
    period_type: str
    is_ttm: bool = False

    # Valuation
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ev: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None

    # Profitability
    roe: Optional[float] = None
    roce: Optional[float] = None
    roa: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None

    # Efficiency
    asset_turnover: Optional[float] = None
    inventory_days: Optional[float] = None
    receivable_days: Optional[float] = None
    payable_days: Optional[float] = None
    cash_conversion_cycle: Optional[float] = None

    # Leverage
    debt_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    interest_coverage: Optional[float] = None

    # Growth
    revenue_growth: Optional[float] = None
    profit_growth: Optional[float] = None

    # Per share
    eps: Optional[float] = None
    book_value_per_share: Optional[float] = None


class RatiosResponse(TimestampedResponse):
    """Ratios response."""
    symbol: str
    ratios: list[RatioPeriod]


# ============ Price Models ============

class PricePoint(BaseModel):
    """Single price point."""
    date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: Optional[float] = None
    volume: int


class PriceResponse(TimestampedResponse):
    """Price data response."""
    symbol: str
    prices: list[PricePoint]


# ============ Screener Models ============

class ScreenerFilter(BaseModel):
    """Single filter criterion."""
    min: Optional[float] = None
    max: Optional[float] = None


class ScreenerRequest(BaseModel):
    """Screening request body."""
    pe_ratio: Optional[ScreenerFilter] = None
    pb_ratio: Optional[ScreenerFilter] = None
    roe: Optional[ScreenerFilter] = None
    roce: Optional[ScreenerFilter] = None
    debt_equity: Optional[ScreenerFilter] = None
    current_ratio: Optional[ScreenerFilter] = None
    market_cap: Optional[ScreenerFilter] = None
    revenue_growth: Optional[ScreenerFilter] = None
    profit_growth: Optional[ScreenerFilter] = None
    dividend_yield: Optional[ScreenerFilter] = None
    operating_margin: Optional[ScreenerFilter] = None
    net_margin: Optional[ScreenerFilter] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class ScreenerResultItem(BaseModel):
    """Single result in screener response."""
    symbol: str
    company_name: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    current_price: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    roe: Optional[float] = None
    roce: Optional[float] = None
    debt_equity: Optional[float] = None
    revenue_growth: Optional[float] = None
    profit_growth: Optional[float] = None


class ScreenerResponse(TimestampedResponse):
    """Screener results response."""
    total_matches: int
    results: list[ScreenerResultItem]


# ============ Quality Models ============

class FieldAccuracy(BaseModel):
    """Accuracy stats for a single field."""
    accuracy: float
    total: int
    within_threshold: int
    outside_threshold: int


class QualityReportResponse(TimestampedResponse):
    """Quality report response."""
    report_date: date
    overall_accuracy: float
    total_checks: int
    within_threshold: int
    outside_threshold: int
    by_field: dict[str, FieldAccuracy]
    worst_deviations: list[dict[str, Any]]


class CompanyQualityResponse(TimestampedResponse):
    """Company-specific quality check results."""
    symbol: str
    checks: list[dict[str, Any]]


# ============ Generic Models ============

class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str
    version: str
