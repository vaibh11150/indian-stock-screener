"""
SQLAlchemy ORM models for the Indian Stock Screener database.
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class StatementType(str, PyEnum):
    """Types of financial statements."""
    PROFIT_LOSS = "profit_loss"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"


class ResultNature(str, PyEnum):
    """Nature of financial results."""
    STANDALONE = "standalone"
    CONSOLIDATED = "consolidated"


class PeriodType(str, PyEnum):
    """Type of reporting period."""
    QUARTERLY = "quarterly"
    HALF_YEARLY = "half_yearly"
    NINE_MONTHS = "nine_months"
    ANNUAL = "annual"


class Company(Base):
    """Company master data."""
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nse_symbol: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True)
    bse_scrip_code: Mapped[Optional[str]] = mapped_column(String(10), unique=True, nullable=True)
    isin: Mapped[str] = mapped_column(String(12), unique=True, nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    industry: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    bse_group: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    listing_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    face_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    financial_statements: Mapped[list["FinancialStatement"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )
    daily_prices: Mapped[list["DailyPrice"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )
    corporate_actions: Mapped[list["CorporateAction"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )
    shareholding_patterns: Mapped[list["ShareholdingPattern"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )
    computed_ratios: Mapped[list["ComputedRatio"]] = relationship(
        back_populates="company", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_companies_nse", "nse_symbol"),
        Index("idx_companies_bse", "bse_scrip_code"),
        Index("idx_companies_isin", "isin"),
    )


class FinancialStatement(Base):
    """Financial statement header (one row per company per period per type)."""
    __tablename__ = "financial_statements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    statement_type: Mapped[StatementType] = mapped_column(
        Enum(StatementType, name="statement_type", create_constraint=True), nullable=False
    )
    result_nature: Mapped[ResultNature] = mapped_column(
        Enum(ResultNature, name="result_nature", create_constraint=True), nullable=False
    )
    period_type: Mapped[PeriodType] = mapped_column(
        Enum(PeriodType, name="period_type", create_constraint=True), nullable=False
    )
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    fiscal_year: Mapped[str] = mapped_column(String(7), nullable=False)
    fiscal_quarter: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    is_audited: Mapped[bool] = mapped_column(Boolean, default=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    company: Mapped["Company"] = relationship(back_populates="financial_statements")
    line_items: Mapped[list["FinancialLineItem"]] = relationship(
        back_populates="statement", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint(
            "company_id", "statement_type", "result_nature", "period_type", "period_end",
            name="uq_financial_statement"
        ),
        Index("idx_fs_company_period", "company_id", "period_end"),
        Index("idx_fs_fiscal", "company_id", "fiscal_year", "fiscal_quarter"),
    )


class FinancialLineItem(Base):
    """Individual line items within a financial statement."""
    __tablename__ = "financial_line_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    statement_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("financial_statements.id", ondelete="CASCADE"), nullable=False
    )
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    field_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    display_order: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)

    # Relationships
    statement: Mapped["FinancialStatement"] = relationship(back_populates="line_items")

    __table_args__ = (
        UniqueConstraint("statement_id", "field_name", name="uq_line_item"),
        Index("idx_fli_statement", "statement_id"),
        Index("idx_fli_field", "field_name"),
        Index("idx_fli_lookup", "statement_id", "field_name"),
    )


class RawFiling(Base):
    """Raw filing data for audit trail."""
    __tablename__ = "raw_filings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    filing_type: Mapped[str] = mapped_column(String(20), nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    raw_content: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    parsed_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    checksum: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("company_id", "source", "filing_type", "period_end", name="uq_raw_filing"),
    )


class DailyPrice(Base):
    """Daily OHLCV price data."""
    __tablename__ = "daily_prices"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    high_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    low_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    close_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    adj_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    delivery_qty: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    source: Mapped[str] = mapped_column(String(10), default="nse")

    # Relationships
    company: Mapped["Company"] = relationship(back_populates="daily_prices")

    __table_args__ = (
        UniqueConstraint("company_id", "trade_date", name="uq_daily_price"),
        Index("idx_prices_company_date", "company_id", "trade_date"),
    )


class CorporateAction(Base):
    """Corporate actions (dividends, splits, bonuses, rights)."""
    __tablename__ = "corporate_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    action_type: Mapped[str] = mapped_column(String(20), nullable=False)
    ex_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    record_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ratio_from: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    ratio_to: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    company: Mapped["Company"] = relationship(back_populates="corporate_actions")


class ShareholdingPattern(Base):
    """Quarterly shareholding patterns."""
    __tablename__ = "shareholding_patterns"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    quarter_end: Mapped[date] = mapped_column(Date, nullable=False)
    promoter_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    fii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    dii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    public_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    govt_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    pledged_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    total_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # Relationships
    company: Mapped["Company"] = relationship(back_populates="shareholding_patterns")

    __table_args__ = (
        UniqueConstraint("company_id", "quarter_end", name="uq_shareholding"),
    )


class ComputedRatio(Base):
    """Materialized computed ratios for fast screening."""
    __tablename__ = "computed_ratios"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    period_type: Mapped[PeriodType] = mapped_column(
        Enum(PeriodType, name="period_type", create_constraint=False), nullable=False
    )
    is_ttm: Mapped[bool] = mapped_column(Boolean, default=False)
    result_nature: Mapped[ResultNature] = mapped_column(
        Enum(ResultNature, name="result_nature", create_constraint=False), nullable=False
    )

    # Valuation metrics
    market_cap: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    pe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    pb_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    ev: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    ev_ebitda: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    dividend_yield: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)

    # Profitability metrics
    roe: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    roce: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    roa: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    operating_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    net_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Efficiency metrics
    asset_turnover: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    inventory_days: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    receivable_days: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    payable_days: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    cash_conversion_cycle: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Leverage metrics
    debt_equity: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    current_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    interest_coverage: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)

    # Growth metrics
    revenue_growth: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    profit_growth: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Per share metrics
    eps: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    book_value_per_share: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)

    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    company: Mapped["Company"] = relationship(back_populates="computed_ratios")

    __table_args__ = (
        UniqueConstraint(
            "company_id", "period_end", "period_type", "is_ttm", "result_nature",
            name="uq_computed_ratio"
        ),
        Index("idx_ratios_screen", "period_type", "is_ttm"),
    )


class QualityCheck(Base):
    """Data quality check results."""
    __tablename__ = "quality_checks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(Integer, ForeignKey("companies.id"), nullable=False)
    check_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    field_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    our_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    reference_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    reference_source: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    pct_deviation: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    is_acceptable: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    period_end: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_qc_company", "company_id", "check_date"),
        Index("idx_qc_unacceptable", "is_acceptable"),
    )


class ScrapeLog(Base):
    """Audit trail for scrape operations."""
    __tablename__ = "scrape_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    scraper_name: Mapped[str] = mapped_column(String(50), nullable=False)
    company_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("companies.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    records_scraped: Mapped[int] = mapped_column(Integer, default=0)
    records_inserted: Mapped[int] = mapped_column(Integer, default=0)
    records_updated: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
