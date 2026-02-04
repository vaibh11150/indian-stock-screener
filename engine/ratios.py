"""
Financial ratio computation engine.

Computes all financial ratios to match screener.in's methodology.

IMPORTANT CONVENTIONS (matching screener.in):
1. Use consolidated numbers where available, else standalone
2. For ratios like ROE/ROCE, use AVERAGE of opening and closing capital
3. Use TTM (trailing twelve months) for PE, EV/EBITDA when possible
4. Market cap = current price × total shares outstanding
5. Screener excludes CWIP, investments from capital employed for ROCE
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from config.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FinancialData:
    """All data needed to compute ratios for one period."""

    # P&L Items
    revenue: float = 0.0
    other_income: float = 0.0
    total_income: float = 0.0
    raw_material_cost: float = 0.0
    employee_cost: float = 0.0
    total_expenses: float = 0.0
    operating_profit: float = 0.0  # EBITDA
    depreciation: float = 0.0
    interest_expense: float = 0.0
    profit_before_exceptional: float = 0.0
    exceptional_items: float = 0.0
    profit_before_tax: float = 0.0
    tax_expense: float = 0.0
    net_profit: float = 0.0
    eps_basic: float = 0.0
    eps_diluted: float = 0.0

    # Balance Sheet Items
    share_capital: float = 0.0
    reserves_surplus: float = 0.0
    total_equity: float = 0.0
    minority_interest: float = 0.0
    long_term_borrowings: float = 0.0
    short_term_borrowings: float = 0.0
    total_borrowings: float = 0.0
    total_current_liabilities: float = 0.0
    total_non_current_liabilities: float = 0.0
    total_liabilities: float = 0.0
    total_current_assets: float = 0.0
    total_non_current_assets: float = 0.0
    total_assets: float = 0.0
    fixed_assets: float = 0.0
    cwip: float = 0.0
    intangible_assets: float = 0.0
    investments: float = 0.0
    non_current_investments: float = 0.0
    current_investments: float = 0.0
    inventory: float = 0.0
    trade_receivables: float = 0.0
    trade_payables: float = 0.0
    cash_and_equivalents: float = 0.0
    goodwill: float = 0.0

    # Cash Flow Items
    cfo: float = 0.0
    cfi: float = 0.0
    cff: float = 0.0
    net_cash_flow: float = 0.0
    capex: float = 0.0

    # Market Data
    current_price: float = 0.0
    shares_outstanding: float = 0.0
    face_value: float = 0.0

    # Computed fields that might be filled
    ebitda: float = field(default=0.0, init=False)
    ebit: float = field(default=0.0, init=False)

    def __post_init__(self):
        """Compute derived fields."""
        # EBITDA calculation
        if self.operating_profit > 0:
            self.ebitda = self.operating_profit
        else:
            # EBITDA = PBT + Interest + Depreciation
            self.ebitda = (
                self.profit_before_tax + self.interest_expense + self.depreciation
            )

        # EBIT calculation
        self.ebit = self.profit_before_tax + self.interest_expense

        # Calculate total borrowings if not provided
        if self.total_borrowings == 0:
            self.total_borrowings = self.long_term_borrowings + self.short_term_borrowings

        # Calculate total equity if not provided
        if self.total_equity == 0 and (self.share_capital > 0 or self.reserves_surplus > 0):
            self.total_equity = self.share_capital + self.reserves_surplus

    @classmethod
    def from_dict(cls, data: dict) -> "FinancialData":
        """Create FinancialData from a dictionary."""
        # Map common field name variations
        field_aliases = {
            "net_profit": ["net_profit", "profit_for_period", "pat"],
            "total_equity": ["total_equity", "shareholders_equity", "shareholder_funds"],
            "interest_expense": ["interest_expense", "finance_costs", "interest"],
        }

        processed = {}
        for field_name in cls.__dataclass_fields__:
            value = data.get(field_name)
            if value is None:
                # Try aliases
                aliases = field_aliases.get(field_name, [])
                for alias in aliases:
                    if alias in data:
                        value = data[alias]
                        break

            if value is not None:
                try:
                    processed[field_name] = float(value)
                except (ValueError, TypeError):
                    pass

        return cls(**processed)


def compute_ratios(
    current: FinancialData,
    previous: Optional[FinancialData] = None,
    is_annual: bool = True,
) -> dict[str, Optional[float]]:
    """
    Compute all financial ratios.

    Args:
        current: Current period financial data
        previous: Previous period data (for averaging balance sheet items)
        is_annual: Whether this is annual or quarterly data

    Returns:
        Dict of ratio names to values
    """
    ratios: dict[str, Optional[float]] = {}

    def safe_div(numerator: float, denominator: float, default: Optional[float] = None) -> Optional[float]:
        """Safe division with default for zero denominator."""
        if denominator and abs(denominator) > 0.001:
            return round(numerator / denominator, 2)
        return default

    def avg(current_val: float, prev_val: Optional[float]) -> float:
        """Calculate average of current and previous values."""
        if previous and prev_val and prev_val > 0:
            return (current_val + prev_val) / 2
        return current_val

    # ===== MARKET & VALUATION =====
    market_cap = current.current_price * current.shares_outstanding
    ratios["market_cap"] = round(market_cap, 2) if market_cap > 0 else None

    # PE Ratio = Market Price / EPS
    ratios["pe_ratio"] = safe_div(current.current_price, current.eps_basic)

    # PB Ratio = Market Price / Book Value per Share
    bvps = safe_div(current.total_equity, current.shares_outstanding)
    ratios["book_value_per_share"] = bvps
    ratios["pb_ratio"] = safe_div(current.current_price, bvps) if bvps and bvps > 0 else None

    # Enterprise Value = Market Cap + Total Debt - Cash
    if market_cap > 0:
        ev = market_cap + current.total_borrowings - current.cash_and_equivalents
        ratios["ev"] = round(ev, 2)

        # EV/EBITDA
        if current.ebitda > 0:
            ratios["ev_ebitda"] = safe_div(ev, current.ebitda)
        else:
            ratios["ev_ebitda"] = None
    else:
        ratios["ev"] = None
        ratios["ev_ebitda"] = None

    # Dividend Yield (requires dividend data)
    ratios["dividend_yield"] = None

    # ===== PROFITABILITY =====

    # Operating Profit Margin = Operating Profit / Revenue × 100
    ratios["operating_margin"] = safe_div(current.ebitda * 100, current.revenue)

    # Net Profit Margin = Net Profit / Revenue × 100
    ratios["net_margin"] = safe_div(current.net_profit * 100, current.revenue)

    # ROE = Net Profit / Average Equity × 100
    prev_equity = previous.total_equity if previous else 0
    avg_equity = avg(current.total_equity, prev_equity)
    ratios["roe"] = safe_div(current.net_profit * 100, avg_equity)

    # ROCE = EBIT / Capital Employed × 100
    # Capital Employed = Total Equity + Total Borrowings - Investments - CWIP
    # (Screener's method)
    capital_employed = (
        current.total_equity
        + current.total_borrowings
        - current.investments
        - current.non_current_investments
        - current.cwip
    )

    prev_ce = 0
    if previous:
        prev_ce = (
            previous.total_equity
            + previous.total_borrowings
            - previous.investments
            - previous.non_current_investments
            - previous.cwip
        )

    avg_ce = avg(capital_employed, prev_ce)
    ratios["roce"] = safe_div(current.ebit * 100, avg_ce)

    # ROA = Net Profit / Average Total Assets × 100
    prev_assets = previous.total_assets if previous else 0
    avg_assets = avg(current.total_assets, prev_assets)
    ratios["roa"] = safe_div(current.net_profit * 100, avg_assets)

    # ===== EFFICIENCY =====

    # Asset Turnover = Revenue / Average Total Assets
    ratios["asset_turnover"] = safe_div(current.revenue, avg_assets)

    # Cost of goods sold (approximate)
    cogs = (
        current.total_expenses
        - current.depreciation
        - current.interest_expense
        - current.employee_cost
    )
    if cogs <= 0:
        cogs = current.total_expenses - current.depreciation - current.interest_expense

    # Inventory Days = (Inventory / COGS) × 365
    if cogs > 0 and current.inventory > 0:
        ratios["inventory_days"] = round((current.inventory * 365) / cogs, 2)
    else:
        ratios["inventory_days"] = None

    # Receivable Days = (Trade Receivables / Revenue) × 365
    if current.revenue > 0 and current.trade_receivables > 0:
        ratios["receivable_days"] = round((current.trade_receivables * 365) / current.revenue, 2)
    else:
        ratios["receivable_days"] = None

    # Payable Days = (Trade Payables / COGS) × 365
    if cogs > 0 and current.trade_payables > 0:
        ratios["payable_days"] = round((current.trade_payables * 365) / cogs, 2)
    else:
        ratios["payable_days"] = None

    # Cash Conversion Cycle = Inventory Days + Receivable Days - Payable Days
    if all(
        ratios.get(k) is not None
        for k in ["inventory_days", "receivable_days", "payable_days"]
    ):
        ratios["cash_conversion_cycle"] = round(
            ratios["inventory_days"] + ratios["receivable_days"] - ratios["payable_days"],
            2,
        )
    else:
        ratios["cash_conversion_cycle"] = None

    # ===== LEVERAGE =====

    # Debt to Equity = Total Borrowings / Total Equity
    ratios["debt_equity"] = safe_div(current.total_borrowings, current.total_equity)

    # Current Ratio = Current Assets / Current Liabilities
    ratios["current_ratio"] = safe_div(
        current.total_current_assets, current.total_current_liabilities
    )

    # Interest Coverage = EBIT / Interest Expense
    ratios["interest_coverage"] = safe_div(current.ebit, current.interest_expense)

    # ===== GROWTH (placeholder - needs previous year data) =====
    ratios["revenue_growth"] = None
    ratios["profit_growth"] = None

    # ===== PER SHARE =====
    ratios["eps"] = current.eps_basic if current.eps_basic != 0 else None

    # Free Cash Flow = CFO - Capex
    if current.cfo != 0:
        fcf = current.cfo - abs(current.capex if current.capex else 0)
        ratios["free_cash_flow"] = round(fcf, 2)
    else:
        ratios["free_cash_flow"] = None

    return ratios


def compute_valuation_ratios(
    price: float,
    eps: float,
    book_value: float,
    shares_outstanding: float,
    total_debt: float,
    cash: float,
    ebitda: float,
) -> dict[str, Optional[float]]:
    """
    Compute valuation ratios given market price.

    Useful for quick updates when only price changes.
    """
    ratios = {}

    # Market cap
    market_cap = price * shares_outstanding
    ratios["market_cap"] = round(market_cap, 2) if market_cap > 0 else None

    # PE
    if eps and abs(eps) > 0.001:
        ratios["pe_ratio"] = round(price / eps, 2)
    else:
        ratios["pe_ratio"] = None

    # PB
    bvps = book_value / shares_outstanding if shares_outstanding > 0 else 0
    if bvps > 0:
        ratios["pb_ratio"] = round(price / bvps, 2)
    else:
        ratios["pb_ratio"] = None

    # EV/EBITDA
    if market_cap > 0:
        ev = market_cap + total_debt - cash
        ratios["ev"] = round(ev, 2)
        if ebitda > 0:
            ratios["ev_ebitda"] = round(ev / ebitda, 2)
        else:
            ratios["ev_ebitda"] = None
    else:
        ratios["ev"] = None
        ratios["ev_ebitda"] = None

    return ratios
