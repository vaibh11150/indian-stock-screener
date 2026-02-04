"""
Field normalizer for mapping raw field names to canonical schema.

NSE, BSE, XBRL, and HTML all use different field names for the same data points.
This module provides mappings to normalize them to a consistent schema.
"""

from typing import Optional

# ===== PROFIT & LOSS FIELDS =====
PL_FIELD_MAP = {
    # Canonical Name: [possible raw field names from various sources]
    "revenue": [
        "Revenue",
        "RevenueFromOperations",
        "Revenue From Operations",
        "revenue_from_operations",
        "IncomeFromOperations",
        "Income From Operations",
        "Sales",
        "Net Sales",
        "Turnover",
        "Total Revenue From Operations",
        "GrossRevenue",
        "Gross Revenue",
        "re_RevenueFromOperations",
        "RevenueFromOperationsGross",
    ],
    "other_income": [
        "OtherIncome",
        "Other Income",
        "other_income",
        "re_OtherIncome",
        "OtherOperatingRevenue",
    ],
    "total_income": [
        "TotalIncome",
        "Total Income",
        "total_income",
        "TotalRevenueFromOperations",
        "TotalRevenue",
        "Total Revenue",
    ],
    "raw_material_cost": [
        "CostOfMaterialsConsumed",
        "Cost Of Materials Consumed",
        "RawMaterialCost",
        "Raw Material Cost",
        "CostOfRawMaterialsConsumed",
        "re_CostOfMaterialsConsumed",
    ],
    "purchase_stock_in_trade": [
        "PurchaseOfStockInTrade",
        "Purchase Of Stock In Trade",
        "PurchasesOfStockInTrade",
        "re_PurchasesOfStockInTrade",
    ],
    "inventory_change": [
        "ChangesInInventories",
        "Changes In Inventories",
        "InventoryChange",
        "ChangesInInventoriesOfFinishedGoods",
        "re_ChangesInInventoriesOfFinishedGoodsWorkInProgressAndStockInTrade",
    ],
    "employee_cost": [
        "EmployeeBenefitExpense",
        "Employee Benefit Expense",
        "EmployeeCost",
        "Employee Cost",
        "Staff Cost",
        "EmployeeBenefitsExpense",
        "re_EmployeeBenefitExpense",
    ],
    "depreciation": [
        "DepreciationAndAmortisation",
        "Depreciation And Amortisation",
        "Depreciation",
        "DepreciationAndAmortizationExpense",
        "DepreciationDepletionAndAmortisationExpense",
        "re_DepreciationAndAmortisationExpense",
    ],
    "interest_expense": [
        "FinanceCosts",
        "Finance Costs",
        "InterestExpense",
        "Interest",
        "Interest Expense",
        "FinanceCost",
        "re_FinanceCosts",
    ],
    "other_expenses": [
        "OtherExpenses",
        "Other Expenses",
        "re_OtherExpenses",
    ],
    "total_expenses": [
        "TotalExpenses",
        "Total Expenses",
        "total_expenses",
        "re_TotalExpenses",
    ],
    "operating_profit": [
        "OperatingProfit",
        "Operating Profit",
        "EBITDA",
        "ProfitBeforeDepreciationInterestAndTax",
        "re_ProfitBeforeExceptionalItemsAndTax",
    ],
    "profit_before_exceptional": [
        "ProfitBeforeExceptionalItems",
        "Profit Before Exceptional Items",
        "ProfitBeforeExceptionalItemsAndTax",
        "re_ProfitBeforeExceptionalItemsAndTax",
    ],
    "exceptional_items": [
        "ExceptionalItems",
        "Exceptional Items",
        "re_ExceptionalItems",
    ],
    "profit_before_tax": [
        "ProfitBeforeTax",
        "Profit Before Tax",
        "profit_before_tax",
        "PBT",
        "re_ProfitBeforeTax",
    ],
    "tax_expense": [
        "TaxExpense",
        "Tax Expense",
        "IncomeTaxExpense",
        "tax_expense",
        "re_TaxExpense",
        "CurrentTax",
        "DeferredTax",
    ],
    "net_profit": [
        "ProfitForThePeriod",
        "Profit For The Period",
        "NetProfit",
        "Net Profit",
        "net_profit",
        "ProfitAfterTax",
        "PAT",
        "re_ProfitForThePeriod",
        "ProfitLossForThePeriod",
        "ProfitLossForPeriodFromContinuingOperations",
    ],
    "eps_basic": [
        "EarningsPerShareBasic",
        "Basic EPS",
        "BasicEPS",
        "eps_basic",
        "EarningsPerEquityShareBasic",
        "re_BasicEPS",
        "BasicEarningsPerShare",
    ],
    "eps_diluted": [
        "EarningsPerShareDiluted",
        "Diluted EPS",
        "DilutedEPS",
        "eps_diluted",
        "EarningsPerEquityShareDiluted",
        "re_DilutedEPS",
        "DilutedEarningsPerShare",
    ],
    "dividend_per_share": [
        "DividendPerShare",
        "Dividend Per Share",
        "DPS",
    ],
}

# ===== BALANCE SHEET FIELDS =====
BS_FIELD_MAP = {
    "share_capital": [
        "ShareCapital",
        "Share Capital",
        "Equity Share Capital",
        "EquityShareCapital",
        "PaidUpCapital",
        "bs_EquityShareCapital",
    ],
    "reserves_surplus": [
        "ReservesAndSurplus",
        "Reserves And Surplus",
        "Reserves & Surplus",
        "OtherEquity",
        "Other Equity",
        "bs_OtherEquity",
        "RetainedEarnings",
    ],
    "total_equity": [
        "TotalEquity",
        "Total Equity",
        "ShareholdersEquity",
        "Total Shareholders Equity",
        "Shareholders Funds",
        "ShareholdersFunds",
        "bs_TotalEquity",
        "EquityAttributableToOwnersOfParent",
    ],
    "minority_interest": [
        "MinorityInterest",
        "Minority Interest",
        "NonControllingInterests",
        "Non Controlling Interests",
        "bs_NonControllingInterests",
    ],
    "long_term_borrowings": [
        "LongTermBorrowings",
        "Long Term Borrowings",
        "NonCurrentBorrowings",
        "Non Current Borrowings",
        "bs_NonCurrentBorrowings",
        "LongTermDebt",
    ],
    "short_term_borrowings": [
        "ShortTermBorrowings",
        "Short Term Borrowings",
        "CurrentBorrowings",
        "Current Borrowings",
        "bs_CurrentBorrowings",
        "ShortTermDebt",
    ],
    "total_borrowings": [
        "TotalBorrowings",
        "Total Borrowings",
        "Total Debt",
        "TotalDebt",
    ],
    "other_non_current_liabilities": [
        "OtherNonCurrentLiabilities",
        "Other Non Current Liabilities",
        "bs_OtherNonCurrentLiabilities",
    ],
    "total_non_current_liabilities": [
        "TotalNonCurrentLiabilities",
        "Total Non Current Liabilities",
        "NonCurrentLiabilities",
        "bs_TotalNonCurrentLiabilities",
    ],
    "trade_payables": [
        "TradePayables",
        "Trade Payables",
        "Creditors",
        "SundryCreditors",
        "bs_TradePayables",
    ],
    "other_current_liabilities": [
        "OtherCurrentLiabilities",
        "Other Current Liabilities",
        "bs_OtherCurrentLiabilities",
    ],
    "total_current_liabilities": [
        "TotalCurrentLiabilities",
        "Total Current Liabilities",
        "CurrentLiabilities",
        "bs_TotalCurrentLiabilities",
    ],
    "total_liabilities": [
        "TotalLiabilities",
        "Total Liabilities",
        "bs_TotalLiabilities",
    ],
    "fixed_assets": [
        "PropertyPlantAndEquipment",
        "Property Plant And Equipment",
        "FixedAssets",
        "Fixed Assets",
        "TangibleAssets",
        "bs_PropertyPlantAndEquipment",
        "NetBlock",
        "GrossBlock",
    ],
    "cwip": [
        "CapitalWorkInProgress",
        "Capital Work In Progress",
        "CWIP",
        "bs_CapitalWorkInProgress",
    ],
    "intangible_assets": [
        "IntangibleAssets",
        "Intangible Assets",
        "OtherIntangibleAssets",
        "bs_OtherIntangibleAssets",
    ],
    "investments": [
        "Investments",
        "TotalInvestments",
        "NonCurrentInvestments",
        "bs_NonCurrentInvestments",
        "bs_CurrentInvestments",
    ],
    "non_current_investments": [
        "NonCurrentInvestments",
        "Non Current Investments",
        "bs_NonCurrentInvestments",
        "LongTermInvestments",
    ],
    "current_investments": [
        "CurrentInvestments",
        "Current Investments",
        "bs_CurrentInvestments",
        "ShortTermInvestments",
    ],
    "total_non_current_assets": [
        "TotalNonCurrentAssets",
        "Total Non Current Assets",
        "NonCurrentAssets",
        "bs_TotalNonCurrentAssets",
    ],
    "inventory": [
        "Inventories",
        "Inventory",
        "Stock",
        "bs_Inventories",
    ],
    "trade_receivables": [
        "TradeReceivables",
        "Trade Receivables",
        "Debtors",
        "SundryDebtors",
        "bs_TradeReceivables",
    ],
    "cash_and_equivalents": [
        "CashAndCashEquivalents",
        "Cash And Cash Equivalents",
        "CashAndBankBalances",
        "Cash And Bank Balances",
        "bs_CashAndCashEquivalents",
        "Cash",
    ],
    "other_current_assets": [
        "OtherCurrentAssets",
        "Other Current Assets",
        "bs_OtherCurrentAssets",
    ],
    "total_current_assets": [
        "TotalCurrentAssets",
        "Total Current Assets",
        "CurrentAssets",
        "bs_TotalCurrentAssets",
    ],
    "total_assets": [
        "TotalAssets",
        "Total Assets",
        "total_assets",
        "bs_TotalAssets",
    ],
    "goodwill": [
        "Goodwill",
        "bs_Goodwill",
    ],
    "deferred_tax_assets": [
        "DeferredTaxAssets",
        "Deferred Tax Assets",
        "bs_DeferredTaxAssets",
    ],
    "deferred_tax_liabilities": [
        "DeferredTaxLiabilities",
        "Deferred Tax Liabilities",
        "bs_DeferredTaxLiabilities",
    ],
}

# ===== CASH FLOW FIELDS =====
CF_FIELD_MAP = {
    "cfo": [
        "CashFlowFromOperatingActivities",
        "Cash Flow From Operating Activities",
        "NetCashFromOperatingActivities",
        "CFO",
        "cf_CashFlowsFromOperatingActivities",
        "NetCashFlowFromOperatingActivities",
    ],
    "cfi": [
        "CashFlowFromInvestingActivities",
        "Cash Flow From Investing Activities",
        "NetCashFromInvestingActivities",
        "CFI",
        "cf_CashFlowsFromInvestingActivities",
        "NetCashFlowFromInvestingActivities",
    ],
    "cff": [
        "CashFlowFromFinancingActivities",
        "Cash Flow From Financing Activities",
        "NetCashFromFinancingActivities",
        "CFF",
        "cf_CashFlowsFromFinancingActivities",
        "NetCashFlowFromFinancingActivities",
    ],
    "net_cash_flow": [
        "NetCashFlow",
        "Net Cash Flow",
        "NetIncreaseDecreaseInCashAndCashEquivalents",
        "cf_NetIncreaseDecreaseInCashAndCashEquivalents",
    ],
    "depreciation_in_cf": [
        "DepreciationAndAmortisation",
        "cf_DepreciationAndAmortisationExpense",
    ],
    "capex": [
        "CapitalExpenditure",
        "Capex",
        "PurchaseOfPropertyPlantAndEquipment",
        "cf_PaymentsForPurchaseOfPropertyPlantAndEquipment",
    ],
    "free_cash_flow": [
        "FreeCashFlow",
        "Free Cash Flow",
        "FCF",
    ],
}


def normalize_field(raw_name: str, field_map: Optional[dict] = None) -> Optional[str]:
    """
    Given a raw field name from a scrape, return the canonical name.

    Args:
        raw_name: Raw field name from source
        field_map: Optional specific field map to use. If None, searches all maps.

    Returns:
        Canonical field name or None if no match found
    """
    if not raw_name:
        return None

    # Normalize input for comparison
    raw_lower = raw_name.strip().lower().replace("_", " ").replace("-", " ")

    # Determine which maps to search
    maps_to_search = [field_map] if field_map else [PL_FIELD_MAP, BS_FIELD_MAP, CF_FIELD_MAP]

    for mapping in maps_to_search:
        if mapping is None:
            continue
        for canonical, variants in mapping.items():
            for variant in variants:
                if raw_lower == variant.lower().replace("_", " "):
                    return canonical

    return None


def get_statement_type(field_name: str) -> Optional[str]:
    """
    Determine which financial statement a field belongs to.

    Args:
        field_name: Canonical or raw field name

    Returns:
        'profit_loss', 'balance_sheet', 'cash_flow', or None
    """
    canonical = normalize_field(field_name)
    if canonical is None:
        canonical = field_name

    if canonical in PL_FIELD_MAP:
        return "profit_loss"
    if canonical in BS_FIELD_MAP:
        return "balance_sheet"
    if canonical in CF_FIELD_MAP:
        return "cash_flow"

    return None


def get_all_canonical_fields(statement_type: Optional[str] = None) -> list[str]:
    """
    Get all canonical field names.

    Args:
        statement_type: Optional filter by statement type

    Returns:
        List of canonical field names
    """
    if statement_type == "profit_loss":
        return list(PL_FIELD_MAP.keys())
    if statement_type == "balance_sheet":
        return list(BS_FIELD_MAP.keys())
    if statement_type == "cash_flow":
        return list(CF_FIELD_MAP.keys())

    return list(PL_FIELD_MAP.keys()) + list(BS_FIELD_MAP.keys()) + list(CF_FIELD_MAP.keys())
