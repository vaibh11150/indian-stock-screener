"""
HTML table parser for financial results.

Used for scraping pre-XBRL historical data from NSE/BSE websites
where financial results are published as HTML tables.
"""

import re
from datetime import date
from typing import Any, Optional

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

from config.logging_config import get_logger
from scrapers.utils.normalizer import normalize_field, PL_FIELD_MAP, BS_FIELD_MAP

logger = get_logger(__name__)


def parse_financial_html_table(html: str) -> Optional[dict[str, Any]]:
    """
    Parse an HTML table of financial results into a structured dict.

    Indian financial result HTML tables typically look like:

    | Particulars          | Q1 FY24 | Q1 FY23 | FY24  | FY23  |
    |---------------------|---------|---------|-------|-------|
    | Revenue from Ops    | 12345   | 11234   | 48765 | 45678 |
    | Other Income        | 234     | 200     | 900   | 800   |
    | Total Expenses      | 10000   | 9500    | 40000 | 38000 |
    | ...                 | ...     | ...     | ...   | ...   |

    Args:
        html: HTML content containing financial tables

    Returns:
        Parsed data including periods and line items, or None if parsing fails
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    if not tables:
        logger.warning("No tables found in HTML content")
        return None

    # Find the main financial results table (usually the largest with relevant headers)
    main_table = _find_financial_table(tables)
    if main_table is None:
        logger.warning("Could not identify main financial results table")
        return None

    rows = main_table.find_all("tr")
    if len(rows) < 3:
        logger.warning("Table has too few rows to be a financial statement")
        return None

    # Parse header row for period information
    header_row = _find_header_row(rows)
    if header_row is None:
        logger.warning("Could not find header row with period information")
        return None

    header_cells = header_row.find_all(["th", "td"])
    periods = []
    for cell in header_cells[1:]:  # Skip first (label) column
        text = cell.get_text(strip=True)
        period = parse_period_from_header(text)
        if period:
            periods.append(period)

    if not periods:
        logger.warning("Could not parse any periods from header")
        return None

    # Parse data rows
    result_columns = [{} for _ in periods]

    for row in rows:
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue

        field_name = cells[0].get_text(strip=True)
        if not field_name or _is_header_text(field_name):
            continue

        canonical = normalize_field(field_name, {**PL_FIELD_MAP, **BS_FIELD_MAP})

        for i, cell in enumerate(cells[1:]):
            if i >= len(periods):
                break

            value = parse_numeric(cell.get_text(strip=True))
            if value is not None:
                key = canonical if canonical else _clean_field_name(field_name)
                result_columns[i][key] = value

    return {
        "periods": periods,
        "data": result_columns,
    }


def _find_financial_table(tables: list) -> Optional[Any]:
    """
    Find the main financial results table from a list of tables.

    Uses heuristics:
    1. Look for tables with financial keywords in headers
    2. Prefer larger tables
    3. Look for tables with numeric data
    """
    financial_keywords = [
        "revenue",
        "income",
        "expenses",
        "profit",
        "loss",
        "particulars",
        "sales",
        "ebitda",
        "assets",
        "liabilities",
    ]

    candidates = []

    for table in tables:
        # Check for financial keywords in table text
        table_text = table.get_text().lower()
        keyword_count = sum(1 for kw in financial_keywords if kw in table_text)

        if keyword_count >= 2:
            row_count = len(table.find_all("tr"))
            candidates.append((table, keyword_count, row_count))

    if not candidates:
        # Fall back to largest table
        return max(tables, key=lambda t: len(t.find_all("tr")), default=None)

    # Sort by keyword count, then by row count
    candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return candidates[0][0]


def _find_header_row(rows: list) -> Optional[Any]:
    """Find the header row containing period information."""
    period_patterns = [
        r"q[1-4]",
        r"fy\s*\d{2,4}",
        r"\d{4}-\d{2,4}",
        r"quarter",
        r"year",
        r"mar",
        r"jun",
        r"sep",
        r"dec",
        r"ended",
    ]

    for row in rows[:5]:  # Check first 5 rows
        row_text = row.get_text().lower()
        match_count = sum(1 for p in period_patterns if re.search(p, row_text))
        if match_count >= 2:
            return row

    # Fall back to first row
    return rows[0] if rows else None


def _is_header_text(text: str) -> bool:
    """Check if text is a header/label rather than a data field."""
    header_indicators = [
        "particulars",
        "description",
        "items",
        "statement of",
        "balance sheet",
        "profit and loss",
        "cash flow",
        "notes",
    ]
    text_lower = text.lower().strip()
    return any(indicator in text_lower for indicator in header_indicators)


def _clean_field_name(field_name: str) -> str:
    """Clean and normalize a field name."""
    # Remove common prefixes/suffixes
    cleaned = re.sub(r"^[\d\.\)\s]+", "", field_name)  # Remove numbering
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    cleaned = re.sub(r"[^\w_]", "", cleaned)
    return cleaned.lower()


def parse_numeric(text: str) -> Optional[float]:
    """
    Parse a numeric value from an HTML cell.

    Handles Indian formatting conventions:
    - Lakhs/Crores notation
    - Comma separators
    - Parentheses for negative numbers
    - Dashes for zero

    Args:
        text: Text content from table cell

    Returns:
        Parsed float value or None
    """
    if not text:
        return None

    text = text.strip()

    # Handle empty/dash cells
    if text in ["-", "--", "—", "", "NA", "N/A", "nil", "Nil", "NIL"]:
        return 0.0

    # Remove commas and spaces
    text = text.replace(",", "").replace(" ", "")

    # Handle parentheses for negative numbers: (1234) → -1234
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]

    # Handle Indian notation for lakhs/crores
    # Some tables use (in Lakhs) or (in Cr) in headers
    multiplier = 1.0

    # Remove any currency symbols
    text = re.sub(r"[₹$€£]", "", text)
    text = re.sub(r"rs\.?", "", text, flags=re.IGNORECASE)

    # Try to parse
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def parse_period_from_header(text: str) -> Optional[dict[str, Any]]:
    """
    Parse period information from a table header.

    Examples:
    - 'Quarter ended 30-Jun-2024' → {'end': date(2024, 6, 30), 'type': 'quarterly'}
    - 'Year ended 31-Mar-2024' → {'end': date(2024, 3, 31), 'type': 'annual'}
    - 'Q1 FY24' → {'end': date(2023, 6, 30), 'type': 'quarterly', 'quarter': 1}
    - 'Sep 2023' → {'end': date(2023, 9, 30), 'type': 'quarterly'}
    - 'FY2024' → {'end': date(2024, 3, 31), 'type': 'annual'}
    - '2023-24' → {'end': date(2024, 3, 31), 'type': 'annual'}

    Args:
        text: Header cell text

    Returns:
        Dict with period_start, period_end, period_type, etc. or None
    """
    if not text:
        return None

    text = text.strip()
    result = {"original": text}

    # Pattern: "Quarter ended DD-Mon-YYYY" or "Year ended DD-Mon-YYYY"
    ended_match = re.search(
        r"(quarter|year|half\s*year|nine\s*months?)\s+ended\s+(\d{1,2}[-/]\w{3}[-/]\d{2,4})",
        text,
        re.IGNORECASE,
    )
    if ended_match:
        period_type = ended_match.group(1).lower()
        date_str = ended_match.group(2)
        try:
            end_date = parse_date(date_str, dayfirst=True).date()
            result["end"] = end_date
            result["type"] = _normalize_period_type(period_type)
            result["start"] = _calculate_period_start(end_date, result["type"])
            return result
        except Exception:
            pass

    # Pattern: "Q1 FY24" or "Q2FY2024"
    quarter_fy_match = re.search(r"q([1-4])\s*fy\s*(\d{2,4})", text, re.IGNORECASE)
    if quarter_fy_match:
        quarter = int(quarter_fy_match.group(1))
        fy_year = quarter_fy_match.group(2)
        if len(fy_year) == 2:
            fy_year = "20" + fy_year
        fy_year = int(fy_year)

        # Convert FY quarter to calendar dates
        # Q1 of FY24 = Apr-Jun 2023 (ends Jun 30, 2023)
        # Q2 of FY24 = Jul-Sep 2023 (ends Sep 30, 2023)
        # Q3 of FY24 = Oct-Dec 2023 (ends Dec 31, 2023)
        # Q4 of FY24 = Jan-Mar 2024 (ends Mar 31, 2024)
        quarter_end_month = {1: 6, 2: 9, 3: 12, 4: 3}
        quarter_end_year = {1: fy_year - 1, 2: fy_year - 1, 3: fy_year - 1, 4: fy_year}
        quarter_end_day = {1: 30, 2: 30, 3: 31, 4: 31}

        end_date = date(
            quarter_end_year[quarter], quarter_end_month[quarter], quarter_end_day[quarter]
        )

        result["end"] = end_date
        result["type"] = "quarterly"
        result["quarter"] = quarter
        result["fiscal_year"] = f"FY{fy_year}"
        result["start"] = _calculate_period_start(end_date, "quarterly")
        return result

    # Pattern: "FY2024" or "FY 2024" or "FY24"
    fy_match = re.search(r"fy\s*(\d{2,4})", text, re.IGNORECASE)
    if fy_match and "q" not in text.lower():  # Exclude Q1FY24 type patterns
        fy_year = fy_match.group(1)
        if len(fy_year) == 2:
            fy_year = "20" + fy_year
        fy_year = int(fy_year)

        # FY2024 ends Mar 31, 2024
        end_date = date(fy_year, 3, 31)
        result["end"] = end_date
        result["type"] = "annual"
        result["fiscal_year"] = f"FY{fy_year}"
        result["start"] = date(fy_year - 1, 4, 1)
        return result

    # Pattern: "2023-24" or "2023-2024"
    range_match = re.search(r"(\d{4})[-/](\d{2,4})", text)
    if range_match:
        start_year = int(range_match.group(1))
        end_year = range_match.group(2)
        if len(end_year) == 2:
            end_year = str(start_year)[:2] + end_year
        end_year = int(end_year)

        if end_year - start_year == 1:  # Fiscal year
            end_date = date(end_year, 3, 31)
            result["end"] = end_date
            result["type"] = "annual"
            result["fiscal_year"] = f"FY{end_year}"
            result["start"] = date(start_year, 4, 1)
            return result

    # Pattern: "Mar 2024" or "March 2024" or "31-Mar-2024"
    month_year_match = re.search(
        r"(\d{1,2}[-/])?(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[-/\s]*(\d{2,4})",
        text,
        re.IGNORECASE,
    )
    if month_year_match:
        day_part = month_year_match.group(1)
        month_str = month_year_match.group(2)
        year_str = month_year_match.group(3)

        if len(year_str) == 2:
            year_str = "20" + year_str

        try:
            if day_part:
                date_str = f"{day_part}{month_str} {year_str}"
            else:
                date_str = f"1 {month_str} {year_str}"

            parsed_date = parse_date(date_str, dayfirst=True).date()

            # If no day was specified, use end of month
            if not day_part:
                import calendar

                last_day = calendar.monthrange(parsed_date.year, parsed_date.month)[1]
                parsed_date = date(parsed_date.year, parsed_date.month, last_day)

            result["end"] = parsed_date

            # Determine period type based on month
            if parsed_date.month == 3:  # March - likely annual
                result["type"] = "annual"
            else:
                result["type"] = "quarterly"

            result["start"] = _calculate_period_start(parsed_date, result["type"])
            return result
        except Exception:
            pass

    return None


def _normalize_period_type(period_type: str) -> str:
    """Normalize period type string."""
    period_type = period_type.lower().strip()
    if "quarter" in period_type:
        return "quarterly"
    if "half" in period_type:
        return "half_yearly"
    if "nine" in period_type:
        return "nine_months"
    if "year" in period_type or "annual" in period_type:
        return "annual"
    return "quarterly"


def _calculate_period_start(end_date: date, period_type: str) -> date:
    """Calculate period start date from end date and period type."""
    from dateutil.relativedelta import relativedelta

    if period_type == "quarterly":
        return end_date - relativedelta(months=3) + relativedelta(days=1)
    elif period_type == "half_yearly":
        return end_date - relativedelta(months=6) + relativedelta(days=1)
    elif period_type == "nine_months":
        return end_date - relativedelta(months=9) + relativedelta(days=1)
    else:  # annual
        return end_date - relativedelta(years=1) + relativedelta(days=1)


def extract_tables_from_html(html: str) -> list[dict[str, Any]]:
    """
    Extract all tables from HTML content and classify them.

    Args:
        html: HTML content

    Returns:
        List of dicts with table info and parsed content
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    results = []
    for i, table in enumerate(tables):
        table_text = table.get_text().lower()

        # Classify table
        if any(kw in table_text for kw in ["revenue", "income", "profit", "loss", "expenses"]):
            table_type = "profit_loss"
        elif any(kw in table_text for kw in ["assets", "liabilities", "equity", "capital"]):
            table_type = "balance_sheet"
        elif any(kw in table_text for kw in ["cash flow", "operating activities", "investing"]):
            table_type = "cash_flow"
        else:
            table_type = "unknown"

        parsed = parse_financial_html_table(str(table))

        results.append(
            {
                "index": i,
                "type": table_type,
                "row_count": len(table.find_all("tr")),
                "parsed": parsed,
            }
        )

    return results
