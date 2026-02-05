#!/usr/bin/env python
"""
Load sample data into SQLite for testing.

This script loads the fetched sample data into a SQLite database,
allowing testing without requiring PostgreSQL.

Usage:
    python scripts/load_sample_to_sqlite.py
"""

import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "sample"
DB_PATH = PROJECT_ROOT / "data" / "indian_screener.db"


def create_tables(conn: sqlite3.Connection):
    """Create database tables."""
    cursor = conn.cursor()

    # Companies table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nse_symbol TEXT UNIQUE,
            bse_scrip_code TEXT,
            isin TEXT UNIQUE NOT NULL,
            company_name TEXT NOT NULL,
            industry TEXT,
            sector TEXT,
            bse_group TEXT,
            listing_date DATE,
            face_value REAL,
            is_active BOOLEAN DEFAULT 1,
            current_price REAL,
            market_cap REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Financial statements table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_statements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            statement_type TEXT NOT NULL,
            result_nature TEXT DEFAULT 'consolidated',
            period_type TEXT NOT NULL,
            period_start DATE,
            period_end DATE NOT NULL,
            fiscal_year TEXT,
            fiscal_quarter INTEGER,
            is_audited BOOLEAN DEFAULT 0,
            source TEXT,
            source_url TEXT,
            filing_date DATE,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_id) REFERENCES companies(id),
            UNIQUE(company_id, statement_type, result_nature, period_type, period_end)
        )
    """)

    # Financial line items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_line_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            field_value REAL,
            display_order INTEGER,
            FOREIGN KEY (statement_id) REFERENCES financial_statements(id) ON DELETE CASCADE,
            UNIQUE(statement_id, field_name)
        )
    """)

    # Daily prices table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            trade_date DATE NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            adj_close REAL,
            volume INTEGER,
            delivery_qty INTEGER,
            source TEXT DEFAULT 'nse',
            FOREIGN KEY (company_id) REFERENCES companies(id),
            UNIQUE(company_id, trade_date)
        )
    """)

    # Computed ratios table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS computed_ratios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            period_end DATE NOT NULL,
            period_type TEXT NOT NULL,
            is_ttm BOOLEAN DEFAULT 0,
            result_nature TEXT DEFAULT 'consolidated',
            market_cap REAL,
            pe_ratio REAL,
            pb_ratio REAL,
            ev REAL,
            ev_ebitda REAL,
            dividend_yield REAL,
            roe REAL,
            roce REAL,
            roa REAL,
            operating_margin REAL,
            net_margin REAL,
            asset_turnover REAL,
            inventory_days REAL,
            receivable_days REAL,
            payable_days REAL,
            cash_conversion_cycle REAL,
            debt_equity REAL,
            current_ratio REAL,
            interest_coverage REAL,
            revenue_growth REAL,
            profit_growth REAL,
            eps REAL,
            book_value_per_share REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_id) REFERENCES companies(id),
            UNIQUE(company_id, period_end, period_type, is_ttm, result_nature)
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_nse ON companies(nse_symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_isin ON companies(isin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fs_company_period ON financial_statements(company_id, period_end)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fli_statement ON financial_line_items(statement_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_company_date ON daily_prices(company_id, trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ratios_screen ON computed_ratios(period_type, is_ttm)")

    conn.commit()
    print("Created database tables")


def load_companies(conn: sqlite3.Connection, companies: list[dict]):
    """Load companies into database."""
    cursor = conn.cursor()

    for company in companies:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO companies
                (nse_symbol, isin, company_name, industry, sector, face_value, current_price, market_cap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company.get("nse_symbol"),
                company.get("isin"),
                company.get("company_name"),
                company.get("industry"),
                company.get("sector"),
                company.get("face_value"),
                company.get("current_price"),
                company.get("market_cap"),
            ))
        except Exception as e:
            print(f"  Error loading {company.get('nse_symbol')}: {e}")

    conn.commit()
    print(f"Loaded {len(companies)} companies")


def load_financials(conn: sqlite3.Connection, financials: dict):
    """Load financial data into database."""
    cursor = conn.cursor()

    statements_inserted = 0
    line_items_inserted = 0

    for symbol, data in financials.items():
        # Get company ID
        cursor.execute("SELECT id FROM companies WHERE nse_symbol = ?", (symbol,))
        row = cursor.fetchone()
        if not row:
            print(f"  Warning: Company {symbol} not found")
            continue

        company_id = row[0]

        for result in data.get("results", []):
            period_end = result.get("period_end")
            if not period_end:
                continue

            period_type = result.get("period_type", "quarterly")

            # Calculate fiscal year
            period_date = datetime.fromisoformat(period_end).date()
            if period_date.month >= 4:
                fiscal_year = f"FY{period_date.year + 1}"
            else:
                fiscal_year = f"FY{period_date.year}"

            # Calculate period start
            if period_type == "annual":
                period_start = date(period_date.year - 1, 4, 1).isoformat()
            else:
                from datetime import timedelta
                period_start = (period_date - timedelta(days=90)).isoformat()

            try:
                # Insert statement
                cursor.execute("""
                    INSERT OR REPLACE INTO financial_statements
                    (company_id, statement_type, result_nature, period_type, period_start, period_end, fiscal_year, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    company_id,
                    "profit_loss",
                    "consolidated",
                    period_type,
                    period_start,
                    period_end,
                    fiscal_year,
                    "nse_api",
                ))

                statement_id = cursor.lastrowid
                statements_inserted += 1

                # Insert line items
                for field_name, field_value in result.get("items", {}).items():
                    if field_value is not None:
                        cursor.execute("""
                            INSERT OR REPLACE INTO financial_line_items
                            (statement_id, field_name, field_value)
                            VALUES (?, ?, ?)
                        """, (statement_id, field_name, field_value))
                        line_items_inserted += 1

            except Exception as e:
                print(f"  Error loading financial for {symbol} {period_end}: {e}")

    conn.commit()
    print(f"Loaded {statements_inserted} statements with {line_items_inserted} line items")


def compute_basic_ratios(conn: sqlite3.Connection):
    """Compute basic financial ratios."""
    cursor = conn.cursor()

    # Get all companies with financial data
    cursor.execute("""
        SELECT DISTINCT c.id, c.nse_symbol, c.current_price
        FROM companies c
        JOIN financial_statements fs ON c.id = fs.company_id
    """)
    companies = cursor.fetchall()

    ratios_computed = 0

    for company_id, symbol, current_price in companies:
        # Get latest financial data (TTM approximation - just use latest quarter)
        cursor.execute("""
            SELECT fs.id, fs.period_end
            FROM financial_statements fs
            WHERE fs.company_id = ? AND fs.statement_type = 'profit_loss'
            ORDER BY fs.period_end DESC
            LIMIT 4
        """, (company_id,))
        statements = cursor.fetchall()

        if not statements:
            continue

        # Aggregate TTM data
        ttm_data = {}
        latest_period_end = statements[0][1]

        for stmt_id, _ in statements:
            cursor.execute("""
                SELECT field_name, field_value
                FROM financial_line_items
                WHERE statement_id = ?
            """, (stmt_id,))
            items = cursor.fetchall()

            for field_name, field_value in items:
                if field_value is not None:
                    if field_name in ttm_data:
                        ttm_data[field_name] += field_value
                    else:
                        ttm_data[field_name] = field_value

        # Compute ratios
        revenue = ttm_data.get("revenue", 0)
        net_profit = ttm_data.get("net_profit", 0)
        eps = ttm_data.get("eps_basic", 0)

        pe_ratio = None
        if current_price and eps and eps > 0:
            pe_ratio = round(current_price / eps, 2)

        net_margin = None
        if revenue and revenue > 0:
            net_margin = round((net_profit / revenue) * 100, 2)

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO computed_ratios
                (company_id, period_end, period_type, is_ttm, result_nature,
                 pe_ratio, net_margin, eps)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_id,
                latest_period_end,
                "quarterly",
                True,
                "consolidated",
                pe_ratio,
                net_margin,
                eps,
            ))
            ratios_computed += 1
        except Exception as e:
            print(f"  Error computing ratios for {symbol}: {e}")

    conn.commit()
    print(f"Computed ratios for {ratios_computed} companies")


def show_database_stats(conn: sqlite3.Connection):
    """Display database statistics."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)

    tables = [
        "companies",
        "financial_statements",
        "financial_line_items",
        "daily_prices",
        "computed_ratios",
    ]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:30} {count:>10,}")

    print("=" * 60)

    # Show sample data
    print("\nSAMPLE COMPANIES WITH DATA:")
    print("-" * 60)

    cursor.execute("""
        SELECT c.nse_symbol, c.company_name, c.current_price, c.sector,
               cr.pe_ratio, cr.net_margin, cr.eps
        FROM companies c
        LEFT JOIN computed_ratios cr ON c.id = cr.company_id AND cr.is_ttm = 1
        ORDER BY c.nse_symbol
        LIMIT 15
    """)

    print(f"{'Symbol':<12} {'Company':<25} {'Price':>10} {'PE':>8} {'Margin':>8} {'EPS':>8}")
    print("-" * 72)

    for row in cursor.fetchall():
        symbol, name, price, sector, pe, margin, eps = row
        name = (name[:22] + "...") if len(name) > 25 else name
        price_str = f"{price:,.0f}" if price else "-"
        pe_str = f"{pe:.1f}" if pe else "-"
        margin_str = f"{margin:.1f}%" if margin else "-"
        eps_str = f"{eps:.2f}" if eps else "-"
        print(f"{symbol:<12} {name:<25} {price_str:>10} {pe_str:>8} {margin_str:>8} {eps_str:>8}")


def main():
    """Main entry point."""
    print("=== Loading Sample Data into SQLite ===\n")

    # Check for sample data
    sample_file = DATA_DIR / "sample_data.json"
    if not sample_file.exists():
        print(f"Error: Sample data not found at {sample_file}")
        print("Run `python scripts/fetch_sample_data.py` first")
        return

    # Load sample data
    print(f"Loading sample data from {sample_file}...")
    with open(sample_file) as f:
        data = json.load(f)

    print(f"  Companies: {len(data['companies'])}")
    print(f"  Financial data for: {len(data['financials'])} companies")

    # Create database
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nCreating database at {DB_PATH}...")

    conn = sqlite3.connect(DB_PATH)

    try:
        # Create tables
        create_tables(conn)

        # Load data
        print("\nLoading companies...")
        load_companies(conn, data["companies"])

        print("\nLoading financial data...")
        load_financials(conn, data["financials"])

        print("\nComputing ratios...")
        compute_basic_ratios(conn)

        # Show stats
        show_database_stats(conn)

    finally:
        conn.close()

    print(f"\n✅ Database created successfully at {DB_PATH}")
    print("\nYou can query it with:")
    print(f"  sqlite3 {DB_PATH}")


if __name__ == "__main__":
    main()
