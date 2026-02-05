#!/usr/bin/env python
"""
Fetch sample data from NSE/BSE for testing and offline population.

This script fetches financial data for top companies and saves it to JSON files
that can be loaded into the database without needing live API access.
"""

import asyncio
import json
import os
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Any, Optional

import httpx
import pandas as pd

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "sample"

# Top NSE companies to fetch
SAMPLE_COMPANIES = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "SUNPHARMA",
    "TITAN", "BAJFINANCE", "WIPRO", "HCLTECH", "NESTLEIND",
    "ULTRACEMCO", "ADANIENT", "NTPC", "POWERGRID", "ONGC",
    "JSWSTEEL", "TATAMOTORS", "COALINDIA", "TECHM", "M&M",
]


class NSEDataFetcher:
    """Fetches data from NSE APIs."""

    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "X-Requested-With": "XMLHttpRequest",
        }

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        # Initialize session with NSE homepage
        await self._get_cookies()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def _get_cookies(self):
        """Visit NSE homepage to get session cookies."""
        print("Initializing NSE session...")
        await self.client.get(
            "https://www.nseindia.com",
            headers={"User-Agent": self.headers["User-Agent"], "Accept": "text/html"},
        )

    async def fetch_equity_list(self) -> pd.DataFrame:
        """Fetch the complete NSE equity list."""
        print("Fetching NSE equity list...")
        resp = await self.client.get(
            "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
            headers=self.headers,
        )
        if resp.status_code == 200:
            df = pd.read_csv(StringIO(resp.text))
            print(f"  Found {len(df)} companies")
            return df
        raise Exception(f"Failed to fetch equity list: {resp.status_code}")

    async def fetch_company_info(self, symbol: str) -> Optional[dict]:
        """Fetch company information."""
        try:
            resp = await self.client.get(
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}",
                headers=self.headers,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"  Warning: Could not fetch info for {symbol}: {e}")
        return None

    async def fetch_financials(self, symbol: str) -> Optional[dict]:
        """Fetch financial results comparison data."""
        try:
            resp = await self.client.get(
                f"https://www.nseindia.com/api/results-comparision?symbol={symbol}",
                headers=self.headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data
        except Exception as e:
            print(f"  Warning: Could not fetch financials for {symbol}: {e}")
        return None

    async def fetch_price_history(self, symbol: str, days: int = 365) -> Optional[list]:
        """Fetch historical price data."""
        try:
            from_date = (datetime.now() - pd.Timedelta(days=days)).strftime("%d-%m-%Y")
            to_date = datetime.now().strftime("%d-%m-%Y")

            resp = await self.client.get(
                f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&from={from_date}&to={to_date}",
                headers=self.headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data", [])
        except Exception as e:
            print(f"  Warning: Could not fetch price history for {symbol}: {e}")
        return None


def parse_nse_financials(data: dict, symbol: str) -> dict:
    """Parse NSE financial data into our canonical format."""
    results = []

    res_data = data.get("resCmpData") or []
    if not res_data:
        return {"symbol": symbol, "results": []}

    for record in res_data:
        period_end = record.get("re_to_dt")
        if not period_end:
            continue

        # Parse the date
        try:
            from datetime import datetime
            period_end_date = datetime.strptime(period_end, "%d-%b-%Y").date()
        except:
            try:
                period_end_date = datetime.strptime(period_end, "%d-%m-%Y").date()
            except:
                continue

        # Determine if quarterly or annual
        period_start = record.get("re_from_dt")
        if period_start:
            try:
                period_start_date = datetime.strptime(period_start, "%d-%b-%Y").date()
                days = (period_end_date - period_start_date).days
                period_type = "annual" if days > 300 else "quarterly"
            except:
                period_type = "quarterly"
        else:
            period_type = "quarterly"

        # Extract financial items
        items = {}

        # Revenue
        if record.get("re_net_sale"):
            items["revenue"] = float(record["re_net_sale"]) / 100  # Convert from lakhs to crores

        # Expenses
        if record.get("re_tot_exp_exc_pro_cont"):
            items["total_expenses"] = float(record["re_tot_exp_exc_pro_cont"]) / 100
        elif record.get("re_rawmat_consump") and record.get("re_staff_cost") and record.get("re_oth_exp"):
            raw = float(record.get("re_rawmat_consump", 0) or 0)
            staff = float(record.get("re_staff_cost", 0) or 0)
            other = float(record.get("re_oth_exp", 0) or 0)
            purchase = float(record.get("re_pur_trd_goods", 0) or 0)
            items["total_expenses"] = (raw + staff + other + purchase) / 100
            items["raw_material_cost"] = raw / 100
            items["employee_cost"] = staff / 100
            items["other_expenses"] = other / 100

        # Profit
        if record.get("re_con_pro_loss"):
            items["net_profit"] = float(record["re_con_pro_loss"]) / 100
        elif record.get("re_proloss_ord_act"):
            items["net_profit"] = float(record["re_proloss_ord_act"]) / 100

        # Interest
        if record.get("re_int_new"):
            items["interest_expense"] = float(record["re_int_new"]) / 100

        # EPS
        if record.get("re_basic_eps_for_cont_dic_opr"):
            try:
                items["eps_basic"] = float(record["re_basic_eps_for_cont_dic_opr"])
            except:
                pass
        if record.get("re_dilut_eps_for_cont_dic_opr"):
            try:
                items["eps_diluted"] = float(record["re_dilut_eps_for_cont_dic_opr"])
            except:
                pass

        # Face value
        if record.get("re_face_val"):
            items["face_value"] = float(record["re_face_val"])

        if items:
            results.append({
                "period_end": period_end_date.isoformat(),
                "period_type": period_type,
                "items": items,
            })

    return {
        "symbol": symbol,
        "results": results,
    }


async def fetch_and_save_sample_data():
    """Fetch sample data and save to JSON files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_data = {
        "companies": [],
        "financials": {},
        "fetched_at": datetime.now().isoformat(),
    }

    async with NSEDataFetcher() as fetcher:
        # Fetch equity list
        equity_df = await fetcher.fetch_equity_list()

        # Save full equity list
        equity_list_path = OUTPUT_DIR / "nse_equity_list.csv"
        equity_df.to_csv(equity_list_path, index=False)
        print(f"Saved equity list to {equity_list_path}")

        # Process sample companies
        for symbol in SAMPLE_COMPANIES:
            print(f"\nProcessing {symbol}...")

            # Find in equity list
            company_row = equity_df[equity_df["SYMBOL"] == symbol]
            if not company_row.empty:
                row = company_row.iloc[0]
                company_data = {
                    "nse_symbol": symbol,
                    "company_name": str(row.get(" NAME OF COMPANY", row.get("NAME OF COMPANY", ""))).strip(),
                    "isin": str(row.get("ISIN NUMBER", row.get(" ISIN NUMBER", ""))).strip(),
                    "series": str(row.get(" SERIES", row.get("SERIES", ""))).strip(),
                    "face_value": float(row.get(" FACE VALUE", row.get("FACE VALUE", 0)) or 0),
                }
                all_data["companies"].append(company_data)

            # Fetch financials
            await asyncio.sleep(1.5)  # Rate limit
            financials = await fetcher.fetch_financials(symbol)

            if financials:
                parsed = parse_nse_financials(financials, symbol)
                all_data["financials"][symbol] = parsed
                print(f"  Found {len(parsed['results'])} financial periods")

            # Fetch company info
            await asyncio.sleep(1.0)
            info = await fetcher.fetch_company_info(symbol)
            if info:
                # Add additional info to company data
                for c in all_data["companies"]:
                    if c["nse_symbol"] == symbol:
                        price_info = info.get("priceInfo", {})
                        c["current_price"] = price_info.get("lastPrice")
                        c["market_cap"] = info.get("info", {}).get("marketCap")
                        c["industry"] = info.get("industryInfo", {}).get("industry")
                        c["sector"] = info.get("industryInfo", {}).get("sector")
                        break

    # Save all data to JSON
    output_path = OUTPUT_DIR / "sample_data.json"
    with open(output_path, "w") as f:
        json.dump(all_data, f, indent=2, default=str)
    print(f"\nSaved all data to {output_path}")

    # Summary
    print("\n" + "=" * 50)
    print("FETCH SUMMARY")
    print("=" * 50)
    print(f"Companies: {len(all_data['companies'])}")
    print(f"Financial data: {len(all_data['financials'])} companies")
    total_periods = sum(len(f['results']) for f in all_data['financials'].values())
    print(f"Total financial periods: {total_periods}")
    print("=" * 50)

    return all_data


async def main():
    print("=== NSE Sample Data Fetcher ===\n")
    await fetch_and_save_sample_data()


if __name__ == "__main__":
    asyncio.run(main())
