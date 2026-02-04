"""
Price Data Scraper.

Fetches daily OHLCV data from NSE and BSE.
Supports both real-time data and historical bhavcopy archives.
"""

import zipfile
from datetime import date, timedelta
from decimal import Decimal
from io import BytesIO, StringIO
from typing import Any, Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import DailyPrice
from scrapers.base import BaseScraper
from scrapers.utils.session_manager import NSESession, BSESession

logger = get_logger(__name__)

# NSE Bhavcopy archive URL pattern
# {month} is 3-letter uppercase: JAN, FEB, MAR, ...
# {date} is 2-digit day: 01, 02, ..., 31
# {year} is 4-digit: 2024
NSE_BHAVCOPY_URL = (
    "https://archives.nseindia.com/content/historical/EQUITIES"
    "/{year}/{month}/cm{date}{month}{year}bhav.csv.zip"
)

# Alternative NSE equity bhavcopy (newer format)
NSE_BHAVCOPY_ALT_URL = (
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
)


class PriceScraper(BaseScraper):
    """Scraper for daily price data."""

    SCRAPER_NAME = "price_scraper"

    def __init__(self, session: Optional[AsyncSession] = None):
        super().__init__(session)

    async def _scrape(
        self,
        symbol: Optional[str] = None,
        company_id: Optional[int] = None,
        trade_date: Optional[date] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        use_bhavcopy: bool = True,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Fetch price data.

        Args:
            symbol: NSE symbol (for single stock)
            company_id: Company ID in database
            trade_date: Single date to fetch (for bhavcopy)
            from_date: Start date for range
            to_date: End date for range
            use_bhavcopy: Whether to use bhavcopy (faster for full market)

        Returns:
            List of price records
        """
        results = []

        if trade_date:
            # Fetch single day's bhavcopy
            if use_bhavcopy:
                df = await self._fetch_bhavcopy(trade_date)
                if df is not None and not df.empty:
                    results = self._parse_bhavcopy(df, symbol)
                    self.increment_scraped(len(results))
        elif symbol and from_date and to_date:
            # Fetch historical data for single symbol
            results = await self._fetch_historical_prices(symbol, from_date, to_date)
            self.increment_scraped(len(results))
        elif symbol:
            # Fetch recent prices for single symbol
            to_date = date.today()
            from_date = to_date - timedelta(days=365)
            results = await self._fetch_historical_prices(symbol, from_date, to_date)
            self.increment_scraped(len(results))

        # Insert into database
        if self.db_session and results and company_id:
            await self._insert_prices(company_id, results)

        return results

    async def _fetch_bhavcopy(self, trade_date: date) -> Optional[pd.DataFrame]:
        """
        Download and parse NSE bhavcopy for a specific date.

        Args:
            trade_date: The trading date

        Returns:
            DataFrame with OHLCV data or None if not available
        """
        month_str = trade_date.strftime("%b").upper()
        date_str = trade_date.strftime("%d")
        year_str = str(trade_date.year)

        url = NSE_BHAVCOPY_URL.format(
            year=year_str,
            month=month_str,
            date=date_str,
        )

        try:
            async with NSESession() as session:
                # Download the zip file
                response = await session.get(url, raw_response=True)

                # Handle if response is bytes or string
                if isinstance(response, str):
                    content = response.encode()
                else:
                    content = response

                # Extract and parse CSV from zip
                with zipfile.ZipFile(BytesIO(content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)

                # Filter to EQ series only (main equities)
                if "SERIES" in df.columns:
                    df = df[df["SERIES"].isin(["EQ", "BE"])]

                return df

        except Exception as e:
            logger.warning(f"Failed to fetch bhavcopy for {trade_date}: {e}")
            return None

    def _parse_bhavcopy(
        self,
        df: pd.DataFrame,
        symbol: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Parse bhavcopy DataFrame into price records.

        Args:
            df: Bhavcopy DataFrame
            symbol: Optional symbol to filter

        Returns:
            List of price dicts
        """
        results = []

        # Column mapping (bhavcopy columns vary)
        column_map = {
            "SYMBOL": ["SYMBOL", "TckrSymb"],
            "OPEN": ["OPEN", "OpnPric"],
            "HIGH": ["HIGH", "HghPric"],
            "LOW": ["LOW", "LwPric"],
            "CLOSE": ["CLOSE", "ClsPric"],
            "VOLUME": ["TOTTRDQTY", "TtlTradgVol", "VOLUME"],
            "DATE": ["TIMESTAMP", "TradDt", "DATE1"],
        }

        def get_column(name: str) -> Optional[str]:
            for col in column_map.get(name, []):
                if col in df.columns:
                    return col
            return None

        symbol_col = get_column("SYMBOL")
        if not symbol_col:
            return results

        # Filter by symbol if specified
        if symbol:
            df = df[df[symbol_col] == symbol]

        for _, row in df.iterrows():
            try:
                record = {
                    "symbol": str(row.get(symbol_col, "")).strip(),
                    "open_price": float(row.get(get_column("OPEN") or "", 0) or 0),
                    "high_price": float(row.get(get_column("HIGH") or "", 0) or 0),
                    "low_price": float(row.get(get_column("LOW") or "", 0) or 0),
                    "close_price": float(row.get(get_column("CLOSE") or "", 0) or 0),
                    "volume": int(row.get(get_column("VOLUME") or "", 0) or 0),
                    "source": "nse_bhavcopy",
                }

                # Parse date
                date_col = get_column("DATE")
                if date_col and row.get(date_col):
                    from dateutil.parser import parse
                    record["trade_date"] = parse(str(row[date_col])).date()

                if record["symbol"] and record.get("close_price", 0) > 0:
                    results.append(record)

            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")

        return results

    async def _fetch_historical_prices(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch historical prices for a single symbol using NSE API.

        Args:
            symbol: NSE symbol
            from_date: Start date
            to_date: End date

        Returns:
            List of price records
        """
        results = []

        async with NSESession() as session:
            try:
                # NSE historical data endpoint
                # Max range is about 1 year
                data = await session.get(
                    "historical/cm/equity",
                    params={
                        "symbol": symbol,
                        "series": '["EQ"]',
                        "from": from_date.strftime("%d-%m-%Y"),
                        "to": to_date.strftime("%d-%m-%Y"),
                    },
                )

                if data and isinstance(data, dict):
                    records = data.get("data", [])
                    for record in records:
                        try:
                            from dateutil.parser import parse
                            trade_date = parse(record.get("CH_TIMESTAMP", "")).date()

                            results.append({
                                "symbol": symbol,
                                "trade_date": trade_date,
                                "open_price": float(record.get("CH_OPENING_PRICE", 0)),
                                "high_price": float(record.get("CH_TRADE_HIGH_PRICE", 0)),
                                "low_price": float(record.get("CH_TRADE_LOW_PRICE", 0)),
                                "close_price": float(record.get("CH_CLOSING_PRICE", 0)),
                                "volume": int(record.get("CH_TOT_TRADED_QTY", 0)),
                                "source": "nse_api",
                            })
                        except Exception as e:
                            logger.debug(f"Failed to parse price record: {e}")

            except Exception as e:
                self.log_error(f"Failed to fetch historical prices: {e}")

        return results

    async def _insert_prices(
        self,
        company_id: int,
        prices: list[dict[str, Any]],
    ) -> None:
        """Insert price data into the database."""
        for price in prices:
            try:
                if not price.get("trade_date"):
                    continue

                stmt = insert(DailyPrice).values(
                    company_id=company_id,
                    trade_date=price["trade_date"],
                    open_price=Decimal(str(price.get("open_price", 0))),
                    high_price=Decimal(str(price.get("high_price", 0))),
                    low_price=Decimal(str(price.get("low_price", 0))),
                    close_price=Decimal(str(price.get("close_price", 0))),
                    volume=price.get("volume", 0),
                    source=price.get("source", "nse"),
                )
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_daily_price",
                    set_={
                        "open_price": stmt.excluded.open_price,
                        "high_price": stmt.excluded.high_price,
                        "low_price": stmt.excluded.low_price,
                        "close_price": stmt.excluded.close_price,
                        "volume": stmt.excluded.volume,
                    },
                )
                await self.db_session.execute(stmt)
                self.increment_inserted()

            except Exception as e:
                self.log_error(f"Failed to insert price: {e}")

        await self.db_session.commit()


async def download_bhavcopy(trade_date: date) -> Optional[pd.DataFrame]:
    """
    Download and parse NSE bhavcopy for a specific date.

    Standalone function for quick access.
    """
    scraper = PriceScraper()
    return await scraper._fetch_bhavcopy(trade_date)


async def fetch_symbol_prices(
    symbol: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch historical prices for a symbol.

    Standalone function for quick access.
    """
    scraper = PriceScraper()
    return await scraper._fetch_historical_prices(symbol, from_date, to_date)
