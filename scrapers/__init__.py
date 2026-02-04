"""Scrapers module for NSE/BSE data collection."""

from scrapers.base import BaseScraper
from scrapers.nse_company_master import NSECompanyMasterScraper
from scrapers.bse_company_master import BSECompanyMasterScraper
from scrapers.nse_xbrl import NSEFinancialScraper
from scrapers.bse_xbrl import BSEFinancialScraper
from scrapers.price_scraper import PriceScraper

__all__ = [
    "BaseScraper",
    "NSECompanyMasterScraper",
    "BSECompanyMasterScraper",
    "NSEFinancialScraper",
    "BSEFinancialScraper",
    "PriceScraper",
]
