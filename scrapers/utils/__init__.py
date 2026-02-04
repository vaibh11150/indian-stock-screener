"""Scraper utility modules."""

from scrapers.utils.session_manager import NSESession, BSESession
from scrapers.utils.normalizer import normalize_field, PL_FIELD_MAP, BS_FIELD_MAP, CF_FIELD_MAP
from scrapers.utils.xbrl_parser import parse_xbrl_financial_result
from scrapers.utils.html_table_parser import parse_financial_html_table, parse_numeric, parse_period_from_header

__all__ = [
    "NSESession",
    "BSESession",
    "normalize_field",
    "PL_FIELD_MAP",
    "BS_FIELD_MAP",
    "CF_FIELD_MAP",
    "parse_xbrl_financial_result",
    "parse_financial_html_table",
    "parse_numeric",
    "parse_period_from_header",
]
