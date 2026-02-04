"""Tests for field normalizer."""

import pytest
from scrapers.utils.normalizer import normalize_field, PL_FIELD_MAP, BS_FIELD_MAP


class TestNormalizer:
    """Tests for the field normalizer."""

    def test_normalize_revenue_variations(self):
        """Test normalizing different revenue field names."""
        variations = [
            "Revenue",
            "RevenueFromOperations",
            "Revenue From Operations",
            "revenue_from_operations",
            "Sales",
            "Net Sales",
        ]
        for variant in variations:
            assert normalize_field(variant, PL_FIELD_MAP) == "revenue", f"Failed for {variant}"

    def test_normalize_net_profit_variations(self):
        """Test normalizing different net profit field names."""
        variations = [
            "ProfitForThePeriod",
            "Net Profit",
            "NetProfit",
            "PAT",
            "ProfitAfterTax",
        ]
        for variant in variations:
            assert normalize_field(variant, PL_FIELD_MAP) == "net_profit", f"Failed for {variant}"

    def test_normalize_total_assets_variations(self):
        """Test normalizing different total assets field names."""
        variations = [
            "TotalAssets",
            "Total Assets",
            "total_assets",
        ]
        for variant in variations:
            assert normalize_field(variant, BS_FIELD_MAP) == "total_assets", f"Failed for {variant}"

    def test_normalize_unknown_field(self):
        """Test that unknown fields return None."""
        assert normalize_field("SomeRandomField", PL_FIELD_MAP) is None
        assert normalize_field("AnotherRandomField") is None

    def test_normalize_empty_string(self):
        """Test that empty string returns None."""
        assert normalize_field("", PL_FIELD_MAP) is None
        assert normalize_field("   ") is None

    def test_normalize_case_insensitive(self):
        """Test that normalization is case insensitive."""
        assert normalize_field("REVENUE", PL_FIELD_MAP) == "revenue"
        assert normalize_field("revenue", PL_FIELD_MAP) == "revenue"
        assert normalize_field("ReVeNuE", PL_FIELD_MAP) == "revenue"
