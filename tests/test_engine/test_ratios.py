"""Tests for ratio computation engine."""

import pytest
from engine.ratios import compute_ratios, FinancialData


class TestRatioComputation:
    """Tests for the ratio computation engine."""

    def test_pe_ratio_calculation(self):
        """Test PE ratio calculation."""
        data = FinancialData(
            current_price=100.0,
            eps_basic=10.0,
            shares_outstanding=1000000,
        )
        ratios = compute_ratios(data)
        assert ratios["pe_ratio"] == 10.0

    def test_pe_ratio_zero_eps(self):
        """Test PE ratio with zero EPS returns None."""
        data = FinancialData(
            current_price=100.0,
            eps_basic=0.0,
            shares_outstanding=1000000,
        )
        ratios = compute_ratios(data)
        assert ratios["pe_ratio"] is None

    def test_debt_equity_ratio(self):
        """Test debt to equity ratio calculation."""
        data = FinancialData(
            total_borrowings=5000000,
            total_equity=10000000,
        )
        ratios = compute_ratios(data)
        assert ratios["debt_equity"] == 0.5

    def test_roe_calculation(self):
        """Test ROE calculation."""
        data = FinancialData(
            net_profit=1000000,
            total_equity=5000000,
        )
        ratios = compute_ratios(data)
        assert ratios["roe"] == 20.0  # 1M / 5M * 100 = 20%

    def test_operating_margin(self):
        """Test operating margin calculation."""
        data = FinancialData(
            revenue=10000000,
            operating_profit=2000000,
        )
        ratios = compute_ratios(data)
        assert ratios["operating_margin"] == 20.0

    def test_current_ratio(self):
        """Test current ratio calculation."""
        data = FinancialData(
            total_current_assets=5000000,
            total_current_liabilities=2500000,
        )
        ratios = compute_ratios(data)
        assert ratios["current_ratio"] == 2.0

    def test_market_cap(self):
        """Test market cap calculation."""
        data = FinancialData(
            current_price=100.0,
            shares_outstanding=1000000,
        )
        ratios = compute_ratios(data)
        assert ratios["market_cap"] == 100000000.0

    def test_book_value_per_share(self):
        """Test book value per share calculation."""
        data = FinancialData(
            total_equity=10000000,
            shares_outstanding=1000000,
        )
        ratios = compute_ratios(data)
        assert ratios["book_value_per_share"] == 10.0

    def test_pb_ratio(self):
        """Test PB ratio calculation."""
        data = FinancialData(
            current_price=20.0,
            total_equity=10000000,
            shares_outstanding=1000000,
        )
        ratios = compute_ratios(data)
        assert ratios["pb_ratio"] == 2.0  # Price 20 / BVPS 10 = 2

    def test_financial_data_from_dict(self):
        """Test creating FinancialData from dictionary."""
        data_dict = {
            "revenue": 10000000,
            "net_profit": 1000000,
            "total_equity": 5000000,
            "total_borrowings": 2000000,
        }
        data = FinancialData.from_dict(data_dict)
        assert data.revenue == 10000000
        assert data.net_profit == 1000000
        assert data.total_equity == 5000000
        assert data.total_borrowings == 2000000
