"""
Configuration settings for the Indian Stock Screener application.
All settings are loaded from environment variables with sensible defaults.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Database
    database_url: str = "postgresql+asyncpg://screener:password@localhost:5432/indian_screener"
    database_url_sync: str = "postgresql://screener:password@localhost:5432/indian_screener"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Scraping rate limits (seconds between requests)
    nse_rate_limit: float = 0.5
    bse_rate_limit: float = 0.3
    max_concurrent_scrapers: int = 3
    scrape_timeout: int = 30
    max_retries: int = 3

    # Quality check settings
    quality_sample_size: int = 100
    quality_threshold_default: float = 2.0

    # API settings
    api_rate_limit: int = 100  # requests per minute per IP
    api_title: str = "Indian Stock Screener API"
    api_version: str = "1.0.0"

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
