"""
Base scraper class with common functionality.

All scrapers inherit from this class to get:
- Rate limiting
- Retry logic
- Logging
- Error handling
- Scrape log recording
"""

import time
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from config.logging_config import get_logger
from db.models import ScrapeLog

logger = get_logger(__name__)


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""

    # Override in subclasses
    SCRAPER_NAME: str = "base_scraper"

    def __init__(self, session: Optional[AsyncSession] = None):
        """
        Initialize the scraper.

        Args:
            session: Optional database session for logging
        """
        self.db_session = session
        self.records_scraped = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.start_time: Optional[datetime] = None
        self.errors: list[str] = []

    async def run(self, **kwargs) -> dict[str, Any]:
        """
        Execute the scraper with logging.

        Args:
            **kwargs: Scraper-specific arguments

        Returns:
            Dict with scrape results and statistics
        """
        self.start_time = datetime.now()
        self.records_scraped = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.errors = []

        logger.info(f"Starting {self.SCRAPER_NAME}")

        try:
            result = await self._scrape(**kwargs)
            status = "success" if not self.errors else "partial_success"
        except Exception as e:
            logger.error(f"{self.SCRAPER_NAME} failed: {e}")
            self.errors.append(str(e))
            result = None
            status = "failed"

        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Log to database
        await self._log_scrape(status, duration)

        logger.info(
            f"{self.SCRAPER_NAME} completed: "
            f"status={status}, scraped={self.records_scraped}, "
            f"inserted={self.records_inserted}, updated={self.records_updated}, "
            f"duration={duration:.2f}s"
        )

        return {
            "status": status,
            "records_scraped": self.records_scraped,
            "records_inserted": self.records_inserted,
            "records_updated": self.records_updated,
            "duration_seconds": duration,
            "errors": self.errors,
            "result": result,
        }

    @abstractmethod
    async def _scrape(self, **kwargs) -> Any:
        """
        Implement the actual scraping logic.

        Override this method in subclasses.

        Returns:
            Scraper-specific result data
        """
        pass

    async def _log_scrape(
        self,
        status: str,
        duration: float,
        company_id: Optional[int] = None,
    ) -> None:
        """Log the scrape operation to the database."""
        if self.db_session is None:
            return

        try:
            log_entry = ScrapeLog(
                scraper_name=self.SCRAPER_NAME,
                company_id=company_id,
                status=status,
                records_scraped=self.records_scraped,
                records_inserted=self.records_inserted,
                records_updated=self.records_updated,
                error_message="; ".join(self.errors) if self.errors else None,
                duration_seconds=Decimal(str(round(duration, 2))),
                started_at=self.start_time,
            )
            self.db_session.add(log_entry)
            await self.db_session.commit()
        except Exception as e:
            logger.warning(f"Failed to log scrape: {e}")

    def log_error(self, error: str) -> None:
        """Record an error during scraping."""
        self.errors.append(error)
        logger.error(f"{self.SCRAPER_NAME}: {error}")

    def increment_scraped(self, count: int = 1) -> None:
        """Increment the scraped records counter."""
        self.records_scraped += count

    def increment_inserted(self, count: int = 1) -> None:
        """Increment the inserted records counter."""
        self.records_inserted += count

    def increment_updated(self, count: int = 1) -> None:
        """Increment the updated records counter."""
        self.records_updated += count
