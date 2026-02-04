"""
FastAPI main application.
"""

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers import (
    companies_router,
    financials_router,
    ratios_router,
    prices_router,
    screener_router,
    quality_router,
)
from api.schemas import ErrorResponse, HealthResponse
from config.settings import settings
from config.logging_config import setup_logging, get_logger
from db import init_db, close_db

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    setup_logging()
    logger.info("Starting Indian Stock Screener API")
    # await init_db()  # Uncomment if you want to auto-create tables
    yield
    # Shutdown
    logger.info("Shutting down Indian Stock Screener API")
    await close_db()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description=(
            "Financial data API sourced from NSE/BSE primary filings. "
            "Provides company financials, ratios, prices, and screening capabilities."
        ),
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(
        companies_router,
        prefix="/api/v1/companies",
        tags=["Companies"],
    )
    app.include_router(
        financials_router,
        prefix="/api/v1/financials",
        tags=["Financials"],
    )
    app.include_router(
        ratios_router,
        prefix="/api/v1/ratios",
        tags=["Ratios"],
    )
    app.include_router(
        prices_router,
        prefix="/api/v1/prices",
        tags=["Prices"],
    )
    app.include_router(
        screener_router,
        prefix="/api/v1/screen",
        tags=["Screener"],
    )
    app.include_router(
        quality_router,
        prefix="/api/v1/quality",
        tags=["Quality"],
    )

    # Health check endpoint
    @app.get("/health", response_model=HealthResponse, tags=["Health"])
    async def health_check():
        """Health check endpoint."""
        return HealthResponse(
            status="healthy",
            database="connected",
            version=settings.api_version,
        )

    # Root endpoint
    @app.get("/", tags=["Root"])
    async def root():
        """API root endpoint."""
        return {
            "name": settings.api_title,
            "version": settings.api_version,
            "docs": "/docs",
            "health": "/health",
            "timestamp": datetime.now().isoformat(),
        }

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal server error",
                detail=str(exc) if settings.log_level == "DEBUG" else None,
            ).model_dump(),
        )

    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
