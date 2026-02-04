"""API routers."""

from api.routers.companies import router as companies_router
from api.routers.financials import router as financials_router
from api.routers.ratios import router as ratios_router
from api.routers.prices import router as prices_router
from api.routers.screener import router as screener_router
from api.routers.quality import router as quality_router

__all__ = [
    "companies_router",
    "financials_router",
    "ratios_router",
    "prices_router",
    "screener_router",
    "quality_router",
]
