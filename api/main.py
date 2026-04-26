"""FastAPI application entrypoint for the embedding service."""

from __future__ import annotations

from fastapi import FastAPI

from api.routes import intake, registry, scan
from api.routes.internal import router as internal_router


def create_app() -> FastAPI:
    """Create the embedding service app with all mounted routers."""
    app = FastAPI(title="WISMO Embedding Service")
    app.include_router(intake.router)
    app.include_router(scan.router)
    app.include_router(registry.router)
    app.include_router(internal_router)
    return app


app = create_app()
