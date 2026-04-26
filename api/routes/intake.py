"""Baseline intake routes for connector-driven source discovery."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Form, UploadFile
from pydantic import BaseModel, Field

from connectors import ContaboConnector, DownloadableConnector, ManualUploadConnector

router = APIRouter(prefix="/intake", tags=["intake"])


class DownloadableIntakeRequest(BaseModel):
    """Request body for downloadable source intake."""

    url: str


class ContaboScanRequest(BaseModel):
    """Request body for scanning a Contabo prefix."""

    bucket: str
    prefix: str = Field(default="")


def _summary(connector: str, candidates: list[Any]) -> dict[str, Any]:
    return {
        "connector": connector,
        "items_found": len(candidates),
        "candidates": [candidate.to_summary() for candidate in candidates],
    }


@router.post("/upload")
async def intake_upload(
    file: UploadFile = File(...),
    raw_bucket: str | None = Form(default=None),
) -> dict[str, Any]:
    """Normalize a manual upload into a source candidate summary."""
    connector = ManualUploadConnector()
    candidates = connector.discover_items(upload=file, raw_bucket=raw_bucket)
    return _summary("manual_upload", candidates)


@router.post("/downloadable")
async def intake_downloadable(body: DownloadableIntakeRequest) -> dict[str, Any]:
    """Normalize a downloadable URL into source candidate summaries."""
    connector = DownloadableConnector()
    candidates = connector.discover_items(body.url)
    return _summary("downloadable", candidates)


@router.post("/contabo/scan")
async def intake_contabo_scan(body: ContaboScanRequest) -> dict[str, Any]:
    """Scan a Contabo bucket/prefix and return candidate summaries."""
    connector = ContaboConnector()
    candidates = connector.discover_items(bucket=body.bucket, prefix=body.prefix)
    return _summary("contabo_prefix", candidates)
