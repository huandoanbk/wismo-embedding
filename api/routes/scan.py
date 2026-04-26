"""Scan orchestration routes for connector-backed source discovery."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from connectors import ContaboConnector, DownloadableConnector
from core.content_profiles import CONTENT_PROFILE_KEY, with_declared_content_profile
from core.database import get_db
from core.ingestion_models import IngestionSourceConfig
from core.ingestion_queue_service import IngestionQueueService
from core.ingestion_registry import IngestionRegistryService

router = APIRouter(prefix="/scan", tags=["scan"])


class ContaboPrefixScanRequest(BaseModel):
    """Request body for scanning a Contabo bucket/prefix."""

    bucket: str
    prefix: str = Field(default="")
    content_profile: str


def _persist_scan_results(candidates, db: Session, *, content_profile: str | None = None) -> dict[str, Any]:
    """Upsert discovered candidates and enqueue what needs processing."""
    registry = IngestionRegistryService(db)
    queue_service = IngestionQueueService(db)

    results: list[dict[str, Any]] = []
    for candidate in candidates:
        if content_profile:
            candidate.metadata_json = with_declared_content_profile(candidate.metadata_json, content_profile)
        decision = registry.upsert_candidate(candidate)
        queue_decision = queue_service.enqueue_if_needed(decision.item)
        results.append(
            {
                "item_id": str(decision.item.id),
                "discovery_status": decision.item.discovery_status,
                "processing_status": decision.item.processing_status,
                "skip_reason": decision.skip_reason or queue_decision.skip_reason,
                "queued": queue_decision.queued,
            }
        )
    return {"items_found": len(candidates), "results": results}


@router.post("/contabo-prefix")
async def scan_contabo_prefix(body: ContaboPrefixScanRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Discover candidates, upsert registry items, and enqueue what needs processing."""
    connector = ContaboConnector()
    candidates = connector.discover_items(bucket=body.bucket, prefix=body.prefix)
    payload = _persist_scan_results(candidates, db, content_profile=body.content_profile)
    db.commit()
    return payload


@router.post("/source-config/{config_id}")
async def scan_source_config(config_id: UUID, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Run discovery for a saved source config when its source type supports scanning."""
    source_config = db.get(IngestionSourceConfig, config_id)
    if source_config is None:
        raise HTTPException(status_code=404, detail="source_config_not_found")

    if not source_config.is_enabled:
        raise HTTPException(status_code=409, detail="source_config_disabled")

    config_json = dict(source_config.config_json)
    if source_config.source_type == "contabo_prefix":
        bucket = config_json.get("bucket")
        if not bucket:
            raise HTTPException(status_code=400, detail="source_config_bucket_required")
        prefix = config_json.get("prefix", "")
        candidates = ContaboConnector().discover_items(bucket=bucket, prefix=prefix)
    elif source_config.source_type == "downloadable_file":
        url = config_json.get("url")
        if not url:
            raise HTTPException(status_code=400, detail="source_config_url_required")
        candidates = DownloadableConnector().discover_items(url=url)
    else:
        raise HTTPException(status_code=400, detail="source_config_scan_not_supported")

    payload = _persist_scan_results(candidates, db, content_profile=config_json.get(CONTENT_PROFILE_KEY))
    source_config.last_scan_at = datetime.utcnow()
    source_config.last_scan_status = "success"
    payload["source_config_id"] = str(source_config.id)
    db.commit()
    return payload
