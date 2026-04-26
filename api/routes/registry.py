"""Registry mutation routes for ingestion orchestration."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from core.database import get_db
from core.ingestion_models import IngestionSourceItem
from services.dashboard_ops import reprocess_source_item

router = APIRouter(prefix="/registry", tags=["registry"])


@router.post("/reprocess/{item_id}")
async def reprocess_registry_item(item_id: UUID, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Reset a registry item to initial state and re-enqueue for full reprocessing.

    Deletes all derived data (queue rows, embedding batches, chunks, embeddings,
    source document) before re-enqueueing the item from scratch.
    """
    item = db.get(IngestionSourceItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="registry_item_not_found")

    result = reprocess_source_item(db, item_id)
    db.commit()
    return result
