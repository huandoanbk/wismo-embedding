"""Upstash/Redis task queue inspection and management helpers."""

from __future__ import annotations

import base64
import json
from typing import Any
from uuid import UUID

import redis as redis_lib
from sqlalchemy.orm import Session

from core.config import settings
from core.ingestion_models import EmbeddingBatch, IngestionSourceItem

MANAGED_QUEUES = ("whisper", "embedding", "llm_refinement", "llm_enrichment")


def get_broker_url() -> tuple[str, bool]:
    """Return (url, is_upstash) for the Vast.ai broker.

    is_upstash is False when vastai_redis_url is not configured and we fall
    back to the local Redis — caller can warn the user in that case.
    """
    if settings.vastai_redis_url:
        return settings.vastai_redis_url, True
    return settings.redis_url, False


def _connect(redis_url: str | None = None) -> redis_lib.Redis:
    """Return a Redis client connected to the Vast.ai broker (Upstash)."""
    url = redis_url or get_broker_url()[0]
    return redis_lib.from_url(url, decode_responses=False, socket_connect_timeout=5, socket_timeout=10)


def _decode_celery_message(raw: bytes) -> dict[str, Any]:
    """Decode one raw Celery Redis list entry into a structured dict.

    Celery protocol v2 stores messages as JSON with a base64-encoded body.
    Body structure after decode: [args, kwargs, embed_info].

    Returns a dict always — sets decode_error on failure so callers can surface it.
    """
    raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    try:
        msg = json.loads(raw)
        body = json.loads(base64.b64decode(msg["body"]))
        args = body[0] if isinstance(body, list) and body else []
        headers = msg.get("headers") or {}
        return {
            "task_id": headers.get("id", ""),
            "task_name": headers.get("task", ""),
            "args": args,
            "raw": raw_str,
            "decode_error": None,
        }
    except Exception as exc:
        return {
            "task_id": "",
            "task_name": "",
            "args": [],
            "raw": raw_str[:200],  # truncate for display
            "decode_error": str(exc),
        }


def _to_uuid(value: str | None) -> UUID | None:
    """Parse a string to UUID, returning None on failure."""
    if not value:
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _resolve_display_info(
    db: Session,
    queue: str,
    args: list[Any],
) -> tuple[str | None, str | None, str | None, str | None]:
    """Return (source_item_id, batch_id, display_name, source_path) for one task."""
    source_item_id: str | None = None
    batch_id: str | None = None
    display_name: str | None = None
    source_path: str | None = None

    if queue in ("whisper", "llm", "llm_refinement", "llm_enrichment"):
        source_item_id = str(args[0]) if args else None
        item = db.get(IngestionSourceItem, _to_uuid(source_item_id))
        if item:
            display_name = item.display_name
            source_path = item.source_path

    elif queue == "embedding":
        batch_id = str(args[0]) if args else None
        batch = db.get(EmbeddingBatch, _to_uuid(batch_id))
        if batch and batch.source_item_id:
            item = db.get(IngestionSourceItem, batch.source_item_id)
            if item:
                display_name = item.display_name
                source_path = item.source_path

    return source_item_id, batch_id, display_name, source_path


def get_queue_stats(db: Session, *, redis_client: redis_lib.Redis | None = None) -> list[dict[str, Any]]:
    """Return pending count and enriched task list for each managed queue.

    Each queue entry contains:
    - queue: queue name
    - pending: number of tasks waiting
    - tasks: list of task dicts with display_name resolved from DB
    """
    r = redis_client or _connect()
    result = []
    for queue_name in MANAGED_QUEUES:
        raw_messages = r.lrange(queue_name, 0, -1)
        tasks: list[dict[str, Any]] = []
        for raw in raw_messages:
            msg = _decode_celery_message(raw)
            if msg["decode_error"] is not None:
                tasks.append({
                    "task_id": "",
                    "task_name": "(decode error)",
                    "queue": queue_name,
                    "source_item_id": None,
                    "batch_id": None,
                    "display_name": "(decode error)",
                    "source_path": None,
                    "raw": msg["raw"],
                    "decode_error": msg["decode_error"],
                })
                continue
            source_item_id, batch_id, display_name, source_path = _resolve_display_info(
                db, queue_name, msg["args"]
            )
            short_name = msg["task_name"].rsplit(".", 1)[-1] if msg["task_name"] else ""
            tasks.append(
                {
                    "task_id": msg["task_id"],
                    "task_name": short_name,
                    "queue": queue_name,
                    "source_item_id": source_item_id,
                    "batch_id": batch_id,
                    "display_name": display_name or "(unknown)",
                    "source_path": source_path,
                    "raw": msg["raw"],
                    "decode_error": None,
                }
            )
        result.append(
            {
                "queue": queue_name,
                "pending": len(raw_messages),
                "tasks": tasks,
            }
        )
    return result


def purge_queue(queue_name: str, *, redis_client: redis_lib.Redis | None = None) -> int:
    """Delete all tasks from one queue. Returns number of tasks removed.

    Raises ValueError for unknown queue names.
    """
    if queue_name not in MANAGED_QUEUES:
        raise ValueError(f"unknown_queue:{queue_name}")
    r = redis_client or _connect()
    count = r.llen(queue_name)
    r.delete(queue_name)
    return count


def remove_task(
    queue_name: str,
    raw_message: str,
    *,
    redis_client: redis_lib.Redis | None = None,
) -> bool:
    """Remove one specific task from the queue by matching its raw message bytes.

    Uses LREM to remove the first occurrence. Returns True if the task was found
    and removed; False if it was no longer in the queue (already picked up by worker).
    """
    if queue_name not in MANAGED_QUEUES:
        raise ValueError(f"unknown_queue:{queue_name}")
    r = redis_client or _connect()
    payload = raw_message.encode("utf-8") if isinstance(raw_message, str) else raw_message
    removed = r.lrem(queue_name, 1, payload)
    return removed > 0
