"""Thin Celery task module for the embedding worker lane."""

from __future__ import annotations

import ssl as _ssl

from celery import Celery
import httpx

from gpu_worker.embedding_runtime.config import load_settings
from gpu_worker.embedding_runtime.runtime import EmbeddingRuntime

settings = load_settings()
app = Celery("wismo_embedding_gpu", broker=settings.broker_url)
app.conf.task_serializer = "json"
app.conf.result_backend = None
if settings.broker_url.startswith("rediss://"):
    app.conf.broker_use_ssl = {"ssl_cert_reqs": _ssl.CERT_NONE}
EMBED_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def _runtime() -> EmbeddingRuntime:
    """Return a runtime facade built from environment variables."""
    return EmbeddingRuntime(settings)


def _retry_countdown(retries: int) -> int:
    """Return a bounded exponential backoff in seconds for embedding retries."""
    return min(60 * (2**retries), 300)


@app.task(
    name="gpu_worker.tasks.embed_chunks_batch",
    queue="embedding",
    bind=True,
    max_retries=2,
    time_limit=1800,
)
def embed_chunks_batch(self, batch_id: str, chunk_ids: list[str]) -> dict[str, int]:
    """Fetch chunk contents from VPS, encode with BGE-M3, and callback vectors."""
    task_id = getattr(self.request, "id", None)
    runtime = _runtime()
    try:
        chunks = runtime.get_chunks(chunk_ids)
        if not chunks:
            runtime.update_batch_status(batch_id, "skipped", error_message="no_chunks_found")
            runtime.send_alert(
                severity="warning",
                event_type="embed_chunks_batch_skipped",
                source="embedding_worker",
                task_id=task_id,
                details={"message": "no_chunks_found", "chunk_ids": chunk_ids, "batch_id": batch_id},
            )
            return {"stored": 0, "errors": 0, "status": "skipped_no_chunks"}

        from FlagEmbedding import BGEM3FlagModel

        model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=True)
        embeddings = model.encode(
            [chunk["content"] for chunk in chunks],
            batch_size=32,
        )["dense_vecs"]
        payload = [
            {"chunk_id": chunks[index]["id"], "embedding": embeddings[index].tolist()}
            for index in range(len(chunks))
        ]
        result = runtime.store_embeddings(payload)
        runtime.update_batch_status(batch_id, "completed")
        return result
    except httpx.HTTPStatusError as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in EMBED_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="embed_chunks_batch_retrying",
                source="embedding_worker",
                task_id=task_id,
                details={
                    "message": str(exc),
                    "chunk_ids": chunk_ids,
                    "batch_id": batch_id,
                    "status_code": exc.response.status_code,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        try:
            runtime.update_batch_status(batch_id, "skipped", error_message=str(exc))
        except Exception:  # noqa: BLE001
            pass
        runtime.send_alert(
            severity="warning",
            event_type="embed_chunks_batch_skipped",
            source="embedding_worker",
            task_id=task_id,
            details={"message": str(exc), "chunk_ids": chunk_ids, "batch_id": batch_id},
        )
        return {"stored": 0, "errors": 1, "status": "skipped_after_error"}
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="embed_chunks_batch_retrying",
                source="embedding_worker",
                task_id=task_id,
                details={
                    "message": str(exc),
                    "chunk_ids": chunk_ids,
                    "batch_id": batch_id,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        try:
            runtime.update_batch_status(batch_id, "skipped", error_message=str(exc))
        except Exception:  # noqa: BLE001
            pass
        runtime.send_alert(
            severity="warning",
            event_type="embed_chunks_batch_skipped",
            source="embedding_worker",
            task_id=task_id,
            details={"message": str(exc), "chunk_ids": chunk_ids, "batch_id": batch_id},
        )
        return {"stored": 0, "errors": 1, "status": "skipped_after_retries_exhausted"}
