"""GPU-side Celery tasks for Whisper transcription and batch embedding."""

from __future__ import annotations

from datetime import datetime
import gzip
import json
import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import ssl as _ssl

import httpx
import boto3
from celery import Celery

BROKER_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
CALLBACK_URL = os.environ.get("VPS_CALLBACK_URL", "http://127.0.0.1:8000")
INTERNAL_SECRET = os.environ.get("INTERNAL_SHARED_SECRET", "changeme")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION = os.environ.get("S3_REGION", "eu-central-1")
S3_BUCKET_RAW = os.environ.get("S3_BUCKET_RAW", "wismo-raw-files")
S3_BUCKET_EXTRACTS = os.environ.get("S3_BUCKET_EXTRACTS", "wismo-text-extracts")
REQUEST_TIMEOUT = float(os.environ.get("GPU_TASK_REQUEST_TIMEOUT_SECONDS", "120"))
ALERT_RETRY_ATTEMPTS = 2
logger = logging.getLogger(__name__)

app = Celery("wismo_gpu", broker=BROKER_URL)
app.conf.task_serializer = "json"
app.conf.result_backend = None
if BROKER_URL.startswith("rediss://"):
    app.conf.broker_use_ssl = {"ssl_cert_reqs": _ssl.CERT_NONE}
TRANSCRIBE_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
TRANSCRIBE_STALE_STATUS_CODES = {404, 409}
EMBED_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def _headers() -> dict[str, str]:
    return {"X-Internal-Secret": INTERNAL_SECRET}


def _s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def _send_alert(
    *,
    severity: str,
    event_type: str,
    source: str,
    task_id: str | None = None,
    source_item_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Send an operational alert without failing the main worker task."""
    payload: dict[str, Any] = {
        "severity": severity,
        "event_type": event_type,
        "source": source,
        "occurred_at": datetime.utcnow().isoformat(),
        "task_id": task_id,
        "source_item_id": source_item_id,
        "details": details or {},
    }
    last_error: Exception | None = None
    for _ in range(ALERT_RETRY_ATTEMPTS):
        try:
            response = httpx.post(
                f"{CALLBACK_URL}/api/internal/alert/",
                json=payload,
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning("worker alert callback failed: %s", exc)
    if last_error is not None:
        logger.error("worker alert callback exhausted retries: %s", last_error)


def _retry_countdown(retries: int) -> int:
    """Return a bounded exponential backoff in seconds for transcript retries."""
    return min(60 * (2**retries), 300)


@app.task(queue="whisper", bind=True, max_retries=2, time_limit=3600)
def transcribe_audio(self, source_item_id: str) -> dict[str, str]:
    """Transcribe a mirrored media asset and callback the canonical extract key."""
    task_id = getattr(self.request, "id", None)
    try:
        response = httpx.post(
            f"{CALLBACK_URL}/api/internal/get-source-item/",
            json={"source_item_id": source_item_id},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        raw_bucket = payload.get("raw_bucket") or S3_BUCKET_RAW
        raw_key = payload["raw_key"]
        source_id = payload["source_id"]
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in TRANSCRIBE_STALE_STATUS_CODES:
            _send_alert(
                severity="warning",
                event_type="source_item_lookup_skipped",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "status": "skipped_stale_source_item",
                "status_code": str(exc.response.status_code),
            }
        if exc.response.status_code in TRANSCRIBE_RETRYABLE_STATUS_CODES:
            retry_count = int(getattr(self.request, "retries", 0))
            if retry_count < int(getattr(self, "max_retries", 0)):
                _send_alert(
                    severity="warning",
                    event_type="transcribe_audio_retrying",
                    source="whisper_worker",
                    task_id=task_id,
                    source_item_id=source_item_id,
                    details={
                        "message": str(exc),
                        "status_code": exc.response.status_code,
                        "retry_count": retry_count + 1,
                    },
                )
                raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        _send_alert(
            severity="error",
            event_type="source_item_lookup_failed",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        raise
    except Exception as exc:
        _send_alert(
            severity="error",
            event_type="source_item_lookup_failed",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        raise

    local_path: Path | None = None
    s3 = _s3_client()
    try:
        with NamedTemporaryFile(suffix=Path(raw_key).suffix or ".media", delete=False) as handle:
            local_path = Path(handle.name)
        s3.download_file(raw_bucket, raw_key, str(local_path))

        from faster_whisper import WhisperModel

        model = WhisperModel("large-v3", device="cuda", compute_type="float16")
        segments, info = model.transcribe(str(local_path), beam_size=5)
        segment_list = list(segments)
        transcript = {
            "source_id": source_id,
            "source_type": payload["source_type"],
            "extracted_at": payload.get("requested_at"),
            "extractor_version": "whisper-large-v3",
            "language": info.language,
            "language_probability": getattr(info, "language_probability", None),
            "duration_sec": info.duration,
            "full_text": " ".join(segment.text.strip() for segment in segment_list).strip(),
            "segments": [
                {"start": segment.start, "end": segment.end, "text": segment.text}
                for segment in segment_list
            ],
        }
        extract_key = f"{source_id}.json.gz"
        s3.put_object(
            Bucket=S3_BUCKET_EXTRACTS,
            Key=extract_key,
            Body=gzip.compress(json.dumps(transcript, ensure_ascii=False).encode("utf-8")),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        callback = httpx.post(
            f"{CALLBACK_URL}/api/internal/transcript-ready/",
            json={"source_id": source_id, "extract_key": extract_key},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        callback.raise_for_status()
        return {"source_id": source_id, "extract_key": extract_key}
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            _send_alert(
                severity="warning",
                event_type="transcribe_audio_retrying",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "raw_key": raw_key,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        _send_alert(
            severity="warning",
            event_type="transcribe_audio_skipped_after_retries",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={
                "message": str(exc),
                "raw_key": raw_key,
                "retry_count": retry_count,
            },
        )
        try:
            httpx.post(
                f"{CALLBACK_URL}/api/internal/transcript-status/",
                json={"source_id": source_id, "status": "failed", "error_message": str(exc)},
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
        except Exception as status_exc:  # noqa: BLE001
            _send_alert(
                severity="error",
                event_type="transcript_failure_callback_failed",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(status_exc), "raw_key": raw_key},
            )
        return {
            "source_item_id": source_item_id,
            "status": "skipped_after_retries_exhausted",
            "raw_key": raw_key,
        }
    finally:
        if local_path is not None:
            local_path.unlink(missing_ok=True)


@app.task(queue="embedding", bind=True, max_retries=2, time_limit=1800)
def embed_chunks_batch(self, batch_id: str, chunk_ids: list[str]) -> dict[str, int]:
    """Fetch chunk contents from VPS, encode with BGE-M3, and callback vectors."""
    task_id = getattr(self.request, "id", None)
    try:
        chunks_resp = httpx.post(
            f"{CALLBACK_URL}/api/internal/get-chunks/",
            json={"chunk_ids": chunk_ids},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        chunks_resp.raise_for_status()
        chunks = chunks_resp.json()["chunks"]
        if not chunks:
            httpx.post(
                f"{CALLBACK_URL}/api/internal/embedding-batch-status/",
                json={"batch_id": batch_id, "status": "skipped", "error_message": "no_chunks_found"},
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
            _send_alert(
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
        store_resp = httpx.post(
            f"{CALLBACK_URL}/api/internal/store-embeddings/",
            json={
                "items": [
                    {"chunk_id": chunks[index]["id"], "embedding": embeddings[index].tolist()}
                    for index in range(len(chunks))
                ]
            },
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        store_resp.raise_for_status()
        status_resp = httpx.post(
            f"{CALLBACK_URL}/api/internal/embedding-batch-status/",
            json={"batch_id": batch_id, "status": "completed"},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        status_resp.raise_for_status()
        return store_resp.json()
    except httpx.HTTPStatusError as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in EMBED_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            _send_alert(
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
            httpx.post(
                f"{CALLBACK_URL}/api/internal/embedding-batch-status/",
                json={"batch_id": batch_id, "status": "skipped", "error_message": str(exc)},
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
        except Exception:  # noqa: BLE001
            pass
        _send_alert(
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
            _send_alert(
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
            httpx.post(
                f"{CALLBACK_URL}/api/internal/embedding-batch-status/",
                json={"batch_id": batch_id, "status": "skipped", "error_message": str(exc)},
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
        except Exception:  # noqa: BLE001
            pass
        _send_alert(
            severity="warning",
            event_type="embed_chunks_batch_skipped",
            source="embedding_worker",
            task_id=task_id,
            details={"message": str(exc), "chunk_ids": chunk_ids, "batch_id": batch_id},
        )
        return {"stored": 0, "errors": 1, "status": "skipped_after_retries_exhausted"}
