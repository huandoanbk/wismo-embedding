"""Thin whisper runtime helpers for callback and storage interactions."""

from __future__ import annotations

from datetime import datetime
import gzip
import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import boto3
import httpx

from gpu_worker.whisper_runtime.config import WhisperRuntimeSettings

ALERT_RETRY_ATTEMPTS = 2
logger = logging.getLogger(__name__)


class WhisperRuntime:
    """Thin runtime facade for one whisper worker process."""

    def __init__(self, settings: WhisperRuntimeSettings) -> None:
        self.settings = settings

    def headers(self) -> dict[str, str]:
        """Return internal auth headers for callback requests."""
        return {"X-Internal-Secret": self.settings.internal_secret}

    def s3_client(self) -> Any:
        """Return an S3-compatible client for raw and extract artifacts."""
        return boto3.client(
            "s3",
            endpoint_url=self.settings.s3_endpoint_url,
            aws_access_key_id=self.settings.s3_access_key,
            aws_secret_access_key=self.settings.s3_secret_key,
            region_name=self.settings.s3_region,
        )

    def post(self, path: str, payload: dict[str, Any]) -> httpx.Response:
        """Call the embedding service using the stable internal callback contract."""
        return httpx.post(
            f"{self.settings.callback_url}{path}",
            json=payload,
            headers=self.headers(),
            timeout=self.settings.request_timeout_seconds,
        )

    def send_alert(
        self,
        *,
        severity: str,
        event_type: str,
        source: str,
        task_id: str | None = None,
        source_item_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Send an operational alert without masking the main task failure."""
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
                response = self.post("/api/internal/alert/", payload)
                response.raise_for_status()
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning("worker alert callback failed: %s", exc)
        if last_error is not None:
            logger.error("worker alert callback exhausted retries: %s", last_error)

    def resolve_source_item(self, source_item_id: str) -> dict[str, Any]:
        """Resolve the raw media location for one source item via the internal API."""
        response = self.post("/api/internal/get-source-item/", {"source_item_id": source_item_id})
        response.raise_for_status()
        return response.json()

    def upload_transcript(self, source_id: str, transcript: dict[str, Any]) -> str:
        """Upload the canonical transcript artifact and return its extract key."""
        extract_key = f"{source_id}.json.gz"
        self.s3_client().put_object(
            Bucket=self.settings.s3_bucket_extracts,
            Key=extract_key,
            Body=gzip.compress(json.dumps(transcript, ensure_ascii=False).encode("utf-8")),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        return extract_key

    def notify_transcript_ready(self, source_id: str, extract_key: str) -> None:
        """Notify the embedding service that the transcript artifact is ready."""
        callback = self.post(
            "/api/internal/transcript-ready/",
            {"source_id": source_id, "extract_key": extract_key},
        )
        callback.raise_for_status()

    def notify_transcript_status(self, source_id: str, status: str, error_message: str | None = None) -> None:
        """Notify the embedding service about one terminal transcript job state."""
        callback = self.post(
            "/api/internal/transcript-status/",
            {"source_id": source_id, "status": status, "error_message": error_message},
        )
        callback.raise_for_status()

    def download_raw_media(self, raw_bucket: str, raw_key: str) -> Path:
        """Mirror the raw media object into a temporary local file for Whisper."""
        with NamedTemporaryFile(suffix=Path(raw_key).suffix or ".media", delete=False) as handle:
            local_path = Path(handle.name)
        self.s3_client().download_file(raw_bucket, raw_key, str(local_path))
        return local_path
