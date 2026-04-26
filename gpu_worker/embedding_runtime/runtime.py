"""Thin embedding runtime helpers for callback interactions."""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

import httpx

from gpu_worker.embedding_runtime.config import EmbeddingRuntimeSettings

ALERT_RETRY_ATTEMPTS = 2
logger = logging.getLogger(__name__)


class EmbeddingRuntime:
    """Thin runtime facade for one embedding worker process."""

    def __init__(self, settings: EmbeddingRuntimeSettings) -> None:
        self.settings = settings

    def headers(self) -> dict[str, str]:
        """Return internal auth headers for callback requests."""
        return {"X-Internal-Secret": self.settings.internal_secret}

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
        details: dict[str, Any] | None = None,
    ) -> None:
        """Send an operational alert without masking the main task failure."""
        payload: dict[str, Any] = {
            "severity": severity,
            "event_type": event_type,
            "source": source,
            "occurred_at": datetime.utcnow().isoformat(),
            "task_id": task_id,
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

    def get_chunks(self, chunk_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch chunk payloads via the stable internal read contract."""
        response = self.post("/api/internal/get-chunks/", {"chunk_ids": chunk_ids})
        response.raise_for_status()
        return response.json()["chunks"]

    def store_embeddings(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Store computed embeddings through the internal write contract."""
        response = self.post("/api/internal/store-embeddings/", {"items": items})
        response.raise_for_status()
        return response.json()

    def update_batch_status(self, batch_id: str, status: str, error_message: str | None = None) -> None:
        """Update DB-backed embedding batch lifecycle through the internal API."""
        payload: dict[str, Any] = {"batch_id": batch_id, "status": status}
        if error_message is not None:
            payload["error_message"] = error_message
        response = self.post("/api/internal/embedding-batch-status/", payload)
        response.raise_for_status()
