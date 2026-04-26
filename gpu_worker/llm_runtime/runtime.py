"""Thin HTTP client for the LLM worker lane — VPS callbacks and vLLM inference."""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

import httpx

from gpu_worker.llm_runtime.config import LlmRuntimeSettings

ALERT_RETRY_ATTEMPTS = 2
logger = logging.getLogger(__name__)


class LlmRuntime:
    """Thin runtime facade for one LLM worker process.

    Handles:
    - Fetching refinement/enrichment items from VPS internal API.
    - Calling local vLLM OpenAI-compatible endpoint for inference.
    - Posting callback results back to VPS.
    - Sending operational alerts.

    Side effects:
        All methods make HTTP calls. No DB access — VPS owns persistence.
    """

    def __init__(self, settings: LlmRuntimeSettings) -> None:
        self.settings = settings

    def headers(self) -> dict[str, str]:
        """Return internal auth headers for VPS callback requests."""
        return {"X-Internal-Secret": self.settings.internal_secret}

    def post_vps(self, path: str, payload: dict[str, Any]) -> httpx.Response:
        """POST to VPS internal API using the shared secret contract."""
        return httpx.post(
            f"{self.settings.callback_url}{path}",
            json=payload,
            headers=self.headers(),
            timeout=self.settings.request_timeout_seconds,
        )

    def get_vps(self, path: str) -> httpx.Response:
        """GET from VPS internal API using the shared secret contract."""
        return httpx.get(
            f"{self.settings.callback_url}{path}",
            headers=self.headers(),
            timeout=self.settings.request_timeout_seconds,
        )

    def call_vllm(self, prompt: str, *, temperature: float = 0.0) -> str:
        """Call local vLLM OpenAI-compatible chat completions and return text.

        Inputs:
            prompt: full prompt string to send as user message.
            temperature: sampling temperature (0.0 for deterministic decisions).

        Output:
            Text string from choices[0].message.content.

        Raises:
            httpx.HTTPError: on non-2xx response.
            KeyError: if response shape is unexpected.
        """
        response = httpx.post(
            f"{self.settings.vllm_base_url}/v1/chat/completions",
            json={
                "model": self.settings.model_name,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "max_tokens": 512,
            },
            timeout=self.settings.llm_request_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def get_refinement_items(self, source_item_id: str) -> dict[str, Any]:
        """Fetch candidate chunks for boundary refinement from VPS.

        Output:
            dict with candidate_chunks list, content_profile, transcript_quality_tier.
        """
        response = self.post_vps(
            "/api/internal/get-refinement-items/",
            {"source_item_id": source_item_id},
        )
        response.raise_for_status()
        return response.json()

    def get_enrichment_items(self, source_item_id: str) -> dict[str, Any]:
        """Fetch chunks and source metadata for enrichment from VPS.

        Output:
            dict with chunks list, content_profile, source_metadata.
        """
        response = self.post_vps(
            "/api/internal/get-enrichment-items/",
            {"source_item_id": source_item_id},
        )
        response.raise_for_status()
        return response.json()

    def get_runtime_config(self, *, operation: str) -> dict[str, Any]:
        """Fetch live LLM worker tuning config from VPS."""
        response = self.get_vps(f"/api/internal/llm-runtime-config/?operation={operation}")
        response.raise_for_status()
        return response.json()

    def post_refinement_callback(self, payload: dict[str, Any]) -> dict[str, Any]:
        """POST boundary refinement results to VPS callback endpoint."""
        response = self.post_vps("/api/internal/llm-refinement-callback/", payload)
        response.raise_for_status()
        return response.json()

    def post_enrichment_callback(self, payload: dict[str, Any]) -> dict[str, Any]:
        """POST enrichment results to VPS callback endpoint."""
        response = self.post_vps("/api/internal/llm-enrichment-callback/", payload)
        response.raise_for_status()
        return response.json()

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
                response = self.post_vps("/api/internal/alert/", payload)
                response.raise_for_status()
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning("llm worker alert callback failed: %s", exc)
        if last_error is not None:
            logger.error("llm worker alert callback exhausted retries: %s", last_error)
