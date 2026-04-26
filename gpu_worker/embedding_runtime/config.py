"""Minimal environment-backed settings for the thin embedding runtime."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class EmbeddingRuntimeSettings:
    """Thin runtime settings loaded directly from environment variables."""

    broker_url: str
    callback_url: str
    internal_secret: str
    request_timeout_seconds: float
    worker_mode: str


def load_settings() -> EmbeddingRuntimeSettings:
    """Return the minimal worker settings required for the embedding lane."""
    return EmbeddingRuntimeSettings(
        broker_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        callback_url=os.environ.get("VPS_CALLBACK_URL", "http://127.0.0.1:8000"),
        internal_secret=os.environ.get("INTERNAL_SHARED_SECRET", "changeme"),
        request_timeout_seconds=float(os.environ.get("GPU_TASK_REQUEST_TIMEOUT_SECONDS", "120")),
        worker_mode=os.environ.get("WORKER_MODE", "embedding"),
    )
