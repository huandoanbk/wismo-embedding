"""Minimal environment-backed settings for the thin whisper runtime."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class WhisperRuntimeSettings:
    """Thin runtime settings loaded directly from environment variables."""

    broker_url: str
    callback_url: str
    internal_secret: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket_raw: str
    s3_bucket_extracts: str
    request_timeout_seconds: float
    worker_mode: str


def load_settings() -> WhisperRuntimeSettings:
    """Return the minimal worker settings required for the whisper lane."""
    return WhisperRuntimeSettings(
        broker_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        callback_url=os.environ.get("VPS_CALLBACK_URL", "http://127.0.0.1:8000"),
        internal_secret=os.environ.get("INTERNAL_SHARED_SECRET", "changeme"),
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL", ""),
        s3_access_key=os.environ.get("S3_ACCESS_KEY", ""),
        s3_secret_key=os.environ.get("S3_SECRET_KEY", ""),
        s3_region=os.environ.get("S3_REGION", "eu-central-1"),
        s3_bucket_raw=os.environ.get("S3_BUCKET_RAW", "wismo-raw-files"),
        s3_bucket_extracts=os.environ.get("S3_BUCKET_EXTRACTS", "wismo-text-extracts"),
        request_timeout_seconds=float(os.environ.get("GPU_TASK_REQUEST_TIMEOUT_SECONDS", "120")),
        worker_mode=os.environ.get("WORKER_MODE", "whisper"),
    )
