"""Minimal environment-backed settings for the thin LLM runtime."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class LlmRuntimeSettings:
    """Thin runtime settings loaded directly from environment variables."""

    broker_url: str
    callback_url: str
    internal_secret: str
    vllm_base_url: str
    model_name: str
    llm_request_timeout_seconds: float
    request_timeout_seconds: float
    worker_mode: str
    llm_mode: str


def load_settings() -> LlmRuntimeSettings:
    """Return the minimal worker settings required for the LLM lane."""
    return LlmRuntimeSettings(
        broker_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        callback_url=os.environ.get("VPS_CALLBACK_URL", "http://127.0.0.1:8000"),
        internal_secret=os.environ.get("INTERNAL_SHARED_SECRET", "changeme"),
        vllm_base_url=os.environ.get("VLLM_BASE_URL", "http://localhost:8000"),
        model_name=os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen3-14B-AWQ"),
        llm_request_timeout_seconds=float(
            os.environ.get("LLM_REQUEST_TIMEOUT_SECONDS", "120")
        ),
        request_timeout_seconds=float(
            os.environ.get("GPU_TASK_REQUEST_TIMEOUT_SECONDS", "60")
        ),
        worker_mode=os.environ.get("WORKER_MODE", "llm"),
        llm_mode=os.environ.get("LLM_RUNTIME_MODE", "strict"),
    )
