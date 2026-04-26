"""Health server for the LLM worker image — CUDA, vLLM readiness, model probe."""

from __future__ import annotations

import os

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

VLLM_BASE_URL = os.environ.get("VLLM_BASE_URL", "http://localhost:8000")
LLM_MODEL_NAME = os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen3-14B-AWQ")


@app.get("/health")
def health() -> JSONResponse:
    """Report CUDA availability and vLLM model readiness for lane `llm` health checks.

    Criteria:
    - cuda: torch.cuda.is_available()
    - vllm_ready: vLLM /health returns 200
    - model_usable: vLLM /v1/chat/completions responds to a 1-token probe

    Returns 200 when all three criteria pass, 503 otherwise.
    """
    import torch

    cuda_available = bool(torch.cuda.is_available())

    vllm_ready = False
    try:
        resp = httpx.get(f"{VLLM_BASE_URL}/health", timeout=5.0)
        vllm_ready = resp.status_code == 200
    except Exception:  # noqa: BLE001
        vllm_ready = False

    model_usable = False
    if vllm_ready:
        try:
            resp = httpx.post(
                f"{VLLM_BASE_URL}/v1/chat/completions",
                json={
                    "model": LLM_MODEL_NAME,
                    "messages": [{"role": "user", "content": "hi"}],
                    "max_tokens": 1,
                    "temperature": 0.0,
                },
                timeout=15.0,
            )
            model_usable = resp.status_code == 200
        except Exception:  # noqa: BLE001
            model_usable = False

    healthy = cuda_available and vllm_ready and model_usable
    return JSONResponse(
        {
            "status": "healthy" if healthy else "unhealthy",
            "cuda": cuda_available,
            "vllm_ready": vllm_ready,
            "model_usable": model_usable,
            "worker_mode": "llm",
        },
        status_code=200 if healthy else 503,
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
