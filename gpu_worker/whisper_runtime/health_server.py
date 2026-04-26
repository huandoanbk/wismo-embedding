"""Thin health server for the whisper worker image."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from gpu_worker.whisper_runtime.config import load_settings

app = FastAPI()


@app.get("/health")
def health() -> JSONResponse:
    """Report process and CUDA availability for whisper lane health checks."""
    import ctranslate2

    settings = load_settings()
    try:
        cuda_available = ctranslate2.get_cuda_device_count() > 0
    except Exception:  # noqa: BLE001
        cuda_available = False
    status_code = 200 if cuda_available else 503
    payload = {
        "status": "healthy" if cuda_available else "unhealthy",
        "cuda": cuda_available,
        "worker_mode": settings.worker_mode,
    }
    return JSONResponse(payload, status_code=status_code)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
