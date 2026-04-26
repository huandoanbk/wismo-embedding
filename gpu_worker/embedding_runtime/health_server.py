"""Thin health server for the embedding worker image."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from gpu_worker.embedding_runtime.config import load_settings

app = FastAPI()


@app.get("/health")
def health() -> JSONResponse:
    """Report process and CUDA availability for embedding lane health checks."""
    import torch

    settings = load_settings()
    cuda_available = bool(torch.cuda.is_available())
    status_code = 200 if cuda_available else 503
    payload = {
        "status": "healthy" if cuda_available else "unhealthy",
        "cuda": cuda_available,
        "worker_mode": settings.worker_mode,
    }
    return JSONResponse(payload, status_code=status_code)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
