"""Minimal health server for GPU workers."""

from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/health")
def health() -> JSONResponse:
    """Report process and CUDA availability for manager health checks."""
    import torch

    cuda_available = bool(torch.cuda.is_available())
    status_code = 200 if cuda_available else 503
    payload = {
        "status": "healthy" if cuda_available else "unhealthy",
        "cuda": cuda_available,
        "worker_mode": os.environ.get("WORKER_MODE", "both"),
    }
    return JSONResponse(payload, status_code=status_code)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
