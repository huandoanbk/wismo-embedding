"""Backend retrieval integration client for dashboard test-search flows."""

from __future__ import annotations

from typing import Any

import httpx

from core.config import settings


def get_test_search_status() -> dict[str, Any]:
    """Return whether backend retrieval integration is configured."""
    configured = bool(settings.backend_retrieval_base_url and settings.backend_retrieval_search_path)
    url = ""
    if configured:
        url = settings.backend_retrieval_base_url.rstrip("/") + settings.backend_retrieval_search_path
    return {"configured": configured, "url": url}


def run_test_search(
    query: str,
    *,
    top_k: int = 3,
    client: Any | None = None,
) -> dict[str, Any]:
    """Delegate a retrieval test query to the configured backend endpoint."""
    status = get_test_search_status()
    if not status["configured"]:
        return {"configured": False, "error": "not configured", "results": []}

    headers: dict[str, str] = {}
    if settings.backend_retrieval_auth_token:
        headers["Authorization"] = f"Bearer {settings.backend_retrieval_auth_token}"

    payload = {"query": query, "top_k": top_k}
    if client is None:
        response = httpx.post(
            status["url"],
            json=payload,
            headers=headers,
            timeout=settings.backend_retrieval_timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
    else:
        data = client.post(status["url"], json=payload, headers=headers, timeout=settings.backend_retrieval_timeout_seconds)
        if hasattr(data, "raise_for_status"):
            data.raise_for_status()
            data = data.json()

    results = data.get("results")
    if results is None and "chunks" in data:
        results = data["chunks"]
    if results is None and isinstance(data, list):
        results = data
    return {"configured": True, "results": results or [], "raw": data}
