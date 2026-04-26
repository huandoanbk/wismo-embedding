"""Connector for direct downloadable file URLs."""

from __future__ import annotations

from io import BytesIO
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx

from .base import SourceCandidate


class UnsupportedDynamicPageError(ValueError):
    """Raised when a URL resolves to HTML instead of a direct downloadable file."""


class DownloadableConnector:
    """Discover downloadable files from direct file URLs only."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self.client = client or httpx.Client(follow_redirects=True, timeout=30.0)

    def normalize_identity(self, raw_input: str | dict[str, Any]) -> dict[str, Any]:
        """Normalize a direct downloadable URL into a canonical identity."""
        url = raw_input if isinstance(raw_input, str) else str(raw_input["url"])
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Expected an http(s) downloadable URL")
        normalized = urlunparse(parsed._replace(fragment=""))
        return {
            "external_item_id": normalized,
            "normalized_source_url": normalized,
        }

    def discover_items(self, url: str) -> list[SourceCandidate]:
        """Discover a file from a direct downloadable URL."""
        normalized = self.normalize_identity(url)["normalized_source_url"]
        response = self.client.get(normalized)
        response.raise_for_status()
        if not self._is_direct_file(normalized, response):
            raise UnsupportedDynamicPageError("downloadable_direct_file_required")
        return [self._candidate_for_file(normalized, response.headers)]

    def _is_direct_file(self, url: str, response: httpx.Response) -> bool:
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            return True
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in (".pdf", ".epub", ".docx", ".txt", ".pptx", ".mp3", ".mp4", ".wav", ".webm"))

    def _candidate_for_file(
        self,
        url: str,
        headers: httpx.Headers,
    ) -> SourceCandidate:
        path = urlparse(url).path
        name = path.rsplit("/", 1)[-1] or url
        content_length = headers.get("content-length")
        return SourceCandidate(
            source_type="downloadable_file",
            external_item_id=url,
            normalized_source_url=url,
            display_name=name,
            source_path=path,
            mime_type=headers.get("content-type"),
            size_bytes=int(content_length) if content_length and content_length.isdigit() else None,
            metadata_json={},
        )

    def fetch_metadata(self, candidate: SourceCandidate) -> dict[str, Any]:
        """Return metadata for a downloadable candidate from an HTTP HEAD request."""
        if not candidate.normalized_source_url:
            raise ValueError("Downloadable candidate is missing a URL")
        response = self.client.head(candidate.normalized_source_url)
        if response.status_code >= 400:
            response = self.client.get(candidate.normalized_source_url)
        response.raise_for_status()
        return {
            "content_type": response.headers.get("content-type"),
            "content_length": response.headers.get("content-length"),
            "final_url": str(response.url),
        }

    def open_stream(self, candidate: SourceCandidate) -> BytesIO:
        """Download the candidate content and return it as a stream."""
        if not candidate.normalized_source_url:
            raise ValueError("Downloadable candidate is missing a URL")
        response = self.client.get(candidate.normalized_source_url)
        response.raise_for_status()
        return BytesIO(response.content)
