"""Shared connector primitives for source discovery and intake."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel, Field


class SourceCandidate(BaseModel):
    """Normalized source item passed from a connector into the next pipeline step."""

    source_type: str
    external_item_id: str | None = None
    normalized_source_url: str | None = None
    display_name: str
    source_path: str | None = None
    mime_type: str | None = None
    size_bytes: int | None = None
    last_modified: datetime | None = None
    raw_bucket: str | None = None
    raw_key: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)

    def to_summary(self) -> dict[str, Any]:
        """Return a JSON-safe candidate summary for API responses."""
        payload = self.model_dump(mode="json", exclude={"metadata_json"})
        metadata = dict(self.metadata_json)
        metadata.pop("upload_file", None)
        payload["metadata_json"] = metadata
        return payload


class SourceConnector(Protocol):
    """Contract that every source connector must implement."""

    def normalize_identity(self, raw_input: str | dict[str, Any] | Any) -> dict[str, Any]:
        """Normalize raw connector input into identity fields."""

    def discover_items(self, *args: Any, **kwargs: Any) -> list[SourceCandidate]:
        """Discover candidate items from a source."""

    def fetch_metadata(self, candidate: SourceCandidate) -> dict[str, Any]:
        """Fetch enriched metadata for a candidate."""

    def open_stream(self, candidate: SourceCandidate) -> Any:
        """Open a stream or stream-like handle for a candidate."""
