"""Canonical content-profile helpers for source-aware chunking setup."""

from __future__ import annotations

from typing import Any

CONTENT_PROFILE_KEY = "content_profile"
CONTENT_PROFILE_DECLARED_KEY = "declared_content_profile"

CONTENT_PROFILE_SUTRA = "sutra"
CONTENT_PROFILE_BOOK = "book"
CONTENT_PROFILE_TRANSCRIPT = "transcript"
CONTENT_PROFILE_AUDIOBOOK = "audiobook"

VALID_CONTENT_PROFILES = frozenset(
    {
        CONTENT_PROFILE_SUTRA,
        CONTENT_PROFILE_BOOK,
        CONTENT_PROFILE_TRANSCRIPT,
        CONTENT_PROFILE_AUDIOBOOK,
    }
)


def normalize_content_profile(value: str | None) -> str:
    """Return one validated canonical content profile."""
    normalized = (value or "").strip().lower()
    if normalized not in VALID_CONTENT_PROFILES:
        raise ValueError(f"invalid_content_profile:{value}")
    return normalized


def merge_content_profile_metadata(
    incoming_metadata: dict[str, Any],
    *,
    existing_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge source-declared metadata into one canonical registry payload."""
    metadata = dict(incoming_metadata)
    current = dict(existing_metadata or {})

    if CONTENT_PROFILE_DECLARED_KEY in current and CONTENT_PROFILE_DECLARED_KEY not in metadata:
        metadata[CONTENT_PROFILE_DECLARED_KEY] = current[CONTENT_PROFILE_DECLARED_KEY]

    declared = metadata.get(CONTENT_PROFILE_DECLARED_KEY)
    canonical = declared or metadata.get(CONTENT_PROFILE_KEY)

    if declared is not None:
        metadata[CONTENT_PROFILE_DECLARED_KEY] = normalize_content_profile(str(declared))
    if canonical is not None:
        metadata[CONTENT_PROFILE_KEY] = normalize_content_profile(str(canonical))

    return metadata


def declared_content_profile_from_metadata(metadata: dict[str, Any]) -> str | None:
    """Return the declared or corrected profile from metadata when present."""
    value = metadata.get(CONTENT_PROFILE_DECLARED_KEY) or metadata.get(CONTENT_PROFILE_KEY)
    if value is None:
        return None
    return normalize_content_profile(str(value))


def require_content_profile(metadata: dict[str, Any]) -> str:
    """Return one canonical content profile or raise a contract error."""
    value = metadata.get(CONTENT_PROFILE_KEY)
    if value is None:
        raise ValueError("content_profile_required")
    return normalize_content_profile(str(value))


def with_declared_content_profile(metadata: dict[str, Any], content_profile: str) -> dict[str, Any]:
    """Attach one source-declared content profile to metadata."""
    payload = dict(metadata)
    payload[CONTENT_PROFILE_DECLARED_KEY] = normalize_content_profile(content_profile)
    return merge_content_profile_metadata(payload)
