"""Shared source-intake helpers and the standalone intake page."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.dashboard_ops import create_source_config, register_manual_upload_source

SOURCE_TYPE_OPTIONS = ["contabo_prefix", "downloadable_file"]
CONTENT_PROFILE_OPTIONS = ["", "book", "sutra", "transcript"]


def render_add_sources_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the standalone source intake page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.title("Source Intake")
        _render_intake_guidance(st_module)
        intake_mode = st_module.radio(
            "Choose Intake Flow",
            ["Create Source Config", "Register Manual Upload"],
            index=0,
        )
        if intake_mode == "Create Source Config":
            source_type, display_name, config_json = render_source_config_intake_fields(
                st_module,
                key_prefix="add_sources_config",
            )
            content_profile = render_content_profile_field(st_module, key_prefix="add_sources_config")
            if st_module.button("Create Source Config"):
                if not content_profile:
                    st_module.error("Content Profile is required to select the correct pipeline chunking lane.")
                else:
                    config = create_source_config(
                        session,
                        source_type=source_type,
                        display_name=display_name or source_type,
                        config_json=config_json,
                        content_profile=content_profile,
                    )
                    session.commit()
                    st_module.success(f"Created source config {config.id}")
        else:
            raw_bucket, upload = render_manual_upload_intake_fields(st_module, key_prefix="add_sources_manual")
            content_profile = render_content_profile_field(st_module, key_prefix="add_sources_manual")
            if st_module.button("Register Manual Upload"):
                if upload is None:
                    st_module.error("Please select a source file before registering a manual upload.")
                elif not content_profile:
                    st_module.error("Content Profile is required to select the correct pipeline chunking lane.")
                else:
                    results = register_manual_upload_source(
                        session,
                        upload=upload,
                        raw_bucket=raw_bucket or None,
                        content_profile=content_profile,
                    )
                    session.commit()
                    st_module.success(f"Registered {len(results)} source item(s)")
                    st_module.dataframe(results, width="stretch")
    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()


def _render_intake_guidance(st_module: Any) -> None:
    """Render one short explanation for connector vs chunking semantics."""
    st_module.caption(
        "Intake has two separate decisions: `Source Type` is the connector/how the system fetches data; "
        "`Content Profile` is the content semantics used to select the chunking pipeline lane."
    )


def render_source_type_field(st_module: Any, *, key_prefix: str) -> str:
    """Render the connector-selection field with business-facing guidance."""
    return st_module.selectbox(
        "Step 1. Source Type (connector)",
        SOURCE_TYPE_OPTIONS,
        index=0,
        key=f"{key_prefix}_source_type",
        format_func=_format_source_type,
        help="Select how the system fetches data: scan a bucket prefix or fetch a single file from a URL.",
    )


def render_content_profile_field(st_module: Any, *, key_prefix: str, initial_value: str = "") -> str:
    """Render the required chunking-profile field."""
    initial_index = CONTENT_PROFILE_OPTIONS.index(initial_value) if initial_value in CONTENT_PROFILE_OPTIONS else 0
    return st_module.selectbox(
        "Step 3. Content Profile (required)",
        CONTENT_PROFILE_OPTIONS,
        index=initial_index,
        key=f"{key_prefix}_content_profile",
        format_func=_format_content_profile,
        help=(
            "Select content semantics so the chunking pipeline runs the correct lane. "
            "Audiobooks use `book`; oral talks use `transcript`."
        ),
    )


def render_source_config_intake_fields(
    st_module: Any,
    *,
    key_prefix: str,
    initial_source_type: str | None = None,
    initial_display_name: str = "",
    initial_config_json: dict[str, str] | None = None,
    allow_source_type_edit: bool = True,
) -> tuple[str, str, dict[str, str]]:
    """Render the source-config intake fields and return raw values."""
    initial_config_json = initial_config_json or {}
    if allow_source_type_edit:
        source_type = render_source_type_field(st_module, key_prefix=key_prefix)
    else:
        source_type = initial_source_type or "contabo_prefix"
        st_module.text_input(
            "Step 1. Source Type (connector)",
            value=_format_source_type(source_type),
            key=f"{key_prefix}_source_type_readonly",
            disabled=True,
            help="Source Type of an existing source config is fixed; to change the connector, create a new source config.",
        )
    st_module.caption("Step 2. Connector Settings")
    display_name = st_module.text_input(
        "Display Name",
        value=initial_display_name,
        key=f"{key_prefix}_display_name",
        help="Human-readable name so operators can identify the source in the dashboard.",
    )
    if source_type == "contabo_prefix":
        bucket = st_module.text_input(
            "Bucket",
            value=initial_config_json.get("bucket", ""),
            key=f"{key_prefix}_bucket",
            help="S3-compatible bucket the connector will scan.",
        )
        prefix = st_module.text_input(
            "Prefix",
            value=initial_config_json.get("prefix", ""),
            key=f"{key_prefix}_prefix",
            help="Prefix/path inside the bucket to limit the scan scope.",
        )
        config_json = {"bucket": bucket, "prefix": prefix}
    else:
        url = st_module.text_input(
            "Download URL",
            value=initial_config_json.get("url", ""),
            key=f"{key_prefix}_url",
            help="Single file URL the connector will download for ingestion.",
        )
        config_json = {"url": url}
    return source_type, display_name, config_json


def render_manual_upload_intake_fields(st_module: Any, *, key_prefix: str) -> tuple[str, Any]:
    """Render the manual-upload intake fields and return raw values."""
    st_module.caption("Step 1. Upload Source File")
    raw_bucket = st_module.text_input(
        "Raw Bucket",
        value="raw-manual-upload",
        key=f"{key_prefix}_raw_bucket",
        help="Target raw bucket — must already exist on Contabo/S3-compatible storage.",
    )
    upload = st_module.file_uploader(
        "Upload File",
        key=f"{key_prefix}_upload_file",
        help="Select the source file to mirror into raw storage before pipeline processing.",
    )
    return raw_bucket, upload


def _format_source_type(value: str) -> str:
    """Return a business-facing label for one source type."""
    labels = {
        "contabo_prefix": "Contabo Prefix: scan a bucket/prefix",
        "downloadable_file": "Downloadable File: ingest a single file URL",
    }
    return labels.get(value, value)


def _format_content_profile(value: str) -> str:
    """Return a business-facing label for one content profile."""
    labels = {
        "": "Select content profile...",
        "book": "Book / Audiobook",
        "sutra": "Sutra / Scripture",
        "transcript": "Talk / Oral Transcript",
    }
    return labels.get(value, value)
