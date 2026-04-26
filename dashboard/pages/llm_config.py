"""LLM Provider Config page — refinement and enrichment are configured independently."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from core.llm_lane import LLM_OPERATION_ENRICHMENT, LLM_OPERATION_REFINEMENT
from services.llm_api_client import (
    LlmApiClient,
    list_llm_operation_configs,
    resolve_shared_vastai_max_in_flight_requests,
    save_llm_operation_config,
)


def _render_operation_section(st_module: Any, *, operation: str, config: Any) -> dict[str, Any]:
    title = "Boundary Refinement" if operation == LLM_OPERATION_REFINEMENT else "Enrichment"
    provider_options = ["vastai", "api"]
    current_provider = config.provider if config else "vastai"
    current_base_url = config.api_base_url if config else "https://api.deepinfra.com/v1/openai"
    current_api_key = config.api_key if config else ""
    current_model = config.api_model if config else "Qwen/Qwen3-14B"
    key_prefix = f"llm_{operation}"

    st_module.subheader(title)
    provider = st_module.radio(
        f"Provider ({title})",
        options=provider_options,
        index=0 if current_provider == "vastai" else 1,
        format_func=lambda x: "Vast.ai (self-hosted vLLM)" if x == "vastai" else "API (OpenAI-compatible)",
        horizontal=True,
        key=f"{key_prefix}_provider",
    )

    if provider == "api":
        api_base_url = st_module.text_input(
            f"Base URL ({title})",
            value=current_base_url,
            key=f"{key_prefix}_base_url",
        )
        api_key = st_module.text_input(
            f"API Key ({title})",
            value=current_api_key,
            type="password",
            key=f"{key_prefix}_api_key",
        )
        api_model = st_module.text_input(
            f"Model ({title})",
            value=current_model,
            key=f"{key_prefix}_model",
        )
        col1, _ = st_module.columns([1, 3])
        with col1:
            if st_module.button(f"Test {title}", key=f"{key_prefix}_test"):
                try:
                    test_client = LlmApiClient(base_url=api_base_url, api_key=api_key, model=api_model)
                    result = test_client.complete("Reply with exactly: OK", temperature=0.0)
                    preview = result[0] if isinstance(result, tuple) else result
                    st_module.success(f"{title}: connection successful. Response: {preview[:100]}")
                except Exception as exc:  # noqa: BLE001
                    st_module.error(f"{title}: connection failed: {exc}")
    else:
        api_base_url = current_base_url
        api_key = current_api_key
        api_model = current_model

    st_module.divider()
    return {
        "operation": operation,
        "provider": provider,
        "api_base_url": api_base_url,
        "api_key": api_key,
        "api_model": api_model,
    }


def render_llm_config_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render operation-scoped LLM runtime configuration."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("LLM Config")
    st_module.caption(
        "Configure provider independently for boundary refinement and enrichment. "
        "When either operation uses Vast.ai, both share one LLM instance pool and one in-flight limit."
    )

    db = session_factory()
    should_close = session_factory is SessionLocal
    try:
        configs = list_llm_operation_configs(db)
        shared_in_flight = resolve_shared_vastai_max_in_flight_requests(db)

        st_module.subheader("Shared Vast.ai Runtime")
        vastai_max_in_flight_requests = st_module.number_input(
            "Max in-flight requests (shared LLM instance pool)",
            min_value=1,
            max_value=32,
            value=int(shared_in_flight or 1),
            step=1,
            key="llm_shared_in_flight",
        )
        st_module.caption(
            "This limit applies to the shared Vast.ai LLM worker that consumes both refinement and enrichment queues."
        )
        st_module.divider()

        refinement_payload = _render_operation_section(
            st_module,
            operation=LLM_OPERATION_REFINEMENT,
            config=configs[LLM_OPERATION_REFINEMENT],
        )
        enrichment_payload = _render_operation_section(
            st_module,
            operation=LLM_OPERATION_ENRICHMENT,
            config=configs[LLM_OPERATION_ENRICHMENT],
        )

        if st_module.button("Copy Refinement -> Enrichment"):
            save_llm_operation_config(db, **{
                "operation": LLM_OPERATION_ENRICHMENT,
                "provider": refinement_payload["provider"],
                "api_base_url": refinement_payload["api_base_url"],
                "api_key": refinement_payload["api_key"],
                "api_model": refinement_payload["api_model"],
                "vastai_max_in_flight_requests": int(vastai_max_in_flight_requests),
            })
            db.commit()
            st_module.success("Copied refinement config to enrichment.")
            st_module.rerun()

        if st_module.button("Save configuration", type="primary"):
            for payload in (refinement_payload, enrichment_payload):
                if payload["provider"] == "api" and (
                    not payload["api_key"] or not payload["api_base_url"] or not payload["api_model"]
                ):
                    st_module.error(f"Please fill API settings for {payload['operation']}.")
                    break
            else:
                for payload in (refinement_payload, enrichment_payload):
                    save_llm_operation_config(
                        db,
                        **payload,
                        vastai_max_in_flight_requests=int(vastai_max_in_flight_requests),
                    )
                db.commit()
                st_module.success("Saved LLM config.")
                st_module.rerun()
    finally:
        if should_close:
            db.close()
