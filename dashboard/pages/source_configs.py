"""Source config page for connector discovery operations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dashboard.pages.add_sources import (
    render_content_profile_field,
    render_manual_upload_intake_fields,
    render_source_config_intake_fields,
)
from core.database import SessionLocal
from services.dashboard_ops import (
    create_source_config,
    list_source_config_rows,
    register_manual_upload_source,
    set_source_config_enabled,
    trigger_source_config_scan,
    update_source_config,
)


def render_source_configs_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Source Configs page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.title("Source Configs")
        st_module.caption(
            "`Source Type` is the connector/how data is ingested; `Content Profile` is the content semantics used to select the chunking pipeline lane."
        )
        st_module.caption("Autoscan uses the shared `Automation -> Scan Source Configs` cron. Source configs no longer carry their own schedule.")
        if "show_create_source_dialog" not in st_module.session_state:
            st_module.session_state["show_create_source_dialog"] = False
        if "manual_upload_in_progress" not in st_module.session_state:
            st_module.session_state["manual_upload_in_progress"] = False
        if st_module.button("Create Source", key="open_create_source_dialog"):
            st_module.session_state["show_create_source_dialog"] = True

        if st_module.session_state["show_create_source_dialog"]:
            _render_create_source_config_dialog(st_module, session)

        st_module.subheader("Existing Source Configs")
        rows = list_source_config_rows(session)
        st_module.dataframe(rows, width="stretch")

        config_options = {
            f"{row['display_name'] or row['source_type']} ({row['source_type']}) [{row['id']}]": row["id"]
            for row in rows
        }
        selected_config_label = st_module.selectbox("Source Config", [""] + list(config_options.keys()), index=0)
        selected_config_id = config_options.get(selected_config_label)
        selected_config = next((row for row in rows if row["id"] == selected_config_id), None)
        action = st_module.selectbox("Config Action", ["enable", "disable", "scan"], index=0)
        if st_module.button("Apply Config Action") and selected_config_id:
            if action == "enable":
                set_source_config_enabled(session, selected_config_id, True)
                session.commit()
                st_module.success(f"Applied {action} for {selected_config_id}")
            elif action == "disable":
                set_source_config_enabled(session, selected_config_id, False)
                session.commit()
                st_module.success(f"Applied {action} for {selected_config_id}")
            else:
                result = trigger_source_config_scan(session, selected_config_id)
                if result.get("status") == "locked":
                    st_module.warning("Scan stage is already running from another trigger.")
                elif result.get("status") == "failed":
                    st_module.error(f"Scan error: {result.get('error', 'unknown_error')}")
                else:
                    st_module.success(f"Applied {action} for {selected_config_id}")

        if selected_config is not None:
            st_module.subheader("Edit Selected Source Config")
            with st_module.form("edit_source_config_form"):
                source_type, display_name, config_json = render_source_config_intake_fields(
                    st_module,
                    key_prefix="edit_source_config",
                    initial_source_type=selected_config["source_type"],
                    initial_display_name=selected_config["display_name"] or "",
                    initial_config_json=selected_config["config_json"],
                    allow_source_type_edit=False,
                )
                content_profile = render_content_profile_field(
                    st_module,
                    key_prefix="edit_source_config",
                    initial_value=selected_config["content_profile"] or "",
                )
                enabled = st_module.selectbox(
                    "Enabled",
                    options=[True, False],
                    index=0 if selected_config["is_enabled"] else 1,
                    format_func=lambda value: "Yes" if value else "No",
                    key="edit_source_config_enabled",
                )
                submitted_edit = st_module.form_submit_button("Save Source Config")

            if submitted_edit:
                if not content_profile:
                    st_module.error("Content Profile is required to select the correct pipeline chunking lane.")
                else:
                    update_source_config(
                        session,
                        selected_config_id,
                        display_name=display_name or source_type,
                        config_json=config_json,
                        content_profile=content_profile,
                        is_enabled=bool(enabled),
                    )
                    session.commit()
                    st_module.success(f"Updated source config {selected_config_id}")
                    st_module.rerun()

        st_module.subheader("Manual Upload")
        with st_module.form("manual_upload_form"):
            raw_bucket, upload = render_manual_upload_intake_fields(st_module, key_prefix="manual_upload")
            content_profile = render_content_profile_field(st_module, key_prefix="manual_upload")
            submit_manual_upload = st_module.form_submit_button(
                "Register Manual Upload",
                disabled=st_module.session_state["manual_upload_in_progress"],
            )

        if submit_manual_upload and upload is not None:
            if not content_profile:
                st_module.error("Content Profile is required to select the correct pipeline chunking lane.")
                return
            st_module.session_state["manual_upload_in_progress"] = True
            try:
                with st_module.spinner("Uploading and registering manual source..."):
                    results = register_manual_upload_source(
                        session,
                        upload=upload,
                        raw_bucket=raw_bucket or None,
                        content_profile=content_profile,
                    )
            except ValueError as exc:
                session.rollback()
                st_module.session_state["manual_upload_in_progress"] = False
                if str(exc).startswith("raw_bucket_not_found:"):
                    bucket_name = str(exc).split(":", 1)[1]
                    st_module.error(f"Raw bucket `{bucket_name}` does not exist or is not accessible.")
                else:
                    raise
            except Exception:
                st_module.session_state["manual_upload_in_progress"] = False
                raise
            else:
                session.commit()
                st_module.session_state["manual_upload_in_progress"] = False
                st_module.success(f"Registered {len(results)} source item(s)")
                st_module.dataframe(results, width="stretch")
    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()


def _render_create_source_config_dialog(st_module: Any, session) -> None:
    """Render a modal dialog for creating one source config."""

    @st_module.dialog("Create Source Config")
    def show_dialog() -> None:
        with st_module.form("create_source_config_form"):
            source_type, display_name, config_json = render_source_config_intake_fields(
                st_module,
                key_prefix="source_config_dialog",
            )
            content_profile = render_content_profile_field(st_module, key_prefix="source_config_dialog")

            submitted = st_module.form_submit_button("Create Source Config")

        if not submitted:
            return
        if not content_profile:
            st_module.error("Content Profile is required to select the correct pipeline chunking lane.")
            return

        config = create_source_config(
            session,
            source_type=source_type,
            display_name=display_name or source_type,
            config_json=config_json,
            content_profile=content_profile,
        )
        session.commit()
        st_module.session_state["show_create_source_dialog"] = False
        st_module.success(f"Created source config {config.id}")
        st_module.rerun()

    show_dialog()
