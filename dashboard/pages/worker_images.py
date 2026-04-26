"""Worker Images page for GPU worker image registry operations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.dashboard_ops import (
    activate_worker_image_record,
    create_worker_image_record,
    list_worker_image_rows,
    validate_worker_image_record,
)
from services.worker_images import WorkerImageError


def render_worker_images_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Worker Images page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.title("Worker Images")

        st_module.subheader("Registered Images")
        rows = list_worker_image_rows(session)
        st_module.dataframe(rows, width="stretch")

        st_module.subheader("Create Worker Image")
        with st_module.form("worker_image_create_form"):
            lane = st_module.selectbox(
                "Lane",
                ["whisper", "embedding", "llm"],
                index=0,
            )
            image_ref = st_module.text_input("Image Ref")
            display_name = st_module.text_input("Display Name")
            notes = st_module.text_area("Notes")
            create_submitted = st_module.form_submit_button("Create Worker Image")

        if create_submitted:
            record = create_worker_image_record(
                session,
                lane=lane,
                image_ref=image_ref,
                display_name=display_name or None,
                notes=notes or None,
            )
            session.commit()
            st_module.success(f"Created worker image {record.id}")
            st_module.rerun()

        st_module.subheader("Validate / Activate")
        image_options = {
            f"{row['lane']} | {row['display_name'] or row['image_ref']} [{row['id']}]": row["id"]
            for row in rows
        }
        selected_label = st_module.selectbox("Worker Image", [""] + list(image_options.keys()), index=0)
        selected_id = image_options.get(selected_label)
        action = st_module.selectbox("Action", ["validate", "activate"], index=0)
        if st_module.button("Apply Worker Image Action") and selected_id:
            if action == "validate":
                record = validate_worker_image_record(session, selected_id)
                session.commit()
                if record.validation_status == "valid":
                    st_module.success(f"Validated worker image {record.id}")
                else:
                    st_module.error(record.validation_error or "Validation failed")
            else:
                record = activate_worker_image_record(session, selected_id)
                session.commit()
                st_module.success(f"Activated worker image {record.id}")
            st_module.rerun()
    except WorkerImageError as exc:
        session.rollback()
        st_module.error(str(exc))
    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()
