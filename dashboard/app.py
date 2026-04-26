"""Streamlit dashboard entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from sqlalchemy.exc import ProgrammingError

# Ensure package imports resolve when Streamlit executes this file as a script.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.pages.overview import render_overview_page  # noqa: E402
from dashboard.pages.queue import render_queue_page  # noqa: E402
from dashboard.pages.source_configs import render_source_configs_page  # noqa: E402
from dashboard.pages.sources import render_sources_page  # noqa: E402
from dashboard.pages.llm_config import render_llm_config_page  # noqa: E402
from dashboard.pages.automation import render_automation_page  # noqa: E402
from dashboard.pages.pipeline_options import render_pipeline_options_page  # noqa: E402
from dashboard.pages.upstash_queue import render_upstash_queue_page  # noqa: E402
from dashboard.pages.vastai import render_vastai_page  # noqa: E402
from dashboard.pages.vastai_lane_config import render_vastai_lane_config_page  # noqa: E402
from dashboard.pages.worker_images import render_worker_images_page  # noqa: E402
from dashboard.worker_alerts import render_worker_alerts_page  # noqa: E402


def main(st_module: Any | None = None) -> None:
    """Render the embedding service dashboard."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.set_page_config(page_title="WISMO Embedding Dashboard", layout="wide")
    navigation = st_module.navigation(
        [
            st_module.Page(_wrap_page_renderer(render_overview_page), title="Overview", url_path="overview", default=True),
            st_module.Page(_wrap_page_renderer(render_sources_page), title="Sources", url_path="sources"),
            st_module.Page(_wrap_page_renderer(render_source_configs_page), title="Source Configs", url_path="source-configs"),
            st_module.Page(_wrap_page_renderer(render_queue_page), title="Queue", url_path="queue"),
            st_module.Page(_wrap_page_renderer(render_upstash_queue_page), title="Task Queue (Upstash)", url_path="upstash-queue"),
            st_module.Page(_wrap_page_renderer(render_llm_config_page), title="LLM Config", url_path="llm-config"),
            st_module.Page(_wrap_page_renderer(render_automation_page), title="Automation", url_path="automation"),
            st_module.Page(_wrap_page_renderer(render_pipeline_options_page), title="Pipeline Options", url_path="pipeline-options"),
            st_module.Page(_wrap_page_renderer(render_vastai_page), title="Vast.ai", url_path="vastai"),
            st_module.Page(_wrap_page_renderer(render_vastai_lane_config_page), title="Vast.ai Lane Config", url_path="vastai-lane-config"),
            st_module.Page(_wrap_page_renderer(render_worker_images_page), title="Worker Images", url_path="worker-images"),
            st_module.Page(_wrap_page_renderer(render_worker_alerts_page), title="Worker Alerts", url_path="worker-alerts"),
        ],
        position="sidebar",
    )
    navigation.run()


def _wrap_page_renderer(renderer):
    """Wrap a page renderer so dashboard schema errors become user-facing messages."""

    def run() -> None:
        import streamlit as st  # type: ignore[import-not-found]

        try:
            renderer(st)
        except ProgrammingError as exc:
            if _is_missing_relation_error(exc):
                st.error(
                    "Database schema for the embedding dashboard is missing one or more tables. "
                    "Verify `VECTOR_DB_URL` points to the expected database and run `./.venv/bin/alembic upgrade head` "
                    "against a database that has not been incorrectly stamped."
                )
                st.code(str(exc))
                return
            raise

    return run


def _is_missing_relation_error(exc: ProgrammingError) -> bool:
    """Return True when the database reports an undefined table or column."""
    message = str(exc).lower()
    return (
        "undefinedtable" in message
        or "undefinedcolumn" in message
        or 'relation "' in message
        and "does not exist" in message
        or 'column "' in message
        and "does not exist" in message
    )


if __name__ == "__main__":
    main()
