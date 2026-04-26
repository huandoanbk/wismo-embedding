"""CRUD helpers for per-lane Vast.ai search configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from core.config import settings
from core.models import VastaiLaneConfig

LANES = ("whisper", "embedding", "llm")
LEGACY_LLM_LANE = "llm"

# Defaults from settings — used when no DB row exists for a lane
_DEFAULTS: dict[str, dict[str, Any]] = {
    "whisper": {
        "search_filter": settings.vastai_search_filter,
        "disk_gb": 20,
        "max_hourly_cost": settings.vastai_max_hourly_cost,
        "max_instances": settings.vastai_max_instances,
        "destroy_idle_enabled": True,
        "idle_timeout_minutes": settings.vastai_idle_timeout_minutes,
    },
    "embedding": {
        "search_filter": settings.vastai_search_filter,
        "disk_gb": 20,
        "max_hourly_cost": settings.vastai_max_hourly_cost,
        "max_instances": settings.vastai_max_instances,
        "destroy_idle_enabled": True,
        "idle_timeout_minutes": settings.vastai_idle_timeout_minutes,
    },
    "llm": {
        "search_filter": settings.vastai_llm_search_filter,
        "disk_gb": settings.vastai_llm_disk_gb,
        "max_hourly_cost": settings.vastai_llm_max_hourly_cost,
        "max_instances": settings.vastai_max_instances,
        "destroy_idle_enabled": True,
        "idle_timeout_minutes": settings.vastai_idle_timeout_minutes,
    },
}


@dataclass
class LaneConfig:
    """Resolved config for one lane — DB values with settings fallback."""

    lane: str
    search_filter: str
    disk_gb: int
    max_hourly_cost: float
    max_instances: int
    destroy_idle_enabled: bool
    idle_timeout_minutes: int
    from_db: bool


def get_lane_config(db: Session, lane: str) -> LaneConfig:
    """Return the effective config for one lane, falling back to settings defaults."""
    if lane not in LANES and lane not in {LEGACY_LLM_LANE, "llm_refinement", "llm_enrichment"}:
        raise ValueError(f"unknown_lane:{lane}")
    normalized_lane = LEGACY_LLM_LANE if lane in {"llm_refinement", "llm_enrichment"} else lane
    effective_lane = normalized_lane if normalized_lane in LANES else LEGACY_LLM_LANE
    defaults = _DEFAULTS[effective_lane]
    lookup_lanes = [normalized_lane]
    if normalized_lane == LEGACY_LLM_LANE:
        lookup_lanes.extend(["llm_refinement", "llm_enrichment"])
    row = None
    for candidate in lookup_lanes:
        row = db.query(VastaiLaneConfig).filter_by(lane=candidate).first()
        if row is not None:
            break
    if row is None:
        return LaneConfig(
            lane=normalized_lane,
            search_filter=defaults["search_filter"],
            disk_gb=defaults["disk_gb"],
            max_hourly_cost=defaults["max_hourly_cost"],
            max_instances=defaults["max_instances"],
            destroy_idle_enabled=defaults["destroy_idle_enabled"],
            idle_timeout_minutes=defaults["idle_timeout_minutes"],
            from_db=False,
        )
    return LaneConfig(
        lane=normalized_lane,
        search_filter=row.search_filter or defaults["search_filter"],
        disk_gb=row.disk_gb if row.disk_gb is not None else defaults["disk_gb"],
        max_hourly_cost=row.max_hourly_cost if row.max_hourly_cost is not None else defaults["max_hourly_cost"],
        max_instances=row.max_instances if row.max_instances is not None else defaults["max_instances"],
        destroy_idle_enabled=(
            row.destroy_idle_enabled
            if row.destroy_idle_enabled is not None
            else defaults["destroy_idle_enabled"]
        ),
        idle_timeout_minutes=(
            row.idle_timeout_minutes
            if row.idle_timeout_minutes is not None
            else defaults["idle_timeout_minutes"]
        ),
        from_db=True,
    )


def get_all_lane_configs(db: Session) -> list[LaneConfig]:
    """Return resolved config for all lanes."""
    return [get_lane_config(db, lane) for lane in LANES]


def upsert_lane_config(
    db: Session,
    lane: str,
    *,
    search_filter: str | None = None,
    disk_gb: int | None = None,
    max_hourly_cost: float | None = None,
    max_instances: int | None = None,
    destroy_idle_enabled: bool | None = None,
    idle_timeout_minutes: int | None = None,
) -> VastaiLaneConfig:
    """Create or update lane config row. Pass None to reset a field to default."""
    if lane not in LANES and lane not in {"llm_refinement", "llm_enrichment"}:
        raise ValueError(f"unknown_lane:{lane}")
    normalized_lane = LEGACY_LLM_LANE if lane in {"llm_refinement", "llm_enrichment"} else lane
    row = db.query(VastaiLaneConfig).filter_by(lane=normalized_lane).first()
    if row is None:
        row = VastaiLaneConfig(lane=normalized_lane)
        db.add(row)
    row.search_filter = search_filter
    row.disk_gb = disk_gb
    row.max_hourly_cost = max_hourly_cost
    row.max_instances = max_instances
    row.destroy_idle_enabled = destroy_idle_enabled
    row.idle_timeout_minutes = idle_timeout_minutes
    db.commit()
    db.refresh(row)
    return row
