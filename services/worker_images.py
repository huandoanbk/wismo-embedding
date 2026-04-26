"""Worker image control-plane services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.models import WorkerImage

VALID_LANES = {"whisper", "embedding", "llm", "llm_refinement", "llm_enrichment"}
LEGACY_LLM_LANE = "llm"
LLM_OPERATION_IMAGE_LANES = {"llm_refinement", "llm_enrichment"}
VALIDATION_PENDING = "pending"
VALIDATION_VALID = "valid"
VALIDATION_INVALID = "invalid"
OCI_ACCEPT_HEADER = (
    "application/vnd.oci.image.index.v1+json, "
    "application/vnd.docker.distribution.manifest.list.v2+json, "
    "application/vnd.oci.image.manifest.v1+json, "
    "application/vnd.docker.distribution.manifest.v2+json"
)


class WorkerImageError(ValueError):
    """Base error for invalid worker image operations."""


class WorkerImageConfigError(RuntimeError):
    """Raised when runtime image state is invalid for one worker lane."""


@dataclass
class ImageValidationResult:
    """Result of validating one image ref against the container registry."""

    status: str
    error: str | None = None
    last_validated_at: datetime | None = None


def normalize_lane(lane: str) -> str:
    """Validate and normalize a worker lane name."""
    normalized = (lane or "").strip().lower()
    if normalized not in VALID_LANES:
        raise WorkerImageError("invalid_lane")
    return normalized


def lane_lookup_order(lane: str) -> list[str]:
    """Return preferred DB lookup order for a requested lane.

    New operation-scoped LLM lanes fall back to the legacy ``llm`` lane so
    existing deployments remain launchable while operators migrate images.
    """
    normalized = normalize_lane(lane)
    if normalized in LLM_OPERATION_IMAGE_LANES:
        return [LEGACY_LLM_LANE, normalized]
    return [normalized]


def validate_image_ref_format(image_ref: str) -> str:
    """Validate image ref format for registry/repository:tag style refs."""
    normalized = (image_ref or "").strip()
    if not normalized:
        raise WorkerImageError("invalid_image_ref")
    if "@" in normalized:
        raise WorkerImageError("digest_refs_not_supported")
    if "/" not in normalized or ":" not in normalized.rsplit("/", 1)[-1]:
        raise WorkerImageError("invalid_image_ref")
    if re.search(r"\s", normalized):
        raise WorkerImageError("invalid_image_ref")
    return normalized


def parse_image_ref(image_ref: str) -> tuple[str, str, str]:
    """Split an image ref into registry, repository, and tag parts."""
    normalized = validate_image_ref_format(image_ref)
    registry, remainder = normalized.split("/", 1)
    repository, tag = remainder.rsplit(":", 1)
    if not registry or not repository or not tag:
        raise WorkerImageError("invalid_image_ref")
    return registry, repository, tag


def list_worker_images(db: Session) -> list[WorkerImage]:
    """Return worker image records ordered for operator inspection."""
    stmt = select(WorkerImage).order_by(WorkerImage.lane.asc(), WorkerImage.created_at.desc())
    return list(db.execute(stmt).scalars())


def create_worker_image(
    db: Session,
    *,
    lane: str,
    image_ref: str,
    display_name: str | None = None,
    notes: str | None = None,
) -> WorkerImage:
    """Create one worker image record in pending validation state."""
    record = WorkerImage(
        lane=normalize_lane(lane),
        image_ref=validate_image_ref_format(image_ref),
        display_name=(display_name or "").strip() or None,
        notes=(notes or "").strip() or None,
        is_active=False,
        validation_status=VALIDATION_PENDING,
        last_validated_at=None,
        validation_error=None,
    )
    db.add(record)
    db.flush()
    return record


def validate_worker_image(
    db: Session,
    worker_image_id: UUID | str,
    *,
    client: httpx.Client | None = None,
) -> WorkerImage:
    """Validate one image record against registry manifest and platform."""
    record = db.get(WorkerImage, UUID(str(worker_image_id)))
    if record is None:
        raise WorkerImageError("worker_image_not_found")

    result = probe_image_ref(record.image_ref, client=client)
    record.validation_status = result.status
    record.last_validated_at = result.last_validated_at or datetime.utcnow()
    record.validation_error = result.error
    record.updated_at = datetime.utcnow()
    db.flush()
    return record


def activate_worker_image(db: Session, worker_image_id: UUID | str) -> WorkerImage:
    """Activate one valid image record for its lane and deactivate old active rows."""
    record = db.get(WorkerImage, UUID(str(worker_image_id)))
    if record is None:
        raise WorkerImageError("worker_image_not_found")
    if record.validation_status != VALIDATION_VALID:
        raise WorkerImageError("worker_image_not_valid")

    now = datetime.utcnow()
    active_stmt = select(WorkerImage).where(
        WorkerImage.lane == record.lane,
        WorkerImage.is_active.is_(True),
        WorkerImage.id != record.id,
    )
    for active in db.execute(active_stmt).scalars():
        active.is_active = False
        active.updated_at = now
    # Flush deactivation first so the partial unique index on (lane WHERE is_active)
    # is satisfied before activating the new record.
    db.flush()

    record.is_active = True
    record.activated_at = now
    record.updated_at = now
    db.flush()
    return record


def get_active_worker_image(db: Session, lane: str) -> WorkerImage:
    """Return the single valid active worker image for one lane or fail closed."""
    normalized_lane = normalize_lane(lane)
    last_error = WorkerImageConfigError(f"no_active_worker_image:{normalized_lane}")
    for candidate_lane in lane_lookup_order(normalized_lane):
        stmt = select(WorkerImage).where(
            WorkerImage.lane == candidate_lane,
            WorkerImage.is_active.is_(True),
        )
        rows = list(db.execute(stmt).scalars())
        if not rows:
            last_error = WorkerImageConfigError(f"no_active_worker_image:{normalized_lane}")
            continue
        if len(rows) > 1:
            raise WorkerImageConfigError(f"multiple_active_worker_images:{candidate_lane}")
        record = rows[0]
        if record.validation_status != VALIDATION_VALID:
            last_error = WorkerImageConfigError(f"active_worker_image_not_valid:{candidate_lane}")
            continue
        return record
    raise last_error


def get_worker_image_lane_status(db: Session, lane: str) -> dict[str, Any]:
    """Return operator-facing readiness state for one worker lane.

    For lane ``llm``: ``launch_ready`` reflects only the image
    validated+active criterion. Full LLM readiness (health, model_usable,
    callback_path) is checked separately via ``get_llm_lane_readiness`` in
    services.vastai_manager.
    """
    normalized_lane = normalize_lane(lane)
    active_rows: list[WorkerImage] = []
    resolved_lane = normalized_lane
    for candidate_lane in lane_lookup_order(normalized_lane):
        active_stmt = select(WorkerImage).where(
            WorkerImage.lane == candidate_lane,
            WorkerImage.is_active.is_(True),
        )
        active_rows = list(db.execute(active_stmt).scalars())
        if active_rows:
            resolved_lane = candidate_lane
            break
    status = "ready"
    message = None
    active_record = active_rows[0] if len(active_rows) == 1 else None
    if not active_rows:
        status = "missing_active_image"
        message = f"Lane {normalized_lane} chưa có active image."
    elif len(active_rows) > 1:
        status = "invalid_active_state"
        message = f"Lane {normalized_lane} có nhiều active images."
    elif active_record.validation_status != VALIDATION_VALID:
        status = "active_image_not_valid"
        message = f"Lane {normalized_lane} có active image nhưng chưa valid."
    return {
        "lane": normalized_lane,
        "resolved_lane": resolved_lane,
        "launch_ready": status == "ready",
        "status": status,
        "message": message,
        "active_image_id": str(active_record.id) if active_record else None,
        "active_image_ref": active_record.image_ref if active_record else None,
        "validation_status": active_record.validation_status if active_record else None,
    }


def _resolve_bearer_token(
    client: httpx.Client,
    www_authenticate: str,
    repository: str,
) -> str | None:
    """Exchange a WWW-Authenticate Bearer challenge for an anonymous pull token.

    Handles registries (e.g. GHCR) that return 401 + Bearer challenge even for
    public images. Returns the token string or None on failure.
    """
    # Parse realm, service, scope from WWW-Authenticate header
    realm = re.search(r'realm="([^"]+)"', www_authenticate)
    service = re.search(r'service="([^"]+)"', www_authenticate)
    scope = re.search(r'scope="([^"]+)"', www_authenticate)
    if not realm:
        return None
    params: dict[str, str] = {}
    if service:
        params["service"] = service.group(1)
    if scope:
        params["scope"] = scope.group(1)
    else:
        # Fallback scope for pull access
        params["scope"] = f"repository:{repository}:pull"
    try:
        resp = client.get(realm.group(1), params=params, timeout=10.0)
        if resp.status_code == 200:
            return resp.json().get("token") or resp.json().get("access_token")
    except Exception:  # noqa: BLE001
        pass
    return None


def probe_image_ref(image_ref: str, *, client: httpx.Client | None = None) -> ImageValidationResult:
    """Validate manifest existence and linux/amd64 support for one image ref.

    Handles Bearer token challenges (e.g. GHCR) by performing an anonymous
    token exchange before retrying the manifest request.
    """
    registry, repository, tag = parse_image_ref(image_ref)
    close_client = client is None
    http_client = client or httpx.Client(timeout=10.0, follow_redirects=True)
    validated_at = datetime.utcnow()
    try:
        manifest_url = f"https://{registry}/v2/{repository}/manifests/{tag}"
        headers = {"Accept": OCI_ACCEPT_HEADER}
        response = http_client.get(manifest_url, headers=headers)

        # Handle Bearer token challenge (e.g. GHCR requires this even for public images)
        if response.status_code == 401:
            www_auth = response.headers.get("www-authenticate", "")
            token = _resolve_bearer_token(http_client, www_auth, repository)
            if token:
                headers = {**headers, "Authorization": f"Bearer {token}"}
                response = http_client.get(manifest_url, headers=headers)

        if response.status_code >= 400:
            return ImageValidationResult(
                status=VALIDATION_INVALID,
                error=f"manifest_request_failed:{response.status_code}",
                last_validated_at=validated_at,
            )

        content_type = response.headers.get("content-type", "").split(";")[0].strip()
        payload = response.json()
        auth_headers = {k: v for k, v in headers.items() if k.lower() == "authorization"}
        if _manifest_supports_linux_amd64(payload, content_type, http_client, registry, repository, auth_headers):
            return ImageValidationResult(status=VALIDATION_VALID, error=None, last_validated_at=validated_at)
        return ImageValidationResult(
            status=VALIDATION_INVALID,
            error="platform_not_supported:linux/amd64",
            last_validated_at=validated_at,
        )
    except WorkerImageError as exc:
        return ImageValidationResult(status=VALIDATION_INVALID, error=str(exc), last_validated_at=validated_at)
    except httpx.HTTPError as exc:
        return ImageValidationResult(status=VALIDATION_INVALID, error=f"registry_http_error:{exc}", last_validated_at=validated_at)
    finally:
        if close_client:
            http_client.close()


def _manifest_supports_linux_amd64(
    payload: dict[str, Any],
    content_type: str,
    client: httpx.Client,
    registry: str,
    repository: str,
    auth_headers: dict[str, str] | None = None,
) -> bool:
    """Return True when manifest or index supports linux/amd64."""
    extra = auth_headers or {}
    manifests = payload.get("manifests")
    if isinstance(manifests, list):
        for manifest in manifests:
            platform = manifest.get("platform") or {}
            if platform.get("os") == "linux" and platform.get("architecture") == "amd64":
                return True
        return False

    if content_type in {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }:
        config = payload.get("config") or {}
        digest = config.get("digest")
        if not digest:
            return False
        blob_url = f"https://{registry}/v2/{repository}/blobs/{digest}"
        response = client.get(blob_url, headers=extra)
        if response.status_code >= 400:
            return False
        blob = response.json()
        return blob.get("os") == "linux" and blob.get("architecture") == "amd64"

    return False


def count_active_worker_images(db: Session, lane: str) -> int:
    """Return the number of active records for one lane."""
    normalized_lane = normalize_lane(lane)
    stmt = select(func.count()).select_from(WorkerImage).where(
        WorkerImage.lane == normalized_lane,
        WorkerImage.is_active.is_(True),
    )
    return int(db.scalar(stmt) or 0)
