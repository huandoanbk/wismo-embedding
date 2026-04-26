"""Vast.ai autoscale orchestration for GPU workers."""

from __future__ import annotations

import ast
from datetime import datetime
import json
import logging
import re
import subprocess
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

import httpx
import redis

from core.database import SessionLocal
from core.config import settings
from core.llm_lane import LLM_QUEUE_ENRICHMENT, LLM_QUEUE_REFINEMENT, LlmLaneReadiness
from services.vastai_lane_config_ops import get_lane_config
from services.worker_images import WorkerImageConfigError, get_active_worker_image, get_worker_image_lane_status, VALIDATION_VALID
from services.worker_alerts import WorkerAlertPayload, emit_local_worker_alert


@dataclass
class VastAIInstance:
    """Minimal running instance view used by autoscale logic."""

    instance_id: str
    public_ipaddr: str | None
    direct_port_start: int | None
    actual_status: str | None
    worker_mode: str | None = None
    image_ref: str | None = None


class VastAIManager:
    """Queue-driven Vast.ai scaling helper with hard operational guardrails."""

    SEARCH = settings.vastai_search_filter
    LLM_SEARCH = settings.vastai_llm_search_filter
    LLM_DISK_GB = settings.vastai_llm_disk_gb
    MAX_INSTANCES = settings.vastai_max_instances
    MAX_HOURLY_COST = settings.vastai_max_hourly_cost
    LLM_MAX_HOURLY_COST = settings.vastai_llm_max_hourly_cost

    def __init__(self) -> None:
        # Queue depth must be checked on the same Redis that workers pull from.
        broker_url = settings.vastai_redis_url or settings.redis_url
        self._redis = redis.from_url(broker_url)

    def _run_vastai(self, *args: str) -> Any:
        """Run a vastai CLI command and return the parsed payload.

        The ``create instance`` subcommand does not honour ``--raw`` and emits
        Python-dict-formatted text instead.  All other commands return proper
        JSON when ``--raw`` is passed, so we handle the two cases separately.
        """
        api_key = settings.vastai_api_key
        key_args = ["--api-key", api_key] if api_key else []
        is_create_instance = list(args[:2]) == ["create", "instance"]
        raw_args = [] if is_create_instance else ["--raw"]
        cmd = ["vastai", *args, *raw_args, *key_args]
        logger.info("vastai cmd: %s", " ".join(a if "api-key" not in a else "***" for a in cmd))
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        logger.info("vastai exit=%s stdout=%r stderr=%r", result.returncode, stdout, stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"vastai CLI exited {result.returncode}: {stderr or stdout!r}"
            )
        if not stdout:
            if stderr:
                raise RuntimeError(f"vastai CLI returned empty stdout. stderr: {stderr!r}")
            return {}
        if is_create_instance:
            return self._parse_create_instance_output(stdout)
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"vastai CLI returned non-JSON output: {stdout!r} stderr: {stderr!r}"
            ) from exc

    @staticmethod
    def _parse_create_instance_output(stdout: str) -> dict[str, Any]:
        """Parse vastai create instance output.

        The CLI emits a line like ``Started. {'new_contract': 123, ...}``
        using Python repr for the dict, not JSON.
        """
        # Strip leading prose ("Started. ", "Failed. ", etc.)
        match = re.search(r"\{.*\}", stdout, re.DOTALL)
        if not match:
            raise RuntimeError(f"vastai create instance: unexpected output {stdout!r}")
        try:
            return ast.literal_eval(match.group(0))
        except (ValueError, SyntaxError) as exc:
            raise RuntimeError(
                f"vastai create instance: could not parse output {stdout!r}"
            ) from exc

    def _get_queue_depth(self, queue_name: str) -> int:
        """Return Redis queue depth for a Celery queue."""
        return int(self._redis.llen(queue_name))

    def _instance_mode_key(self, instance_id: str) -> str:
        """Return Redis key used to remember the intended worker mode of one instance."""
        return f"vastai:instance_mode:{instance_id}"

    def _remember_instance_mode(self, instance_id: str, mode: str) -> None:
        """Persist the intended worker mode for one launched instance."""
        self._redis.set(self._instance_mode_key(instance_id), mode)

    def _recall_instance_mode(self, instance_id: str) -> str | None:
        """Return the remembered worker mode for one instance when available."""
        raw = self._redis.get(self._instance_mode_key(instance_id))
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    def _forget_instance_mode(self, instance_id: str) -> None:
        """Delete the remembered worker mode for one instance."""
        self._redis.delete(self._instance_mode_key(instance_id))

    def _idle_timeout_exceeded(self, lane: str, timeout_minutes: int) -> bool:
        """Return True when a lane has been idle longer than the configured timeout.

        Records the timestamp when a lane first goes idle in Redis. On each
        subsequent call while still idle, checks whether the elapsed time exceeds
        the configured threshold. Clears the marker when the lane becomes active.
        """
        key = f"vastai:idle_since:{lane}"
        idle_since_raw = self._redis.get(key)
        if idle_since_raw is None:
            # First time we see the lane idle — record now, don't destroy yet.
            self._redis.set(key, datetime.utcnow().isoformat())
            return False
        idle_since = datetime.fromisoformat(idle_since_raw.decode())
        elapsed_minutes = (datetime.utcnow() - idle_since).total_seconds() / 60
        return elapsed_minutes >= timeout_minutes

    def _mark_lane_active(self, lane: str) -> None:
        """Clear the idle timestamp for a lane that has work."""
        self._redis.delete(f"vastai:idle_since:{lane}")

    def _maybe_destroy_idle_lane(self, lane: str, instances: list[VastAIInstance]) -> None:
        """Destroy idle instances for one lane when lane config allows it."""
        cfg = self._lane_config(lane)
        if not cfg.destroy_idle_enabled:
            self._mark_lane_active(lane)
            return
        if self._idle_timeout_exceeded(lane, cfg.idle_timeout_minutes):
            for instance in instances:
                self.destroy_instance(instance.instance_id)

    def _emit_alert(self, *, severity: str, event_type: str, details: dict[str, Any]) -> None:
        """Persist a local service alert without breaking autoscale logic."""
        try:
            emit_local_worker_alert(
                WorkerAlertPayload(
                    severity=severity,
                    event_type=event_type,
                    source="vastai_manager",
                    occurred_at=datetime.utcnow(),
                    details=details,
                )
            )
        except Exception:
            return

    def inspect_instance(self, instance: VastAIInstance, *, timeout: float | None = None) -> dict[str, Any]:
        """Return health and mode details reported by one running worker instance."""
        # direct_port_start of -1 means the instance is still initialising its
        # port mappings.  Treat as "starting" rather than "unhealthy" so
        # auto_scale does not destroy a perfectly valid instance too early.
        if not instance.public_ipaddr or instance.direct_port_start is None or instance.direct_port_start < 0:
            return {"healthy": None, "worker_mode": instance.worker_mode}
        effective_timeout = timeout if timeout is not None else settings.vastai_health_timeout_seconds
        try:
            response = httpx.get(
                f"http://{instance.public_ipaddr}:{instance.direct_port_start}/health",
                timeout=effective_timeout,
            )
            response.raise_for_status()
            payload = response.json()
            return {
                "healthy": payload.get("status") == "healthy",
                "worker_mode": payload.get("worker_mode", instance.worker_mode or "both"),
                "image_ref": instance.image_ref,
            }
        except httpx.HTTPError as exc:
            self._emit_alert(
                severity="warning",
                event_type="gpu_worker_healthcheck_failed",
                details={"message": str(exc), "instance_id": instance.instance_id},
            )
            return {"healthy": False, "worker_mode": instance.worker_mode, "image_ref": instance.image_ref}

    def health_check(self, instance: VastAIInstance) -> bool:
        """Return True when the GPU worker health endpoint reports healthy."""
        return bool(self.inspect_instance(instance)["healthy"])

    def _lane_config(self, mode: str) -> "LaneConfig":  # type: ignore[name-defined]
        """Load per-lane config from DB, falling back to settings defaults."""
        lane = "llm" if mode in ("llm", "llm_refinement", "llm_enrichment") else mode
        lane = lane if lane in ("whisper", "embedding", "llm") else "whisper"
        session = SessionLocal()
        try:
            return get_lane_config(session, lane)
        finally:
            session.close()

    def find_affordable_offers(self, limit: int = 5, mode: str = "both") -> list[dict[str, Any]]:
        """Return up to `limit` offers under the cost limit for the given mode, cheapest first.

        Per-lane filter and cost ceiling are loaded from DB (dashboard-configurable),
        falling back to settings defaults when no DB row exists.
        """
        lane = "llm" if mode in ("llm", "llm_refinement", "llm_enrichment") else mode
        lane = lane if lane in ("whisper", "embedding", "llm") else "whisper"
        cfg = self._lane_config(lane)
        search_filter = cfg.search_filter
        max_cost = cfg.max_hourly_cost
        offers = self._run_vastai("search", "offers", search_filter, "-o", "dph+")
        result = []
        for offer in offers:
            if float(offer.get("dph_total", offer.get("dph", 0.0))) <= max_cost:
                result.append(offer)
                if len(result) >= limit:
                    break
        return result

    def find_cheapest_offer(self, mode: str = "both") -> dict[str, Any] | None:
        """Return the first offer under the configured hourly cost limit for the given mode."""
        offers = self.find_affordable_offers(limit=1, mode=mode)
        return offers[0] if offers else None

    def worker_image_for_mode(self, mode: str) -> str:
        """Return the single valid active registry image for one worker lane."""
        normalized_mode = "llm" if mode in {"llm", "llm_refinement", "llm_enrichment"} else mode
        if normalized_mode not in {"whisper", "embedding", "llm"}:
            raise WorkerImageConfigError(f"unsupported_worker_mode:{mode}")
        session = SessionLocal()
        try:
            return get_active_worker_image(session, normalized_mode).image_ref
        finally:
            session.close()

    def get_lane_launch_status(self, mode: str) -> dict[str, Any]:
        """Return launch readiness for one worker lane from the DB-backed registry."""
        normalized_mode = "llm" if mode in {"llm", "llm_refinement", "llm_enrichment"} else mode
        session = SessionLocal()
        try:
            return get_worker_image_lane_status(session, normalized_mode)
        finally:
            session.close()

    def get_llm_lane_readiness(
        self,
        instance: VastAIInstance | None = None,
        *,
        lane: str = "llm",
        callback_url: str | None = None,
    ) -> LlmLaneReadiness:
        """Return full readiness state for lane `llm` against all four criteria.

        Criteria:
            1. image_validated_active: worker image is validated and active in DB.
            2. worker_health_pass: health endpoint returns healthy=True.
            3. model_usable: /model-ready endpoint responds with usable=True.
            4. callback_path_usable: callback URL is non-empty (runtime reachability
               is checked separately by the caller if needed).

        Inputs:
            instance: optional running VastAIInstance for health/model probing.
                      When None, health and model criteria default to False.
            callback_url: optional VPS callback URL to verify non-empty.

        Output:
            LlmLaneReadiness with all four criteria populated.

        Side effects:
            May make HTTP calls to the running instance health/model endpoints.
        """
        readiness = LlmLaneReadiness()
        details: dict[str, str] = {}

        # Criterion 1: image_validated_active
        session = SessionLocal()
        try:
            lane_status = get_worker_image_lane_status(session, lane)
            readiness.image_validated_active = bool(lane_status.get("launch_ready"))
            if not readiness.image_validated_active:
                details["image"] = lane_status.get("status") or "missing_active_image"
        finally:
            session.close()

        # Criterion 2: worker_health_pass
        if instance is not None:
            inspection = self.inspect_instance(instance)
            readiness.worker_health_pass = bool(inspection.get("healthy"))
            if not readiness.worker_health_pass:
                details["health"] = "health_endpoint_unhealthy"
        else:
            details["health"] = "no_running_instance"

        # Criterion 3: model_usable (probe /model-ready endpoint)
        if instance is not None and instance.public_ipaddr and instance.direct_port_start is not None:
            try:
                response = httpx.get(
                    f"http://{instance.public_ipaddr}:{instance.direct_port_start}/model-ready",
                    timeout=settings.vastai_health_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                readiness.model_usable = bool(payload.get("usable"))
                if not readiness.model_usable:
                    details["model"] = payload.get("reason") or "model_not_usable"
            except httpx.HTTPError as exc:
                details["model"] = f"model_ready_probe_failed:{exc}"
        else:
            details["model"] = "no_running_instance_for_model_probe"

        # Criterion 4: callback_path_usable
        effective_url = callback_url or settings.vps_callback_url
        readiness.callback_path_usable = bool(effective_url and effective_url.strip())
        if not readiness.callback_path_usable:
            details["callback"] = "vps_callback_url_not_configured"

        readiness.details = details
        return readiness

    def launch_instance(self, offer_id: str, mode: str = "both", image: str | None = None) -> str:
        """Launch a Vast.ai instance from the prebuilt registry image."""
        normalized_mode = "llm" if mode in {"llm", "llm_refinement", "llm_enrichment"} else mode
        env_pairs = {
            "REDIS_URL": settings.vastai_redis_url or settings.redis_url,
            "S3_ENDPOINT_URL": settings.s3_endpoint_url,
            "S3_ACCESS_KEY": settings.s3_access_key,
            "S3_SECRET_KEY": settings.s3_secret_key,
            "S3_REGION": settings.s3_region,
            "S3_BUCKET_RAW": settings.s3_bucket_raw,
            "S3_BUCKET_EXTRACTS": settings.s3_bucket_extracts,
            "VPS_CALLBACK_URL": settings.vps_callback_url,
            "WORKER_MODE": normalized_mode,
            "INTERNAL_SHARED_SECRET": settings.internal_shared_secret,
            "GPU_TASK_REQUEST_TIMEOUT_SECONDS": str(settings.gpu_task_request_timeout_seconds),
        }
        if settings.huggingface_hub_token:
            env_pairs["HF_TOKEN"] = settings.huggingface_hub_token
        if normalized_mode == "llm":
            env_pairs["LLM_MODEL_NAME"] = settings.llm_model_name
            env_pairs["LLM_QUANTIZATION"] = settings.llm_quantization
        if normalized_mode == "llm":
            env_pairs["LLM_QUEUE_NAMES"] = f"{LLM_QUEUE_REFINEMENT},{LLM_QUEUE_ENRICHMENT}"
        env_string = " ".join(f"-e {key}={value}" for key, value in env_pairs.items())
        lane = normalized_mode if normalized_mode in ("whisper", "embedding", "llm") else "whisper"
        disk_gb = str(self._lane_config(lane).disk_gb)
        entrypoint_map = {
            "whisper": "bash /app/gpu_worker/entrypoint_whisper.sh",
            "embedding": "bash /app/gpu_worker/entrypoint_embedding.sh",
            "llm": "bash /app/gpu_worker/entrypoint_llm.sh",
            "both": "bash /app/gpu_worker/entrypoint_whisper.sh",
        }
        entrypoint = entrypoint_map.get(normalized_mode, "bash /app/gpu_worker/entrypoint_whisper.sh")
        # Embed env vars directly in the onstart command because Vast.ai does not
        # automatically inject --env values into the onstart shell environment.
        env_exports = " ".join(f"export {k}='{v}';" for k, v in env_pairs.items())
        onstart_cmd = f"{env_exports} {entrypoint}"
        try:
            payload = self._run_vastai(
                "create",
                "instance",
                str(offer_id),
                "--image",
                image or self.worker_image_for_mode(normalized_mode),
                "--env",
                env_string,
                "--disk",
                disk_gb,
                "--onstart-cmd",
                onstart_cmd,
            )
            # Vast.ai CLI may return new_contract or id depending on version
            contract_id = (
                payload.get("new_contract")
                or payload.get("id")
                or payload.get("contract_id")
            )
            if not contract_id:
                raise RuntimeError(f"vastai create instance returned unexpected payload: {payload!r}")
            contract_id_str = str(contract_id)
            self._remember_instance_mode(contract_id_str, normalized_mode)
            return contract_id_str
        except Exception as exc:
            self._emit_alert(
                severity="error",
                event_type="vastai_launch_failed",
                details={"message": str(exc), "offer_id": offer_id},
            )
            raise

    def destroy_instance(self, instance_id: str) -> None:
        """Destroy a Vast.ai instance by id."""
        self._run_vastai("destroy", "instance", str(instance_id))
        self._forget_instance_mode(str(instance_id))

    def get_running_instances(self) -> list[VastAIInstance]:
        """Return current Vast.ai instances associated with the worker image."""
        payload = self._run_vastai("show", "instances")
        instances: list[VastAIInstance] = []
        for item in payload:
            instance_id = str(item["id"])
            instances.append(
                VastAIInstance(
                    instance_id=instance_id,
                    public_ipaddr=item.get("public_ipaddr"),
                    direct_port_start=item.get("direct_port_start"),
                    actual_status=item.get("actual_status"),
                    worker_mode=self._recall_instance_mode(instance_id),
                    image_ref=item.get("image") or item.get("image_ref") or item.get("image_uuid"),
                )
            )
        return instances

    def auto_scale(self) -> None:
        """Reconcile lane-specific GPU instances against queue depth, health, and cost guardrails."""
        instances = self.get_running_instances()
        existing_by_mode: dict[str, list[VastAIInstance]] = {
            "whisper": [],
            "embedding": [],
            "llm": [],
            "both": [],
        }
        healthy_by_mode: dict[str, list[VastAIInstance]] = {
            "whisper": [],
            "embedding": [],
            "llm": [],
            "both": [],
        }
        for instance in instances:
            remembered_mode = str(instance.worker_mode or "both")
            if remembered_mode in {"llm_refinement", "llm_enrichment"}:
                remembered_mode = "llm"
            existing_by_mode.setdefault(remembered_mode, []).append(instance)
            inspection = self.inspect_instance(instance)
            healthy = inspection["healthy"]
            if healthy is None:
                # Still initialising (no port mapping yet) — leave it alone.
                worker_mode = str(inspection.get("worker_mode") or instance.worker_mode or "both")
                if worker_mode in {"llm_refinement", "llm_enrichment"}:
                    worker_mode = "llm"
                instance.worker_mode = worker_mode
                healthy_by_mode.setdefault(worker_mode, []).append(instance)
                continue
            if not healthy:
                self.destroy_instance(instance.instance_id)
                continue
            worker_mode = str(inspection.get("worker_mode") or "both")
            if worker_mode in {"llm_refinement", "llm_enrichment"}:
                worker_mode = "llm"
            instance.worker_mode = worker_mode
            healthy_by_mode.setdefault(worker_mode, []).append(instance)

        whisper_depth = self._get_queue_depth("whisper")
        embedding_depth = self._get_queue_depth("embedding")
        llm_refinement_depth = self._get_queue_depth(LLM_QUEUE_REFINEMENT)
        llm_enrichment_depth = self._get_queue_depth(LLM_QUEUE_ENRICHMENT)
        whisper_cfg = self._lane_config("whisper")
        embedding_cfg = self._lane_config("embedding")
        llm_cfg = self._lane_config("llm")
        llm_total_depth = llm_refinement_depth + llm_enrichment_depth

        # Track active lanes to clear idle markers.
        if whisper_depth > 0:
            self._mark_lane_active("whisper")
        if embedding_depth > 0:
            self._mark_lane_active("embedding")
        if llm_total_depth > 0:
            self._mark_lane_active("llm")

        if whisper_depth <= 0:
            self._maybe_destroy_idle_lane("whisper", healthy_by_mode.get("whisper", []))
        if embedding_depth <= 0:
            self._maybe_destroy_idle_lane("embedding", healthy_by_mode.get("embedding", []))
        if llm_total_depth <= 0:
            self._maybe_destroy_idle_lane("llm", healthy_by_mode.get("llm", []))

        total_existing = sum(len(items) for items in existing_by_mode.values())
        whisper_capacity = len(existing_by_mode.get("whisper", [])) + len(existing_by_mode.get("both", []))
        embedding_capacity = len(existing_by_mode.get("embedding", [])) + len(existing_by_mode.get("both", []))
        llm_capacity = len(existing_by_mode.get("llm", []))
        desired_by_mode = {
            "whisper": min(max(0, whisper_depth), whisper_cfg.max_instances),
            "embedding": min(max(0, embedding_depth), embedding_cfg.max_instances),
            "llm": min(max(0, llm_total_depth), llm_cfg.max_instances),
        }

        needed_modes: list[str] = []
        if whisper_depth > whisper_capacity and whisper_capacity < desired_by_mode["whisper"]:
            needed_modes.append("whisper")
        if embedding_depth > embedding_capacity and embedding_capacity < desired_by_mode["embedding"]:
            needed_modes.append("embedding")
        if llm_total_depth > llm_capacity and llm_capacity < desired_by_mode["llm"]:
            needed_modes.append("llm")

        if not needed_modes:
            return

        for mode in needed_modes:
            if total_existing >= self.MAX_INSTANCES:
                break
            lane_status = self.get_lane_launch_status(mode)
            if not lane_status["launch_ready"]:
                self._emit_alert(
                    severity="error",
                    event_type="worker_image_not_launch_ready",
                    details=lane_status,
                )
                continue
            offers = self.find_affordable_offers(limit=5, mode=mode)
            if not offers:
                self._emit_alert(
                    severity="warning",
                    event_type="vastai_no_offers_found",
                    details={"mode": mode},
                )
                continue
            # Try each offer in order — offers can disappear between search and create
            instance_id = None
            for offer in offers:
                try:
                    instance_id = self.launch_instance(str(offer["id"]), mode=mode)
                    break
                except RuntimeError as exc:
                    logger.warning("offer %s failed: %s — trying next", offer["id"], exc)
            if instance_id is None:
                self._emit_alert(
                    severity="error",
                    event_type="vastai_no_offer_accepted",
                    details={"mode": mode, "tried": [str(o["id"]) for o in offers]},
                )
                continue
            total_existing += 1
            healthy_by_mode.setdefault(mode, []).append(
                VastAIInstance(
                    instance_id=instance_id,
                    public_ipaddr=None,
                    direct_port_start=None,
                    actual_status="launching",
                    worker_mode=mode,
                    image_ref=lane_status["active_image_ref"],
                )
            )
