"""Thin Celery task module for the whisper worker lane."""

from __future__ import annotations

import ssl as _ssl
from pathlib import Path

from celery import Celery
import httpx

from gpu_worker.whisper_runtime.config import load_settings
from gpu_worker.whisper_runtime.runtime import WhisperRuntime

settings = load_settings()
app = Celery("wismo_whisper_gpu", broker=settings.broker_url)
app.conf.task_serializer = "json"
app.conf.result_backend = None
if settings.broker_url.startswith("rediss://"):
    app.conf.broker_use_ssl = {"ssl_cert_reqs": _ssl.CERT_NONE}
TRANSCRIBE_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
TRANSCRIBE_STALE_STATUS_CODES = {404, 409}


def _runtime() -> WhisperRuntime:
    """Return a runtime facade built from environment variables."""
    return WhisperRuntime(settings)


def _retry_countdown(retries: int) -> int:
    """Return a bounded exponential backoff in seconds for transcript retries."""
    return min(60 * (2**retries), 300)


@app.task(
    name="gpu_worker.tasks.transcribe_audio",
    queue="whisper",
    bind=True,
    max_retries=2,
    time_limit=3600,
)
def transcribe_audio(self, source_item_id: str) -> dict[str, str]:
    """Transcribe a mirrored media asset and callback the canonical extract key."""
    task_id = getattr(self.request, "id", None)
    runtime = _runtime()
    try:
        payload = runtime.resolve_source_item(source_item_id)
        raw_bucket = payload.get("raw_bucket") or settings.s3_bucket_raw
        raw_key = payload["raw_key"]
        source_id = payload["source_id"]
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in TRANSCRIBE_STALE_STATUS_CODES:
            runtime.send_alert(
                severity="warning",
                event_type="source_item_lookup_skipped",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "status": "skipped_stale_source_item",
                "status_code": str(exc.response.status_code),
            }
        if exc.response.status_code in TRANSCRIBE_RETRYABLE_STATUS_CODES:
            retry_count = int(getattr(self.request, "retries", 0))
            if retry_count < int(getattr(self, "max_retries", 0)):
                runtime.send_alert(
                    severity="warning",
                    event_type="transcribe_audio_retrying",
                    source="whisper_worker",
                    task_id=task_id,
                    source_item_id=source_item_id,
                    details={
                        "message": str(exc),
                        "status_code": exc.response.status_code,
                        "retry_count": retry_count + 1,
                    },
                )
                raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="error",
            event_type="source_item_lookup_failed",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        raise
    except Exception as exc:
        runtime.send_alert(
            severity="error",
            event_type="source_item_lookup_failed",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        raise

    local_path: Path | None = None
    try:
        local_path = runtime.download_raw_media(raw_bucket, raw_key)

        from faster_whisper import WhisperModel

        model = WhisperModel("large-v3", device="cuda", compute_type="float16")
        segments, info = model.transcribe(
            str(local_path),
            beam_size=5,
            language="vi",
            task="transcribe",
            vad_filter=True,
            vad_parameters={"min_silence_duration_ms": 500},
            temperature=0,
            condition_on_previous_text=False,
            no_speech_threshold=0.3,
        )
        segment_list = list(segments)
        transcript = {
            "source_id": source_id,
            "source_type": payload["source_type"],
            "extracted_at": payload.get("requested_at"),
            "extractor_version": "whisper-large-v3",
            "language": info.language,
            "language_probability": getattr(info, "language_probability", None),
            "duration_sec": info.duration,
            "full_text": " ".join(segment.text.strip() for segment in segment_list).strip(),
            "segments": [
                {"start": segment.start, "end": segment.end, "text": segment.text}
                for segment in segment_list
            ],
        }
        extract_key = runtime.upload_transcript(source_id, transcript)
        runtime.notify_transcript_ready(source_id, extract_key)
        return {"source_id": source_id, "extract_key": extract_key}
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="transcribe_audio_retrying",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "raw_key": raw_key,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="transcribe_audio_skipped_after_retries",
            source="whisper_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={
                "message": str(exc),
                "raw_key": raw_key,
                "retry_count": retry_count,
            },
        )
        try:
            runtime.notify_transcript_status(source_id, "failed", str(exc))
        except Exception:  # noqa: BLE001
            runtime.send_alert(
                severity="error",
                event_type="transcript_failure_callback_failed",
                source="whisper_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(exc), "raw_key": raw_key},
            )
        return {
            "source_item_id": source_item_id,
            "status": "skipped_after_retries_exhausted",
            "raw_key": raw_key,
        }
    finally:
        if local_path is not None:
            local_path.unlink(missing_ok=True)
