"""Manual upload connector that normalizes UploadFile inputs."""

from __future__ import annotations

import hashlib
import os
import uuid
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import UploadFile

from core.config import settings

from .base import SourceCandidate

SUPPORTED_EXTENSIONS = {
    "pdf",
    "docx",
    "epub",
    "txt",
    "pptx",
    "mp3",
    "mp4",
    "wav",
    "webm",
}


class ManualUploadConnector:
    """Convert manual uploads into normalized source candidates."""

    def __init__(self, s3_client: Any | None = None) -> None:
        self.client = s3_client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
        )

    def normalize_identity(self, raw_input: UploadFile | dict[str, Any]) -> dict[str, Any]:
        """Normalize upload metadata into a stable identity dict."""
        upload = raw_input if isinstance(raw_input, UploadFile) else raw_input["upload"]
        extension = self._extension_for(upload.filename or "")
        return {
            "filename": upload.filename,
            "extension": extension,
            "content_type": upload.content_type,
        }

    def discover_items(
        self,
        *,
        upload: UploadFile,
        raw_bucket: str | None = None,
    ) -> list[SourceCandidate]:
        """Return a single candidate for a manual upload."""
        return [self.create_candidate(upload=upload, raw_bucket=raw_bucket)]

    def create_candidate(
        self,
        *,
        upload: UploadFile,
        raw_bucket: str | None = None,
        source_id: str | None = None,
    ) -> SourceCandidate:
        """Create a normalized candidate from an uploaded file."""
        filename = upload.filename or "upload.bin"
        extension = self._extension_for(filename)
        checksum = self.compute_sha256(upload)
        raw_key = self.build_target_raw_key(filename=filename, source_id=source_id)
        size_bytes = self._size_for(upload)
        return SourceCandidate(
            source_type="manual_upload",
            external_item_id=checksum,
            normalized_source_url=None,
            display_name=filename,
            source_path=filename,
            mime_type=upload.content_type,
            size_bytes=size_bytes,
            raw_bucket=raw_bucket,
            raw_key=raw_key,
            metadata_json={
                "checksum_sha256": checksum,
                "extension": extension,
                "upload_file": upload,
            },
        )

    def fetch_metadata(self, candidate: SourceCandidate) -> dict[str, Any]:
        """Return metadata captured during normalization."""
        return self.serializable_metadata(candidate)

    def open_stream(self, candidate: SourceCandidate) -> Any:
        """Return the underlying UploadFile stream for downstream consumers."""
        upload = candidate.metadata_json.get("upload_file")
        if upload is None:
            raise ValueError("Manual upload candidate is missing upload_file metadata")
        upload.file.seek(0)
        return upload.file

    def serializable_metadata(self, candidate: SourceCandidate) -> dict[str, Any]:
        """Return metadata safe to persist in JSON columns."""
        metadata = dict(candidate.metadata_json)
        metadata.pop("upload_file", None)
        return metadata

    def mirror_candidate_to_raw_storage(self, candidate: SourceCandidate) -> None:
        """Upload one manual-upload candidate into the configured raw bucket/key."""
        if not candidate.raw_bucket or not candidate.raw_key:
            raise ValueError("Manual upload candidate requires raw bucket and key")

        if self.head_target_exists(candidate.raw_bucket, candidate.raw_key):
            return
        upload = candidate.metadata_json.get("upload_file")
        if upload is None:
            raise ValueError("Manual upload candidate is missing upload_file metadata")
        upload.file.seek(0)
        payload = upload.file.read()
        self.client.put_object(Bucket=candidate.raw_bucket, Key=candidate.raw_key, Body=payload)
        upload.file.seek(0)

    def compute_sha256(self, upload: UploadFile) -> str:
        """Compute the SHA-256 checksum for an uploaded file without losing stream position."""
        upload.file.seek(0)
        digest = hashlib.sha256()
        while True:
            chunk = upload.file.read(8192)
            if not chunk:
                break
            digest.update(chunk)
        upload.file.seek(0)
        return digest.hexdigest()

    def build_target_raw_key(self, *, filename: str, source_id: str | None = None) -> str:
        """Build the target key for raw storage mirroring."""
        source_uuid = source_id or str(uuid.uuid4())
        extension = self._extension_for(filename)
        prefix = self._prefix_for_extension(extension)
        safe_name = os.path.basename(filename)
        return f"{prefix}/{source_uuid}_{safe_name}"

    def head_target_exists(self, bucket: str, key: str) -> bool:
        """Check whether the target raw object already exists."""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def bucket_exists(self, bucket: str) -> bool:
        """Return True when the destination raw bucket exists and is accessible."""
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchBucket", "NotFound"}:
                return False
            raise

    def _extension_for(self, filename: str) -> str:
        extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if extension not in SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file extension: {extension or 'unknown'}")
        return extension

    def _prefix_for_extension(self, extension: str) -> str:
        if extension == "pdf":
            return "pdf"
        if extension in {"mp3", "wav"}:
            return "audio"
        if extension in {"mp4", "webm"}:
            return "video"
        return "other"

    def _size_for(self, upload: UploadFile) -> int | None:
        try:
            upload.file.seek(0, os.SEEK_END)
            size = upload.file.tell()
            upload.file.seek(0)
            return size
        except OSError:
            upload.file.seek(0)
            return None
