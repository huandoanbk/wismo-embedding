"""Contabo S3-compatible source connector."""

from __future__ import annotations

from datetime import datetime
from typing import Any, BinaryIO
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from core.config import settings

from .base import SourceCandidate


class ContaboConnector:
    """List, inspect, upload, and stream objects from Contabo Object Storage."""

    def __init__(self, s3_client: Any | None = None) -> None:
        self.client = s3_client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
        )

    def normalize_identity(self, raw_input: str | dict[str, Any]) -> dict[str, str]:
        """Normalize bucket/key input or s3:// URI into canonical identity fields."""
        if isinstance(raw_input, str):
            parsed = urlparse(raw_input)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
                raise ValueError("ContaboConnector expects an s3://bucket/key URI")
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
        else:
            bucket = str(raw_input["bucket"])
            key = str(raw_input["key"])

        return {
            "bucket": bucket,
            "key": key,
            "external_item_id": f"{bucket}:{key}",
            "normalized_source_url": f"s3://{bucket}/{key}",
        }

    def list_prefix(self, bucket: str, prefix: str) -> list[SourceCandidate]:
        """List all objects for a bucket/prefix pair."""
        paginator = self.client.get_paginator("list_objects_v2")
        candidates: list[SourceCandidate] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not self._is_scannable_object(obj):
                    continue
                candidates.append(self._map_object(bucket, obj))
        return candidates

    def discover_items(self, *, bucket: str, prefix: str) -> list[SourceCandidate]:
        """Discover all candidates under a Contabo prefix."""
        return self.list_prefix(bucket=bucket, prefix=prefix)

    def _map_object(self, bucket: str, obj: dict[str, Any]) -> SourceCandidate:
        key = str(obj["Key"])
        last_modified = obj.get("LastModified")
        if isinstance(last_modified, str):
            last_modified = datetime.fromisoformat(last_modified)
        extension = self._extension_for_key(key)
        return SourceCandidate(
            source_type="contabo_prefix",
            external_item_id=f"{bucket}:{key}",
            normalized_source_url=f"s3://{bucket}/{key}",
            display_name=key.rsplit("/", 1)[-1],
            source_path=key,
            size_bytes=obj.get("Size"),
            last_modified=last_modified,
            raw_bucket=bucket,
            raw_key=key,
            metadata_json={
                "etag": obj.get("ETag"),
                "storage_class": obj.get("StorageClass"),
                "extension": extension,
            },
        )

    @staticmethod
    def _is_scannable_object(obj: dict[str, Any]) -> bool:
        """Return True only for real file objects that should enter registry scan."""
        key = str(obj.get("Key") or "")
        if not key or key.endswith("/"):
            return False

        size = obj.get("Size")
        if isinstance(size, int) and size <= 0:
            return False
        return True

    @staticmethod
    def _extension_for_key(key: str) -> str:
        """Return normalized lowercase extension from an object key, or empty string."""
        filename = key.rsplit("/", 1)[-1]
        if "." not in filename:
            return ""
        return filename.rsplit(".", 1)[-1].lower()

    def head_object(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Return object metadata when it exists, otherwise None."""
        try:
            return self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def fetch_metadata(self, candidate: SourceCandidate) -> dict[str, Any]:
        """Fetch metadata for an existing object."""
        if not candidate.raw_bucket or not candidate.raw_key:
            raise ValueError("Contabo candidate requires raw_bucket and raw_key")
        return self.head_object(candidate.raw_bucket, candidate.raw_key) or {}

    def upload_stream(self, bucket: str, key: str, stream: bytes | BinaryIO) -> str:
        """Upload bytes or a binary stream to Contabo and return an S3 URI."""
        if isinstance(stream, (bytes, bytearray)):
            self.client.put_object(Bucket=bucket, Key=key, Body=stream)
        else:
            self.client.upload_fileobj(stream, bucket, key)
        return f"s3://{bucket}/{key}"

    def open_stream(self, candidate: SourceCandidate) -> Any:
        """Return the S3 body stream for a candidate."""
        if not candidate.raw_bucket or not candidate.raw_key:
            raise ValueError("Contabo candidate requires raw_bucket and raw_key")
        response = self.client.get_object(Bucket=candidate.raw_bucket, Key=candidate.raw_key)
        return response["Body"]
