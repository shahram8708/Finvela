"""Pluggable storage adapters for raw invoice uploads."""
from __future__ import annotations

import io
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, current_app

from expenseai_ingest.config import IngestSettings
from expenseai_ingest import utils


class StorageError(RuntimeError):
    """Raised when a storage backend fails to persist an artifact."""


@dataclass(frozen=True)
class StorageResult:
    stored_filename: str
    original_filename: str
    mime_type: str
    filesize_bytes: int
    source_path: str
    checksum_sha256: str
    backend: str
    uri: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "stored_filename": self.stored_filename,
            "original_filename": self.original_filename,
            "mime_type": self.mime_type,
            "filesize_bytes": self.filesize_bytes,
            "source_path": self.source_path,
            "checksum_sha256": self.checksum_sha256,
            "backend": self.backend,
            "uri": self.uri,
        }


class StorageBackend:
    def store_bytes(self, *, data: bytes, original_name: str, mime_type: str) -> StorageResult:  # pragma: no cover - interface only
        raise NotImplementedError

    def store_path(self, *, path: Path, original_name: Optional[str] = None, mime_type: Optional[str] = None) -> StorageResult:  # pragma: no cover - interface only
        raise NotImplementedError


class LocalStorageBackend(StorageBackend):
    def __init__(self, app: Flask):
        self.app = app
        self.root = Path(app.instance_path) / app.config["UPLOAD_STORAGE_DIR"]
        self.root.mkdir(parents=True, exist_ok=True)

    def _destination(self, original_name: str) -> tuple[str, Path]:
        timestamp = datetime.utcnow()
        ext = Path(original_name).suffix or ".dat"
        relative = Path(str(timestamp.year), f"{timestamp.month:02d}", f"{uuid4().hex}{ext}")
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        return str(relative).replace("\\", "/"), destination

    def store_bytes(self, *, data: bytes, original_name: str, mime_type: str) -> StorageResult:
        relative_key, destination = self._destination(original_name)
        destination.write_bytes(data)
        checksum = utils.compute_sha256(data)
        return StorageResult(
            stored_filename=relative_key,
            original_filename=original_name,
            mime_type=mime_type,
            filesize_bytes=len(data),
            source_path=relative_key,
            checksum_sha256=checksum,
            backend="local",
            uri=str(destination),
        )

    def store_path(self, *, path: Path, original_name: Optional[str] = None, mime_type: Optional[str] = None) -> StorageResult:
        original = original_name or path.name
        relative_key, destination = self._destination(original)
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as src, destination.open("wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        return StorageResult(
            stored_filename=relative_key,
            original_filename=original,
            mime_type=mime_type or utils.guess_mime_from_name(original) or "application/octet-stream",
            filesize_bytes=size,
            source_path=relative_key,
                checksum_sha256=digest.hexdigest(),
            backend="local",
            uri=str(destination),
        )


class S3StorageBackend(StorageBackend):
    def __init__(self, app: Flask, settings: IngestSettings):
        if not settings.storage.bucket:
            raise StorageError("S3 bucket not configured")
        session = boto3.session.Session(
            aws_access_key_id=settings.storage.aws_key or None,
            aws_secret_access_key=settings.storage.aws_secret or None,
            region_name=settings.storage.region or None,
        )
        self.client = session.client("s3")
        self.bucket = settings.storage.bucket
        self.prefix = app.config.get("UPLOAD_STORAGE_DIR", "uploads").strip("/")

    def _object_key(self, original_name: str) -> str:
        timestamp = datetime.utcnow()
        ext = Path(original_name).suffix or ".dat"
        key = "/".join([
            self.prefix,
            str(timestamp.year),
            f"{timestamp.month:02d}",
            f"{uuid4().hex}{ext}",
        ])
        return key

    def store_bytes(self, *, data: bytes, original_name: str, mime_type: str) -> StorageResult:
        key = self._object_key(original_name)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=io.BytesIO(data),
                ContentType=mime_type,
            )
        except (ClientError, BotoCoreError) as exc:  # pragma: no cover - requires AWS
            raise StorageError(f"Failed to upload to S3: {exc}") from exc
        checksum = utils.compute_sha256(data)
        return StorageResult(
            stored_filename=key,
            original_filename=original_name,
            mime_type=mime_type,
            filesize_bytes=len(data),
            source_path=key,
            checksum_sha256=checksum,
            backend="s3",
            uri=f"s3://{self.bucket}/{key}",
        )

    def store_path(self, *, path: Path, original_name: Optional[str] = None, mime_type: Optional[str] = None) -> StorageResult:
        data = path.read_bytes()
        return self.store_bytes(
            data=data,
            original_name=original_name or path.name,
            mime_type=mime_type or utils.guess_mime_from_name(path.name) or "application/octet-stream",
        )


def get_storage(app: Flask | None = None) -> StorageBackend:
    app = app or current_app
    settings = IngestSettings.from_app(app)
    cache = app.extensions.setdefault("expenseai_ingest", {})
    backend = cache.get("storage_backend")
    if backend:
        return backend
    if settings.storage.backend == "s3":
        backend = S3StorageBackend(app, settings)
    else:
        backend = LocalStorageBackend(app)
    cache["storage_backend"] = backend
    return backend


__all__ = ["StorageBackend", "StorageResult", "StorageError", "get_storage"]
