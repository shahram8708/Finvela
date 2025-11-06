"""Configuration dataclasses for the ingestion subsystem."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from flask import Flask


@dataclass(frozen=True)
class EmailSettings:
    host: str
    username: str
    password: str
    use_ssl: bool
    folder: str
    poll_secs: int

    @property
    def enabled(self) -> bool:
        return all([self.host, self.username, self.password])


@dataclass(frozen=True)
class StorageSettings:
    backend: str
    bucket: str
    region: str
    aws_key: str
    aws_secret: str

    @property
    def is_s3(self) -> bool:
        return self.backend == "s3"


@dataclass(frozen=True)
class IngestSettings:
    watch_paths: tuple[str, ...]
    allowed_extensions: tuple[str, ...]
    allowed_mime_types: tuple[str, ...]
    max_file_mb: int
    email: EmailSettings
    storage: StorageSettings

    @classmethod
    def from_app(cls, app: Flask) -> "IngestSettings":
        extensions: Iterable[str] = app.config.get("UPLOAD_ALLOWED_EXTENSIONS", ())
        mime_types: Iterable[str] = app.config.get("UPLOAD_ALLOWED_MIME_TYPES", ())
        watch_paths = tuple(app.config.get("INGEST_WATCH_PATHS", ()) or ())
        email = EmailSettings(
            host=app.config.get("INGEST_EMAIL_HOST", ""),
            username=app.config.get("INGEST_EMAIL_USER", ""),
            password=app.config.get("INGEST_EMAIL_PASS", ""),
            use_ssl=bool(app.config.get("INGEST_EMAIL_SSL", True)),
            folder=app.config.get("INGEST_EMAIL_FOLDER", "INBOX"),
            poll_secs=int(app.config.get("INGEST_POLL_SECS", 10)),
        )
        storage = StorageSettings(
            backend=str(app.config.get("STORAGE_BACKEND", "local")).lower(),
            bucket=app.config.get("S3_BUCKET", ""),
            region=app.config.get("S3_REGION", ""),
            aws_key=app.config.get("AWS_ACCESS_KEY_ID", ""),
            aws_secret=app.config.get("AWS_SECRET_ACCESS_KEY", ""),
        )
        return cls(
            watch_paths=watch_paths,
            allowed_extensions=tuple(extensions),
            allowed_mime_types=tuple(mime_types),
            max_file_mb=int(app.config.get("INGEST_MAX_FILE_MB", 20)),
            email=email,
            storage=storage,
        )

    @property
    def max_bytes(self) -> int:
        return self.max_file_mb * 1024 * 1024


__all__ = ["IngestSettings", "EmailSettings", "StorageSettings"]
