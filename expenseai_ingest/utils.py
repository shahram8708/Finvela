"""Helper utilities shared across ingestion components."""
from __future__ import annotations

import base64
import hashlib
import mimetypes
from pathlib import Path
from typing import Iterable

import filetype


def normalize_extension(filename: str) -> str:
    return Path(filename).suffix.lower().lstrip(".")


def validate_extension(filename: str, allowed: Iterable[str]) -> None:
    ext = normalize_extension(filename)
    if ext not in set(allowed):
        raise ValueError(f"Unsupported file extension: {ext or '<none>'}")


def detect_mime(data: bytes, fallback: str | None = None) -> str:
    guess = filetype.guess(data)
    if guess is not None:
        return guess.mime
    if fallback:
        return fallback
    return "application/octet-stream"


def enforce_mime(mime: str, allowed: Iterable[str]) -> None:
    if mime not in set(allowed):
        raise ValueError(f"Unsupported MIME type: {mime}")


def guess_mime_from_name(filename: str) -> str | None:
    mime, _ = mimetypes.guess_type(filename)
    return mime


def compute_sha256(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def encode_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def decode_bytes(data_b64: str) -> bytes:
    return base64.b64decode(data_b64.encode("ascii"))
