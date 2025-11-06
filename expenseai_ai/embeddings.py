"""Embedding utilities for normalized invoice item descriptions."""
from __future__ import annotations

import hashlib
import math
import time

from flask import current_app

from expenseai_ai import gemini_client, norm
from expenseai_ext.db import db
from expenseai_models.item_embedding import ItemEmbedding


def normalize_for_embedding(text: str) -> str:
    """Normalize text prior to embedding, ensuring deterministic hashing."""
    normalized = norm.normalize_description(text)
    if not normalized:
        # Fallback to raw trimmed text to avoid empty lookups downstream.
        normalized = (text or "").strip().lower()
    return normalized


def embed_text(text: str, *, force_remote: bool = False) -> list[float]:
    """Return a Finvela embedding vector for the supplied text."""
    normalized = normalize_for_embedding(text)
    if not normalized:
        raise ValueError("Cannot embed empty text")

    app = current_app._get_current_object()

    model_name = app.config.get("EMBEDDING_MODEL")
    if not model_name:
        model_name = app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite")
    timeout = app.config.get("GEMINI_REQUEST_TIMEOUT", 90)
    max_retries = app.config.get("GEMINI_MAX_RETRIES", 3)
    backoff = float(app.config.get("GEMINI_RETRY_BACKOFF_SECS", 2.0))
    fallback_dims = int(app.config.get("EMBEDDING_FALLBACK_DIMS", 64))
    if app.config.get("EMBEDDING_DISABLE_REMOTE", False) and not force_remote:
        app.logger.debug(
            "Remote embeddings disabled; using fallback",
            extra={"text_preview": (normalized[:120] + "…") if len(normalized) > 120 else normalized},
        )
        return _fallback_embedding(normalized, fallback_dims)

    truncated = (normalized[:120] + "…") if len(normalized) > 120 else normalized
    for attempt in range(1, max_retries + 1):
        try:
            response = gemini_client.embed_content(
                content=normalized,
                model_name=model_name,
                task_type="RETRIEVAL_QUERY",
                app=app,
                request_options={"timeout": timeout},
            )
            embedding = getattr(response, "embedding", None)
            if embedding is None and isinstance(response, dict):
                embedding = response.get("embedding") or response.get("embeddings") or response.get("data")
            if embedding is None and isinstance(response, list):
                embedding = response
            if embedding is None:
                app.logger.warning(
                    "Embedding response missing vector",
                    extra={"text_preview": truncated, "attempt": attempt},
                )
                return _fallback_embedding(normalized, fallback_dims)
            values = getattr(embedding, "values", None) or getattr(embedding, "value", None)
            if values is None and isinstance(embedding, dict):
                values = (
                    embedding.get("values")
                    or embedding.get("value")
                    or embedding.get("embedding")
                    or embedding.get("data")
                )
            if values is None and isinstance(embedding, list):
                values = embedding
            if isinstance(values, dict):
                values = (
                    values.get("values")
                    or values.get("value")
                    or values.get("embedding")
                    or values.get("data")
                )
            if isinstance(values, (int, float)):
                values = [float(values)]
            if values is None or not isinstance(values, (list, tuple)):
                app.logger.warning(
                    "Embedding response missing values list",
                    extra={"text_preview": truncated, "attempt": attempt},
                )
                return _fallback_embedding(normalized, fallback_dims)
            try:
                vector = [float(val) for val in values]
            except (TypeError, ValueError) as exc:
                app.logger.warning(
                    "Embedding values not numeric",
                    extra={"text_preview": truncated, "attempt": attempt, "error": str(exc)},
                )
                return _fallback_embedding(normalized, fallback_dims)
            if not vector:
                app.logger.warning(
                    "Embedding response returned empty vector",
                    extra={"text_preview": truncated, "attempt": attempt},
                )
                return _fallback_embedding(normalized, fallback_dims)
            app.logger.debug(
                "Generated embedding",
                extra={"text_preview": truncated, "length": len(vector)},
            )
            return vector
        except Exception as exc:  # pragma: no cover - relies on network
            should_retry = _is_retryable_embedding_error(exc)
            app.logger.warning(
                "Embedding request failed",
                extra={
                    "attempt": attempt,
                    "retry": should_retry and attempt < max_retries,
                    "error": str(exc),
                    "text_preview": truncated,
                },
            )
            if attempt >= max_retries or not should_retry:
                app.logger.warning(
                    "Falling back to local embedding",
                    extra={"text_preview": truncated},
                )
                return _fallback_embedding(normalized, fallback_dims)
            sleep_for = backoff * math.pow(2, attempt - 1)
            time.sleep(sleep_for)
    app.logger.warning(
        "Embedding retries exhausted; using fallback embedding",
        extra={"text_preview": truncated},
    )
    return _fallback_embedding(normalized, fallback_dims)


def text_hash(text_norm: str) -> str:
    """Return a deterministic SHA256 hash for the normalized text."""
    digest = hashlib.sha256(text_norm.encode("utf-8")).hexdigest()
    return digest


def get_or_create_item_embedding(text: str) -> ItemEmbedding:
    """Fetch a cached embedding or create a new one for the text."""
    normalized = normalize_for_embedding(text)
    if not normalized:
        raise ValueError("Cannot embed empty text")

    key = text_hash(normalized)
    record = ItemEmbedding.query.filter_by(hash_key=key).first()
    if record:
        return record

    vector = embed_text(normalized)
    record = ItemEmbedding(hash_key=key, text_norm=normalized, vector=vector)
    db.session.add(record)
    db.session.flush()
    return record


def _is_retryable_embedding_error(exc: Exception) -> bool:
    """Heuristic to determine whether an embedding error warrants retry."""
    message = str(exc).lower()
    transient_tokens = ("timeout", "temporar", "unavailable", "quota", "rate", "exhaust")
    return any(token in message for token in transient_tokens)


def _fallback_embedding(text: str, dimensions: int) -> list[float]:
    """Return a deterministic embedding using locality-sensitive hashing as fallback."""
    dims = max(16, dimensions)
    vector = [0.0] * dims
    if not text:
        return vector

    tokens = [token for token in text.split() if token]
    if not tokens:
        tokens = [text]

    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        for offset in range(0, len(digest), 4):
            chunk = int.from_bytes(digest[offset : offset + 4], "big", signed=False)
            idx = chunk % dims
            magnitude = ((chunk >> 8) & 0xFFFF) / 65535.0  # scale to [0,1]
            sign = 1.0 if (chunk & 1) == 0 else -1.0
            vector[idx] += sign * (0.5 + magnitude)

    norm = math.sqrt(sum(val * val for val in vector))
    if norm > 0:
        vector = [val / norm for val in vector]
    return vector


__all__ = [
    "embed_text",
    "text_hash",
    "get_or_create_item_embedding",
    "normalize_for_embedding",
]
