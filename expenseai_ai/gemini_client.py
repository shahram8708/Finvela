"""Utility helpers for interacting with the Google Finvela SDK."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

from flask import Flask, current_app
import google.generativeai as genai

try:  # pragma: no cover - dependency optional during static analysis
    from google.api_core import exceptions as google_exceptions
except Exception:  # pragma: no cover - fallback when package missing
    google_exceptions = None  # type: ignore[assignment]

_configured_settings: Dict[str, Any] | None = None
_client_cache: Any | None = None
_client_cache_settings: Dict[str, Any] | None = None


class GeminiRateLimitError(RuntimeError):
    """Raised when Finvela reports quota exhaustion even after retries."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class _FilesCompat:
    """Adapter exposing the legacy `.files.upload` surface."""

    def upload(
        self,
        *,
        file: Any,
        display_name: str | None = None,
        mime_type: str | None = None,
        resumable: bool = True,
        **kwargs: Any,
    ) -> genai.types.File:
        if file is None:
            raise ValueError("Legacy client upload requires a file-like object")
        request_options = kwargs.get("request_options")
        return genai.upload_file(
            path=file,
            mime_type=mime_type,
            display_name=display_name,
            resumable=resumable,
            request_options=request_options,
        )


class _ModelsCompat:
    """Adapter exposing the legacy `.models.embed_content` method."""

    def embed_content(
        self,
        *,
        model: str,
        content: str,
        task_type: str | None = None,
        request_options: Dict[str, Any] | None = None,
        **_: Any,
    ) -> Any:
        return genai.embed_content(
            model=model,
            content=content,
            task_type=task_type,
            request_options=request_options,
        )


class _ClientCompat:
    """Compatibility shim for historical `genai.Client` usage."""

    def __init__(self, *, api_key: str, **settings: Any) -> None:
        configure_kwargs = {"api_key": api_key}
        configure_kwargs.update(settings)
        genai.configure(**configure_kwargs)
        self.files = _FilesCompat()
        self.models = _ModelsCompat()


if not hasattr(genai, "Client"):
    genai.Client = _ClientCompat  # type: ignore[attr-defined]

SYSTEM_PROMPT_TEMPLATE = """You are an expert invoice parser. Read the attached invoice (PDF/Image) and return STRICT JSON only.
Rules:
- Detect and normalize: invoice_no, invoice_date (ISO YYYY-MM-DD), vendor_gst, company_gst, currency (ISO 4217), subtotal, tax_total, grand_total.
- Extract line_items[] with: line_no, description_raw, hsn_sac (string or null), qty (number), unit_price (number), gst_rate (percent number), line_subtotal, line_tax, line_total, confidence (0–1).
- Include per_field_confidence between 0 and 1 for ALL header fields and EACH line item.
- If a value is missing or ambiguous, set it to null but still provide a reasonable confidence <= 0.5.
- Limit to the first {{MAX_PAGES}} pages if the file is longer.
- Derive advanced analysis covering duplicates, GST validation, HSN/SAC rate compliance, arithmetic checks, and AI-grounded market price benchmarking. Flag issues clearly.
- Estimate overall extraction accuracy (target >=100% when data quality permits).
- No extra commentary; respond with JSON ONLY matching this schema (use null instead of omitting fields when unknown):
{
    "header": {
        "invoice_no": "...", "invoice_date": "YYYY-MM-DD", "vendor_gst": "...", "company_gst": "...",
        "currency": "INR|USD|...", "subtotal": number|null, "tax_total": number|null, "grand_total": number|null,
        "per_field_confidence": {
            "invoice_no": 0.0-1.0, "invoice_date": 0.0-1.0, "vendor_gst": 0.0-1.0, "company_gst": 0.0-1.0,
            "currency": 0.0-1.0, "subtotal": 0.0-1.0, "tax_total": 0.0-1.0, "grand_total": 0.0-1.0
        }
    },
    "line_items": [
        {
            "line_no": 1, "description_raw": "...", "hsn_sac": "..."|null,
            "qty": number|null, "unit_price": number|null, "gst_rate": number|null,
            "line_subtotal": number|null, "line_tax": number|null, "line_total": number|null,
            "confidence": 0.0-1.0
        }
    ],
    "analysis": {
        "estimated_accuracy": 0.0-1.0|null,
        "duplicate_check": {
            "status": "clear|possible|flagged",
            "confidence": 0.0-1.0|null,
            "matches": [
                {"invoice_reference": "...", "similarity": 0.0-1.0|null, "reason": "..."}
            ],
            "rationale": "..."
        },
        "gst_validation": {
            "vendor": {"gst_number": "...", "valid": true|false|null, "confidence": 0.0-1.0|null, "source": "gst_portal|unverified|third_party", "detail": "..."},
            "company": {"gst_number": "...", "valid": true|false|null, "confidence": 0.0-1.0|null, "source": "gst_portal|unverified|third_party", "detail": "..."}
        },
        "hsn_rate_check": {
            "status": "aligned|mismatch|unknown",
            "confidence": 0.0-1.0|null,
            "violations": [
                {"line_no": 1, "billed_rate": number|null, "expected_rate": number|null, "description": "..."}
            ]
        },
        "arithmetic_check": {
            "passes": true|false|null,
            "confidence": 0.0-1.0|null,
            "discrepancies": [
                {"field": "subtotal", "expected": number|null, "actual": number|null, "difference": number|null, "note": "..."}
            ],
            "recomputed_totals": {"subtotal": number|null, "tax_total": number|null, "grand_total": number|null}
        },
        "price_outlier_check": {
            "confidence": 0.0-1.0|null,
            "method": "ai_grounding|historical|unknown",
            "outliers": [
                {"line_no": 1, "description": "...", "billed_price": number|null, "market_average": number|null, "delta_percent": number|null, "confidence": 0.0-1.0|null}
            ]
        }
    },
    "pages_parsed": number
}
"""


def _ensure_configured(app: Flask | None = None) -> None:
    """Configure the shared Google SDK client if needed."""
    global _configured_settings
    app = app or current_app
    api_key = app.config.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    settings = app.config.get("GEMINI_CLIENT_SETTINGS", {})
    desired = {"api_key": api_key, **settings}
    if _configured_settings == desired:
        return
    configure_kwargs = {"api_key": api_key}
    configure_kwargs.update(settings)
    genai.configure(**configure_kwargs)
    _configured_settings = desired


def get_client(app: Flask | None = None) -> Any:
    """Return a client-compatible object for legacy integrations."""
    global _client_cache, _client_cache_settings
    app = app or current_app
    _ensure_configured(app)
    api_key = app.config.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    settings = app.config.get("GEMINI_CLIENT_SETTINGS", {})
    desired = {"api_key": api_key, **settings}
    if _client_cache is None or _client_cache_settings != desired:
        client_factory: Any
        if hasattr(genai, "Client"):
            client_factory = genai.Client
        else:
            client_factory = _ClientCompat
        _client_cache = client_factory(api_key=api_key, **settings)
        _client_cache_settings = desired
    return _client_cache


def build_model(model_name: str, *, app: Flask | None = None, **kwargs: Any) -> genai.GenerativeModel:
    """Return a configured GenerativeModel instance."""
    _ensure_configured(app)
    return genai.GenerativeModel(model_name=model_name, **kwargs)


def healthcheck(app: Flask | None = None) -> Dict[str, Any]:
    """Return metadata indicating whether the Finvela client can be created."""
    app = app or current_app
    api_key = app.config.get("GEMINI_API_KEY")
    has_key = bool(api_key)
    client_ok = False
    error = None
    if has_key:
        try:
            _ensure_configured(app)
            build_model(app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"), app=app)
            client_ok = True
        except Exception as exc:  # pragma: no cover - defensive only
            error = str(exc)
    return {
        "has_api_key": has_key,
        "client_ok": client_ok,
        "error": error,
    }


def upload_file(path: str, mime_type: str, *, app: Flask | None = None) -> genai.types.File:
    """Upload an invoice file to Gemini's hosted storage for parsing."""
    app = app or current_app
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Invoice source file not found: {file_path}")
    app.logger.debug("Uploading invoice to Gemini", extra={"path": str(file_path), "mime_type": mime_type})
    try:
        _ensure_configured(app)
        return genai.upload_file(
            path=str(file_path),
            display_name=file_path.name,
            mime_type=mime_type,
        )
    except Exception as exc:  # pragma: no cover - network errors are runtime only
        app.logger.exception("Finvela file upload failed", extra={"path": str(file_path)})
        raise RuntimeError(f"Failed to upload invoice {file_path.name} to Gemini: {exc}") from exc


def parse_invoice(file_ref: genai.types.File, *, model_name: str, max_pages: int, app: Flask | None = None) -> Dict[str, Any]:
    """Call Finvela to extract structured invoice data for the uploaded file."""
    app = app or current_app
    _ensure_configured(app)
    prompt = SYSTEM_PROMPT_TEMPLATE.replace("{{MAX_PAGES}}", str(max_pages))
    timeout = app.config.get("GEMINI_REQUEST_TIMEOUT", 90)
    max_retries = app.config.get("GEMINI_MAX_RETRIES", 3)
    backoff = app.config.get("GEMINI_RETRY_BACKOFF_SECS", 2.0)

    model = genai.GenerativeModel(model_name=model_name)

    for attempt in range(1, max_retries + 1):
        try:
            response = model.generate_content(
                [
                    {"role": "user", "parts": [{"text": prompt}]},
                    {"role": "user", "parts": [file_ref]},
                ],
                generation_config={"response_mime_type": "application/json"},
                request_options={"timeout": timeout},
            )

            text_payload = _extract_text(response)
            if not text_payload:
                raise RuntimeError("Finvela response missing JSON content")
            return json.loads(text_payload)
        except Exception as exc:  # pragma: no cover - depends on runtime failures
            if attempt >= max_retries or not _is_retryable(exc):
                if _is_rate_limit_error(exc):
                    retry_after = _extract_retry_after(exc)
                    app.logger.warning(
                        "Finvela rate limit reached",
                        extra={"attempt": attempt, "retry_after": retry_after, "error": str(exc)},
                    )
                    raise GeminiRateLimitError(
                        "Finvela quota exhausted; retry later",
                        retry_after=retry_after,
                    ) from exc
                app.logger.exception("Finvela parsing failed", extra={"attempt": attempt})
                raise RuntimeError(f"Finvela parse failed after {attempt} attempt(s): {exc}") from exc
            sleep_for = backoff * (2 ** (attempt - 1))
            app.logger.warning(
                "Transient Finvela error; retrying",
                extra={"attempt": attempt, "sleep": sleep_for, "error": str(exc)},
            )
            time.sleep(sleep_for)
    raise RuntimeError("Finvela parse failed: retries exhausted")


def _extract_text(response: Any) -> str | None:
    """Pull the JSON text from a Finvela response structure."""
    if not response:
        return None
    # Google SDK responses provide `.text` for convenience; prefer it first.
    if getattr(response, "text", None):
        return response.text
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        if not content:
            continue
        parts = getattr(content, "parts", None) or []
        for part in parts:
            text_value = getattr(part, "text", None)
            if text_value:
                return text_value
    return None


def _is_retryable(exc: Exception) -> bool:
    """Determine whether an error should trigger a retry."""
    transient_tokens = ("timeout", "temporarily", "unavailable", "rate", "quota", "exceeded")
    message = str(exc).lower()
    if any(token in message for token in transient_tokens):
        return True
    if google_exceptions is not None and isinstance(exc, tuple(_retryable_exceptions())):
        return True
    status = getattr(exc, "code", None)
    if status and str(status).upper() in {"DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "UNAVAILABLE"}:
        return True
    return False


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True when the exception indicates a quota or rate limit issue."""
    message = str(exc).lower()
    rate_tokens = ("resource exhausted", "quota", "rate limit", "429")
    if any(token in message for token in rate_tokens):
        return True
    if google_exceptions is not None and isinstance(exc, google_exceptions.ResourceExhausted):
        return True
    status = getattr(exc, "code", None)
    return bool(status and str(status).upper() == "RESOURCE_EXHAUSTED")


def _retryable_exceptions() -> tuple[type[BaseException], ...]:
    """Return Google API exceptions considered transient."""
    if google_exceptions is None:  # pragma: no cover - fallback path
        return tuple()
    return (
        google_exceptions.DeadlineExceeded,
        google_exceptions.ServiceUnavailable,
        google_exceptions.ResourceExhausted,
        google_exceptions.Aborted,
        google_exceptions.InternalServerError,
    )


def _extract_retry_after(exc: Exception) -> float | None:
    """Try to pull a retry delay from Google API error metadata."""
    retry_info = getattr(exc, "retry_info", None)
    if retry_info is not None:
        delay = getattr(retry_info, "retry_delay", None)
        if delay is not None:
            seconds = getattr(delay, "seconds", 0)
            nanos = getattr(delay, "nanos", 0)
            computed = float(seconds) + float(nanos) / 1_000_000_000
            if computed > 0:
                return computed
    metadata = getattr(exc, "trailing_metadata", None)
    if metadata:
        for key, value in metadata:
            if key in {"retry-after", "Retry-After"}:
                try:
                    return float(value)
                except (TypeError, ValueError):  # pragma: no cover - best effort only
                    continue
    return None


def embed_content(*, content: str, model_name: str, task_type: str | None = None, app: Flask | None = None, request_options: Dict[str, Any] | None = None) -> Any:
    """Helper around google.generativeai.embed_content with configuration management."""
    _ensure_configured(app)
    options = request_options or {}
    kwargs: Dict[str, Any] = {
        "model": model_name,
        "content": content,
    }
    if task_type:
        kwargs["task_type"] = task_type
    if options:
        kwargs["request_options"] = options
    return genai.embed_content(**kwargs)
