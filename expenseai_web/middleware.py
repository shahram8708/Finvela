"""Request middleware helpers for logging and request correlation."""
from __future__ import annotations

import time
import uuid
from typing import Any

from flask import Flask, g, request

from expenseai_ext.logging import SLOW_THRESHOLD_MS, log_info, log_warn


def init_app(app: Flask) -> None:
    """Attach lifecycle hooks that enrich logs and responses."""

    @app.before_request
    def _capture_request_metadata() -> None:
        g.request_id = uuid.uuid4().hex
        g.request_started_at = time.perf_counter()
        g.log_context = {
            "route": request.path,
            "method": request.method,
            "ip": request.remote_addr,
            "ua": request.user_agent.string,
        }
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            _store_request_body_sample(app)

    @app.after_request
    def _inject_response_headers(response):  # type: ignore[override]
        duration = None
        if hasattr(g, "request_started_at"):
            duration = (time.perf_counter() - g.request_started_at) * 1000.0
        if duration is not None:
            g.request_latency_ms = int(duration)
            response.headers["X-Response-Time"] = f"{duration/1000:.4f}s"
        if hasattr(g, "request_id"):
            response.headers["X-Request-ID"] = g.request_id
        g.response_status_code = response.status_code
        if duration is not None and g.request_latency_ms > SLOW_THRESHOLD_MS:
            log_warn("slow request", latency_ms=g.request_latency_ms, status=response.status_code)
        log_info("request completed", status=response.status_code)
        return response

    @app.teardown_request
    def _clear_request_context(exc: Exception | None) -> None:
        g.pop("request_started_at", None)
        if exc:
            log_warn("request teardown due to exception", context={"error": str(exc)})


def _store_request_body_sample(app: Flask) -> None:
    max_chars = app.config.get("REQUEST_BODY_LOG_MAX", 2048)
    redact_keys = app.config.get("REDACT_KEYS", [])
    try:
        if request.mimetype and "multipart" in request.mimetype:
            sample: Any = "<multipart omitted>"
        elif request.is_json:
            json_payload = request.get_json(silent=True) or {}
            sample = _scrub_payload(json_payload, redact_keys)
        else:
            raw = request.get_data(cache=True, as_text=True)
            if not raw:
                return
            sample = raw[:max_chars]
            if len(raw) > max_chars:
                sample += "…"
        if sample is not None:
            g.log_context["request_body"] = sample
    except Exception:  # pragma: no cover - defensive logging
        log_warn("failed to capture request body", component="middleware")


def _scrub_payload(payload: Any, keys: list[str]) -> Any:
    if isinstance(payload, dict):
        lowered = {key.lower() for key in keys}
        result: dict[str, Any] = {}
        for key, value in payload.items():
            if key.lower() in lowered:
                result[key] = "***"
            else:
                result[key] = _scrub_payload(value, keys)
        return result
    if isinstance(payload, list):
        return [_scrub_payload(item, keys) for item in payload[:50]]
    return payload
