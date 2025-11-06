"""Structured logging helpers used by the Flask application."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict

from flask import Flask, current_app, g, has_request_context, request
from flask_login import current_user


SLOW_THRESHOLD_MS = 1000


class StructuredFormatter(logging.Formatter):
    """Formatter that emits JSON or plain logs with shared context."""

    def __init__(self, as_json: bool = True) -> None:
        super().__init__()
        self.as_json = as_json

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload = _record_payload(record)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if self.as_json:
            return json.dumps(payload, ensure_ascii=True)
        return _format_plain(payload)


def _format_plain(payload: Dict[str, Any]) -> str:
    parts = [f"[{payload['level']}]", payload.get("msg", "").strip()]
    route = payload.get("route")
    if route:
        parts.append(f"route={route}")
    method = payload.get("method")
    if method:
        parts.append(f"method={method}")
    status = payload.get("status")
    if status is not None:
        parts.append(f"status={status}")
    latency = payload.get("latency_ms")
    if latency is not None:
        parts.append(f"latency_ms={latency}")
    component = payload.get("component")
    if component:
        parts.append(f"component={component}")
    return " ".join(parts)


def _record_payload(record: logging.LogRecord) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": record.levelname,
        "msg": record.getMessage(),
        "component": getattr(record, "component", "app"),
        "invoice_id": getattr(record, "invoice_id", None),
    }
    if has_request_context():
        payload["request_id"] = getattr(g, "request_id", None)
        payload["route"] = request.path
        payload["method"] = request.method
        payload["ip"] = request.remote_addr
        payload["ua"] = request.user_agent.string
        if getattr(current_user, "is_authenticated", False):
            try:
                payload["user_id"] = current_user.get_id()
            except Exception:  # pragma: no cover - defensive
                payload["user_id"] = None
        payload.update(getattr(g, "log_context", {}))
        latency_ms = getattr(g, "request_latency_ms", None)
        if latency_ms is not None:
            payload["latency_ms"] = latency_ms
        status = getattr(g, "response_status_code", None)
        if status is not None:
            payload["status"] = status
    for attr in ("latency_ms", "status"):
        value = getattr(record, attr, None)
        if value is not None:
            payload[attr] = value
    context = getattr(record, "context", None)
    if context:
        payload.setdefault("context", {}).update(context)
    return {k: v for k, v in payload.items() if v is not None}


def configure_logging(app: Flask) -> None:
    """Configure application logging based on the current environment."""

    level_name = app.config.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    app.logger.setLevel(level)

    use_json = app.config.get("LOG_FORMAT", "json").lower() == "json"
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter(as_json=use_json))

    app.logger.handlers.clear()
    app.logger.addHandler(handler)


def log_info(message: str, *, component: str = "app", **extra: Any) -> None:
    current_app.logger.info(message, extra=_prepare_extra(component, extra))


def log_warn(message: str, *, component: str = "app", **extra: Any) -> None:
    current_app.logger.warning(message, extra=_prepare_extra(component, extra))


def log_error(message: str, *, component: str = "app", **extra: Any) -> None:
    current_app.logger.error(message, extra=_prepare_extra(component, extra))


def _prepare_extra(component: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(extra)
    data.setdefault("component", component)
    if "context" in data and not isinstance(data["context"], dict):
        data["context"] = {"value": data["context"]}
    return data
