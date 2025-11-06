"""Application-wide error handling and typed exceptions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple
import math

from flask import Flask, Response, current_app, g, jsonify, make_response, render_template, request
from werkzeug.exceptions import HTTPException

_ERROR_TEMPLATES = {
    400: "errors/400.html",
    401: "errors/401.html",
    403: "errors/403.html",
    404: "errors/404.html",
    409: "errors/409.html",
    429: "errors/429.html",
    500: "errors/500.html",
    503: "errors/503.html",
    504: "errors/504.html",
}


@dataclass(eq=False)
class AppError(Exception):
    """Base exception carrying structured context."""

    user_msg: str
    code: str = "APP_ERROR"
    http_status: int = 500
    detail: str | None = None
    safe_context: Dict[str, Any] | None = None

    def payload(self, *, include_detail: bool = False) -> Dict[str, Any]:
        request_id = getattr(g, "request_id", None)
        data: Dict[str, Any] = {
            "type": self.__class__.__name__,
            "message": self.user_msg,
            "code": self.code,
            "request_id": request_id,
        }
        if include_detail and self.detail:
            data["detail"] = self.detail
        if self.safe_context:
            data["context"] = _redact_dict(self.safe_context, current_app.config.get("REDACT_KEYS", []))
        return {"error": data}

    def to_response(self) -> Tuple[Dict[str, Any], int]:
        include_detail = current_app.debug or current_app.testing
        return self.payload(include_detail=include_detail), self.http_status


class ValidationError(AppError):
    code = "VALIDATION"
    http_status = 400


class AuthError(AppError):
    code = "AUTH"
    http_status = 401


class RateLimitError(AppError):
    code = "RATE_LIMIT"
    http_status = 429


class ConflictError(AppError):
    code = "CONFLICT"
    http_status = 409


class UpstreamError(AppError):
    code = "UPSTREAM"
    http_status = 503


class TimeoutError(AppError):
    code = "TIMEOUT"
    http_status = 504


class NotFoundError(AppError):
    code = "NOT_FOUND"
    http_status = 404


def _redact_dict(data: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
    lowered = {key.lower() for key in keys}
    redacted: Dict[str, Any] = {}
    for key, value in data.items():
        if key.lower() in lowered:
            redacted[key] = "***"
        elif isinstance(value, dict):
            redacted[key] = _redact_dict(value, lowered)
        else:
            redacted[key] = value
    return redacted


def init_app(app: Flask) -> None:
    """Attach global error handlers to the Flask application."""

    for exc in (AppError, ValidationError, AuthError, RateLimitError, ConflictError, UpstreamError, TimeoutError, NotFoundError):
        app.register_error_handler(exc, _handle_app_error)

    app.register_error_handler(HTTPException, _handle_http_exception)
    app.register_error_handler(Exception, _handle_unexpected)


def _handle_app_error(error: AppError):
    return _format_error(error)


def _handle_http_exception(error: HTTPException):
    payload = error.description if isinstance(error.description, dict) else {"message": error.description}
    app_error = AppError(
        user_msg=payload.get("message", "HTTP error"),
        code=error.name.upper().replace(" ", "_"),
        http_status=error.code or 500,
        detail=None,
        safe_context=payload if isinstance(payload, dict) else None,
    )
    return _format_error(app_error)


def _handle_unexpected(error: Exception):
    current_app.logger.exception("Unhandled exception", exc_info=error)
    app_error = AppError(user_msg="An unexpected error occurred.", code="INTERNAL", http_status=500, detail=str(error) if current_app.debug else None)
    return _format_error(app_error)


def _format_error(error: AppError):
    payload, status = error.to_response()
    if request.path.startswith("/api") or request.accept_mimetypes.best == "application/json" or request.is_json:
        resp = jsonify(payload)
        resp.status_code = status
    else:
        template = _ERROR_TEMPLATES.get(status, "errors/500.html")
        resp = make_response(render_template(template, error=payload["error"]), status)
    return _attach_request_id(resp, status)


def _attach_request_id(response: Response, status: int | None = None):
    request_id = getattr(g, "request_id", None)
    if request_id:
        response.headers["X-Request-ID"] = request_id
    status_code = status or getattr(response, "status_code", 500)
    if status_code in {429, 503, 504} and "Retry-After" not in response.headers:
        backoff = current_app.config.get("BACKOFF_BASE_SECS", 1.5)
        try:
            retry_after = max(1, int(math.ceil(float(backoff))))
        except (TypeError, ValueError):
            retry_after = 1
        response.headers["Retry-After"] = str(retry_after)
    return response
