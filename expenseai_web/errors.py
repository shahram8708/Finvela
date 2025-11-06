"""Centralized error handling for HTTP errors and exceptions."""
from __future__ import annotations

from typing import Any, Tuple

from flask import Flask, jsonify, render_template, request

ERROR_TEMPLATES = {
    400: "errors/400.html",
    401: "errors/401.html",
    403: "errors/403.html",
    404: "errors/404.html",
    429: "errors/429.html",
    500: "errors/500.html",
}


def register_error_handlers(app: Flask) -> None:
    """Attach handlers for user-friendly HTML and JSON error responses."""

    def _render_error(status: int, error: Exception | None = None):
        template = ERROR_TEMPLATES.get(status, "errors/500.html")
        payload: dict[str, Any] = {
            "code": status,
            "message": getattr(error, "description", str(error) if error else ""),
        }
        if request.path.startswith("/api"):
            return jsonify(payload), status
        return render_template(template, error=payload), status

    for code in ERROR_TEMPLATES:
        app.errorhandler(code)(_render_error)

    @app.errorhandler(Exception)
    def _handle_exception(error: Exception):  # type: ignore[override]
        app.logger.exception("Unhandled exception", exc_info=error)
        return _render_error(500, error)
