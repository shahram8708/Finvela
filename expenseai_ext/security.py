"""Security-related extensions such as CSRF, headers and rate limiting."""
from __future__ import annotations

from typing import Callable

from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_talisman import Talisman
from flask_wtf import CSRFProtect

csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address, storage_uri="memory://")
talisman = Talisman()


def init_app(app: Flask) -> None:
    """Register security extensions against the Flask application."""
    csrf.init_app(app)

    limiter.init_app(app)
    limiter.default_limits = [app.config.get("RATELIMIT_DEFAULT", "100 per minute")]
    global_limit = app.config.get("GLOBAL_RATE_LIMIT")
    if global_limit:
        limiter.application_limits = [global_limit]
    limiter.headers_enabled = True

    if app.config.get("SECURITY_HEADERS", True):
        talisman.init_app(
            app,
            content_security_policy=app.config.get("CONTENT_SECURITY_POLICY"),
            feature_policy=None,
            force_https=app.config.get("SESSION_COOKIE_SECURE", False),
            session_cookie_secure=app.config.get("SESSION_COOKIE_SECURE", False),
            referrer_policy="strict-origin-when-cross-origin",
        )


def user_or_ip_rate_limit() -> Callable[[], str]:
    """Limiter key that prefers the authenticated user id when available."""

    def _key_func() -> str:
        from flask_login import current_user  # Lazy import to avoid cycles.

        if current_user.is_authenticated:  # type: ignore[attr-defined]
            return f"user:{current_user.get_id()}"  # type: ignore[no-any-return]
        return get_remote_address()

    return _key_func
