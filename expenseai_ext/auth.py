"""Authentication helpers and Flask-Login integration."""
from __future__ import annotations

from functools import wraps
from typing import Callable

from flask import Flask, abort
from flask_login import LoginManager, current_user

login_manager = LoginManager()
login_manager.session_protection = "strong"


def init_app(app: Flask) -> None:
    """Configure Flask-Login for the application."""
    login_manager.login_view = "expenseai_auth.login"
    login_manager.login_message_category = "warning"
    login_manager.refresh_view = "expenseai_auth.login"
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id: str):  # type: ignore[override]
        from expenseai_ext.db import db
        from expenseai_models.user import User

        return db.session.get(User, int(user_id))


def roles_required(*roles: str) -> Callable:
    """Decorator enforcing that the current user owns at least one role."""

    def decorator(view: Callable) -> Callable:
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                return login_manager.unauthorized()
            if not current_user.has_any_role(roles):
                abort(403)
            return view(*args, **kwargs)

        return wrapped

    return decorator
