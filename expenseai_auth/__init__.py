"""Authentication blueprint registration."""
from __future__ import annotations

from flask import Blueprint

auth_bp = Blueprint(
    "expenseai_auth",
    __name__,
    template_folder="../expenseai_web/templates",
)

from expenseai_auth import routes  # noqa: E402,F401
