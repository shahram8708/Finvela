"""Public web blueprint and related registration helpers."""
from __future__ import annotations

from flask import Blueprint

web_bp = Blueprint(
    "expenseai_web",
    __name__,
    template_folder="templates",
    static_folder="static",
)

# Import views after blueprint creation to avoid circular imports.
from expenseai_web import chat, routes  # noqa: E402,F401
