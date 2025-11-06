"""Compliance package aggregation and blueprint registration."""
from __future__ import annotations

from flask import Blueprint

compliance_admin_bp = Blueprint(
    "expenseai_compliance_admin",
    __name__,
    template_folder="templates",
    static_folder=None,
    url_prefix="/admin",
)

# Import routes to register handlers.
from expenseai_compliance import routes  # noqa: E402,F401

__all__ = ["compliance_admin_bp"]
