"""Counterfactual what-if analysis blueprint."""
from __future__ import annotations

from flask import Blueprint

counterfactual_bp = Blueprint(
    "expenseai_counterfactual",
    __name__,
    url_prefix="/invoices",
)

from expenseai_counterfactual import routes  # noqa: E402,F401
