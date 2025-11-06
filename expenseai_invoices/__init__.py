"""Invoice blueprint responsible for uploads and browsing."""
from __future__ import annotations

from flask import Blueprint

invoices_bp = Blueprint(
    "expenseai_invoices",
    __name__,
    template_folder="../expenseai_web/templates",
)

from expenseai_invoices import routes  # noqa: E402,F401
