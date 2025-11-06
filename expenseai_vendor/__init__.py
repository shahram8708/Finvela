"""Vendor fingerprinting blueprint."""
from __future__ import annotations

from flask import Blueprint

vendor_bp = Blueprint("expenseai_vendor", __name__, url_prefix="/vendors")

from expenseai_vendor import routes  # noqa: E402,F401
