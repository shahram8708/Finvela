"""Contextual bandit package wiring blueprints and background trainer."""
from __future__ import annotations

from flask import Blueprint

bandit_bp = Blueprint("expenseai_bandit", __name__, url_prefix="/admin")

from expenseai_bandit import routes  # noqa: E402,F401
