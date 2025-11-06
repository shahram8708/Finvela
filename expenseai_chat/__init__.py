"""Organization chat blueprint initialization."""
from __future__ import annotations

from flask import Blueprint

chat_bp = Blueprint(
    "expenseai_chat",
    __name__,
    template_folder="templates",
    static_folder="static",
)

# Ensure models and routes are discoverable when the blueprint is imported.
from expenseai_chat import models, views  # noqa: E402,F401
