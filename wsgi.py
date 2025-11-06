"""WSGI entrypoint exposing the Flask application for production servers."""
from __future__ import annotations

from expenseai_ext import create_app

# Gunicorn/Waitress (or similar) imports this module and uses the `application`
# object per WSGI convention.
application = create_app()
