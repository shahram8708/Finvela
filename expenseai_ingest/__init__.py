"""Bootstrap utilities for the ingestion subsystem."""
from __future__ import annotations

from flask import Flask

from expenseai_ingest.config import IngestSettings
from expenseai_ingest.emailer import start_email_poller
from expenseai_ingest.routes import ingest_admin_bp
from expenseai_ingest.watcher import start_watchers

__all__ = ["init_app", "shutdown", "ingest_admin_bp"]


def init_app(app: Flask) -> None:
    settings = IngestSettings.from_app(app)
    extension_state = app.extensions.setdefault("expenseai_ingest", {})

    watcher = start_watchers(app, settings)
    if watcher:
        extension_state["watcher"] = watcher

    emailer = start_email_poller(app, settings)
    if emailer:
        extension_state["email"] = emailer


def shutdown(app: Flask) -> None:
    state = app.extensions.get("expenseai_ingest")
    if not state:
        return
    watcher = state.get("watcher")
    if watcher:
        watcher.stop()
    emailer = state.get("email")
    if emailer:
        emailer.stop()
