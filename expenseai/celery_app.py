"""Celery application factory wired to the Flask app configuration."""
from __future__ import annotations

from typing import Any

from celery import Celery
from flask import Flask

celery = Celery("expenseai")


def make_celery(app: Flask | None = None) -> Celery:
    """Configure the global Celery instance against the given Flask app."""
    from expenseai_ext import create_app  # dynamic import to avoid circular deps

    if app is None:
        app = create_app(start_background=False)

    broker_url = app.config["CELERY_BROKER_URL"]
    result_backend = app.config["CELERY_RESULT_BACKEND"]

    celery.conf.update(
        broker_url=broker_url,
        result_backend=result_backend,
        result_expires=app.config.get("CELERY_RESULT_EXPIRES", 3600),
        task_default_queue=app.config.get("CELERY_TASK_DEFAULT_QUEUE", "default"),
        task_acks_late=True,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_always_eager=app.config.get("CELERY_TASK_ALWAYS_EAGER", False),
        task_eager_propagates=True,
        broker_transport_options={
            "visibility_timeout": app.config.get("CELERY_VISIBILITY_TIMEOUT", 3600),
        },
    )

    class AppContextTask(celery.Task):
        """Task base class that wraps execution inside the Flask app context."""

        abstract = True

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            with app.app_context():
                return super().__call__(*args, **kwargs)

    celery.Task = AppContextTask  # type: ignore[assignment]
    celery.flask_app = app  # type: ignore[attr-defined]

    celery.autodiscover_tasks(["expenseai_ingest"])
    app.extensions["celery"] = celery
    return celery


__all__ = ["celery", "make_celery"]


# Configure the default Celery instance when the module loads so that the
# worker entry point (`celery -A expenseai.celery_app:celery worker`) always
# has broker settings available.
make_celery()
