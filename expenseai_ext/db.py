"""Database helpers including SQLAlchemy and Flask-Migrate wiring."""
from __future__ import annotations

from flask import Flask
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

# Initialize the extensions without an app bound so they can be configured
# inside the application factory.
db = SQLAlchemy()
migrate = Migrate()


def init_app(app: Flask) -> None:
    """Bind SQLAlchemy and Flask-Migrate to the provided application."""
    db.init_app(app)
    migrate.init_app(app, db)
