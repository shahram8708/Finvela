"""Application factory and shared extension instances."""
from __future__ import annotations

import os
from importlib import import_module
from typing import Any, Dict, Type

from flask import Flask
from flask_caching import Cache
from markupsafe import Markup, escape

from config import BaseConfig, DevConfig, ProdConfig
from expenseai_ext import auth as auth_ext
from expenseai_ext import db as db_ext
from expenseai_ext import errors as errors_ext
from expenseai_ext import i18n as i18n_ext
from expenseai_ext import logging as logging_ext
from expenseai_ext import security as security_ext

cache = Cache()

CONFIG_MAP: Dict[str, Type[BaseConfig]] = {
    "development": DevConfig,
    "dev": DevConfig,
    "production": ProdConfig,
    "prod": ProdConfig,
}


def create_app(
    config_object: str | Type[BaseConfig] | None = None,
    *,
    start_background: bool = True,
    create_db: bool = True,
) -> Flask:
    """Application factory used by both CLI and WSGI entrypoints."""
    app = Flask(__name__, template_folder=None, static_folder=None)

    _load_config(app, config_object)
    logging_ext.configure_logging(app)
    i18n_ext.init_app(app)
    errors_ext.init_app(app)

    db_ext.init_app(app)
    security_ext.init_app(app)
    auth_ext.init_app(app)
    cache.init_app(app)

    _register_template_filters(app)

    from expenseai.celery_app import make_celery

    make_celery(app)

    _register_blueprints(app)
    _register_error_handlers(app)
    _register_cli(app)
    _register_middleware(app)
    if start_background:
        _start_background_services(app)

    if create_db:
        with app.app_context():
            db_ext.db.create_all() 

    return app


def _load_config(app: Flask, config_object: str | Type[BaseConfig] | None) -> None:
    if config_object is None:
        env_name = os.getenv("FLASK_ENV", "development").lower()
        config_cls = CONFIG_MAP.get(env_name, DevConfig)
    elif isinstance(config_object, str):
        key = config_object.lower()
        if key in CONFIG_MAP:
            config_cls = CONFIG_MAP[key]
        else:
            module_path, _, attr = config_object.rpartition(".")
            if module_path:
                module = import_module(module_path)
                config_cls = getattr(module, attr)
            else:
                raise KeyError(f"Unknown config identifier: {config_object}")
    else:
        config_cls = config_object

    app.config.from_object(config_cls)


def _register_blueprints(app: Flask) -> None:
    from expenseai_auth import auth_bp
    from expenseai_benchmark import benchmark_admin_bp
    from expenseai_compliance import compliance_admin_bp
    from expenseai_invoices import invoices_bp
    from expenseai_counterfactual import counterfactual_bp
    from expenseai_chat import chat_bp
    from expenseai_vendor import vendor_bp
    from expenseai_risk import risk_bp
    from expenseai_web import web_bp
    from expenseai_ingest import ingest_admin_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(invoices_bp, url_prefix="/invoices")
    # Counterfactual simulator endpoints (what-if) — register after invoices to keep routes grouped
    app.register_blueprint(counterfactual_bp)
    app.register_blueprint(chat_bp, url_prefix="/organization")
    app.register_blueprint(compliance_admin_bp)
    app.register_blueprint(benchmark_admin_bp)
    app.register_blueprint(risk_bp)
    app.register_blueprint(vendor_bp)
    app.register_blueprint(ingest_admin_bp)


def _register_error_handlers(app: Flask) -> None:
    # HTML error templates are registered via Blueprint loaders.
    pass


def _register_cli(app: Flask) -> None:
    from expenseai_cli.manage import manage_cli

    app.cli.add_command(manage_cli, "manage")


def _register_middleware(app: Flask) -> None:
    from expenseai_web.middleware import init_app as middleware_init

    middleware_init(app)


def _start_background_services(app: Flask) -> None:
    """Boot background workers once the application is fully configured."""
    from expenseai_ai.parser_service import start_background_worker
    from expenseai_ingest import init_app as ingest_init

    start_background_worker(app)
    ingest_init(app)


def _register_template_filters(app: Flask) -> None:
    @app.template_filter("nl2br")
    def nl2br_filter(value: object) -> Markup:
        """Convert line breaks in plain text to HTML <br> tags."""
        if value is None:
            return Markup("")

        if not isinstance(value, Markup):
            value = escape(value)

        normalized = str(value).replace("\r\n", "\n").replace("\r", "\n")
        # Preserve deliberate double newlines for spacing while converting line breaks.
        return Markup(normalized.replace("\n", "<br>"))
