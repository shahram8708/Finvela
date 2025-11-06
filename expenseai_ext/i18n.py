"""Internationalisation helpers and Babel integration."""
from __future__ import annotations

from typing import Optional

from flask import Flask, current_app, request, session
from flask_babel import Babel, get_locale, gettext

babel = Babel()


def init_app(app: Flask) -> None:
    """Initialise Babel with locale selection hooks."""

    babel.init_app(app, locale_selector=_select_locale)
    app.jinja_env.globals.setdefault("_", gettext)
    app.jinja_env.globals.setdefault("current_locale", current_locale)
    app.jinja_env.globals.setdefault("current_language_label", current_language_label)
    app.jinja_env.globals.setdefault("language_label", language_label)

    @app.context_processor
    def inject_globals():
        return dict(current_app=current_app)


def _select_locale() -> str:
    stored = session.get("preferred_locale")
    if stored:
        return stored
    match = request.accept_languages.best_match(_supported_locales())
    if match:
        return match
    return current_app.config.get("BABEL_DEFAULT_LOCALE", "en")


def _supported_locales() -> list[str]:
    return current_app.config.get("BABEL_SUPPORTED_LOCALES", ["en"])


def current_locale() -> str:
    locale = get_locale()
    return str(locale) if locale is not None else "en"


def current_language_label() -> str:
    return language_label(current_locale())


def language_label(locale_code: str | None) -> str:
    if not locale_code:
        locale_code = current_app.config.get("BABEL_DEFAULT_LOCALE", "en")

    normalized = str(locale_code)
    labels = {
        "en": gettext("English"),
        "hi": gettext("Hindi"),
    }
    return labels.get(normalized, normalized.upper())
