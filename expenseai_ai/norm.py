"""Normalization helpers applied before persisting AI extracted values."""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from flask import current_app


def norm_currency(value: str | None) -> str | None:
    """Return an upper-cased ISO 4217 code, or `None` if invalid."""
    if not value:
        return None
    code = value.strip().upper()
    if len(code) == 3:
        return code
    current_app.logger.info("Discarding non-ISO currency code", extra={"currency": value})
    return None


def norm_gst(value: str | None) -> str | None:
    """Normalize GST identifiers by trimming whitespace and uppercasing."""
    if not value:
        return None
    cleaned = value.strip().upper().replace(" ", "")
    return cleaned or None


def parse_iso_date(value: Any) -> date | None:
    """Parse ISO formatted date strings to `date` objects."""
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        current_app.logger.info("Failed to coerce invoice date", extra={"value": value})
        return None


def to_decimal(value: Any) -> Decimal | None:
    """Convert arbitrary numeric inputs to `Decimal` while logging coercions."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        current_app.logger.info("Unable to convert value to Decimal", extra={"value": value, "error": str(exc)})
        return None


_STOP_WORDS = {
    "invoice",
    "bill",
    "tax",
    "service",
    "services",
    "charge",
    "charges",
    "item",
    "items",
    "unit",
    "gst",
    "hsn",
    "sac",
    "india",
}


def _lemmatize(token: str) -> str:
    """Apply light lemmatization rules without heavy dependencies."""
    if len(token) <= 3:
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("ing") and len(token) > 5:
        return token[:-3]
    if token.endswith("es") and len(token) > 3:
        return token[:-2]
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def normalize_description(raw: str | None) -> str:
    """Lowercase, strip punctuation, and collapse whitespace for descriptions."""
    if not raw:
        return ""
    lowered = raw.lower()
    # Remove punctuation/special characters.
    cleaned = re.sub(r"[^a-z0-9\s]", " ", lowered)
    tokens = []
    for candidate in cleaned.split():
        if candidate in _STOP_WORDS:
            continue
        lemma = _lemmatize(candidate)
        if lemma and lemma not in _STOP_WORDS:
            tokens.append(lemma)
    return " ".join(tokens)
