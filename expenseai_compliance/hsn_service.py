"""Services for loading and querying HSN/SAC rate tables."""
from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Tuple

from flask import current_app

from expenseai_ext.db import db
from expenseai_models.hsn_rate import HsnRate


def load_default_rates() -> Tuple[int, int]:
    """Load initial rates from the configured CSV path if present."""
    source = current_app.config.get("HSN_RATES_SOURCE")
    if not source:
        return (0, 0)
    path = Path(source)
    if not path.exists():
        return (0, 0)
    with path.open("r", encoding="utf-8") as handle:
        return refresh_rates(handle)


def refresh_rates(file_obj, replace_existing: bool = True) -> Tuple[int, int]:
    """Load HSN rates from a CSV file-like object.

    Returns a tuple of (inserted, updated).
    """
    reader = csv.DictReader(file_obj)
    inserted = 0
    updated = 0
    required_columns = {"code", "gst_rate", "effective_from"}
    if not required_columns.issubset({col.strip() for col in reader.fieldnames or []}):
        missing = required_columns - set(reader.fieldnames or [])
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    for row in reader:
        code = row.get("code", "").strip().upper()
        if not code:
            current_app.logger.warning("Skipping HSN rate row without code", extra={"row": row})
            continue
        try:
            gst_rate = Decimal(row.get("gst_rate", "0").strip())
        except (InvalidOperation, ValueError) as exc:
            current_app.logger.warning("Invalid GST rate", extra={"row": row, "error": str(exc)})
            continue
        try:
            effective_from = datetime.strptime(row.get("effective_from", ""), "%Y-%m-%d").date()
        except ValueError:
            current_app.logger.warning("Invalid effective_from date", extra={"row": row})
            continue
        effective_to_value = row.get("effective_to")
        effective_to = None
        if effective_to_value:
            try:
                effective_to = datetime.strptime(effective_to_value, "%Y-%m-%d").date()
            except ValueError:
                current_app.logger.warning("Invalid effective_to date", extra={"row": row})
                effective_to = None

        description = row.get("description")
        existing = (
            HsnRate.query.filter_by(code=code, effective_from=effective_from)
            .order_by(HsnRate.id.asc())
            .first()
        )
        if existing:
            if replace_existing:
                existing.description = description
                existing.gst_rate = gst_rate
                existing.effective_to = effective_to
                updated += 1
            continue

        db.session.add(
            HsnRate(
                code=code,
                description=description,
                gst_rate=gst_rate,
                effective_from=effective_from,
                effective_to=effective_to,
            )
        )
        inserted += 1
    db.session.commit()
    return inserted, updated


def get_rate(code: str | None, on_date: date | None) -> HsnRate | None:
    """Return the matching HSN rate entry for the given code and date."""
    if not code:
        return None
    normalized = code.strip().upper()
    if not normalized:
        return None
    query = HsnRate.query.filter(HsnRate.code == normalized)
    when = on_date or date.today()
    query = query.filter(HsnRate.effective_from <= when)
    query = query.filter((HsnRate.effective_to.is_(None)) | (HsnRate.effective_to >= when))
    return query.order_by(HsnRate.effective_from.desc()).first()


def stats() -> Dict[str, int | str | None]:
    """Return simple statistics about the loaded rates for admin dashboards."""
    total = HsnRate.query.count()
    latest = (
        db.session.query(HsnRate.effective_from)
        .order_by(HsnRate.effective_from.desc())
        .limit(1)
        .scalar()
    )
    return {"count": total, "latest_effective_from": latest.isoformat() if latest else None}
