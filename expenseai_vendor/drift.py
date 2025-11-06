"""Detect and persist vendor behavioural drift metrics."""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Iterable, List

from flask import current_app

from expenseai_ai import embeddings
from expenseai_vendor.fingerprints import _build_text_summary
from expenseai_models import AuditLog
from expenseai_models.invoice import Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_models.item_price_history import ItemPriceHistory
from expenseai_models.vendor_drift import VendorDrift
from expenseai_models.vendor_profile import VendorProfile
from expenseai_ext.db import db


def evaluate_drift(
    vendor_gst: str,
    *,
    invoice_date: date | None = None,
    invoice_id: int | None = None,
    organization_id: int | None = None,
) -> VendorDrift | None:
    """Compute a drift snapshot for the vendor within the supplied organization."""
    cleaned = (vendor_gst or "").strip().upper()
    if not cleaned:
        return None
    if organization_id is None:
        return None

    profile = VendorProfile.query.filter_by(vendor_gst=cleaned, organization_id=organization_id).first()
    if profile is None or not profile.vector_values():
        return None

    window_days = 30
    lookback_end = invoice_date or datetime.utcnow().date()
    window_start = lookback_end - timedelta(days=window_days)

    query = ItemPriceHistory.query.filter(ItemPriceHistory.vendor_gst == cleaned)
    query = query.filter(ItemPriceHistory.organization_id == organization_id)
    query = query.filter(
        (ItemPriceHistory.invoice_date.is_(None))
        | ((ItemPriceHistory.invoice_date >= window_start) & (ItemPriceHistory.invoice_date <= lookback_end))
    )
    entries: List[ItemPriceHistory] = (
        query.order_by(ItemPriceHistory.invoice_date.desc().nullslast(), ItemPriceHistory.created_at.desc()).all()
    )
    if not entries:
        return None

    summary_text = _build_text_summary(entries)
    if not summary_text:
        return None

    try:
        window_vector = embeddings.embed_text(summary_text, force_remote=True)
    except Exception as exc:  # pragma: no cover - embeddings invoke network
        current_app.logger.exception(
            "Failed to embed vendor drift window",
            extra={"vendor_gst": cleaned, "error": str(exc)},
        )
        return None

    base_vector = profile.vector_values()
    drift_score = _cosine_distance(base_vector, window_vector)

    record = VendorDrift(
        organization_id=organization_id,
        vendor_gst=cleaned,
        window_start=window_start,
        window_end=lookback_end,
        drift_score=drift_score,
        n_samples=len(entries),
    )
    record.update_vector(window_vector)
    db.session.add(record)
    db.session.flush()

    threshold = float(current_app.config.get("FINGERPRINT_DRIFT_THRESH", 0.25))
    min_lines = int(current_app.config.get("FINGERPRINT_MIN_LINES", 30))

    if drift_score >= threshold and len(entries) >= min_lines and invoice_id:
        invoice: Invoice | None = db.session.get(Invoice, invoice_id)
        if invoice is not None and invoice.organization_id == organization_id:
            payload = {
                "vendor_gst": cleaned,
                "drift_score": drift_score,
                "window_start": window_start.isoformat(),
                "window_end": lookback_end.isoformat(),
                "n_samples": len(entries),
            }
            InvoiceEvent.record(invoice, "VENDOR_DRIFT_ALERT", payload)
            AuditLog.log(
                action="vendor_drift_alert",
                entity="invoice",
                entity_id=invoice.id,
                data=payload,
            )
    return record


def _cosine_distance(vec_a: Iterable[float], vec_b: Iterable[float]) -> float:
    list_a = list(vec_a)
    list_b = list(vec_b)
    if not list_a or not list_b or len(list_a) != len(list_b):
        return 1.0
    dot = sum(a * b for a, b in zip(list_a, list_b))
    norm_a = math.sqrt(sum(a * a for a in list_a))
    norm_b = math.sqrt(sum(b * b for b in list_b))
    if norm_a == 0 or norm_b == 0:
        return 1.0
    cosine = max(-1.0, min(1.0, dot / (norm_a * norm_b)))
    return 1.0 - cosine
