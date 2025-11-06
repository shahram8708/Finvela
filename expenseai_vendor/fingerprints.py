"""Build and refresh long-term vendor fingerprints."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Iterable, List

from flask import current_app

from expenseai_ai import embeddings
from expenseai_models.item_price_history import ItemPriceHistory
from expenseai_models.vendor_profile import VendorProfile
from expenseai_ext.db import db


def refresh_vendor_profile(vendor_gst: str, *, organization_id: int | None) -> VendorProfile:
    """Recompute the vendor fingerprint embedding and summary statistics."""
    cleaned = (vendor_gst or "").strip().upper()
    if not cleaned:
        raise ValueError("Vendor GST identifier is required for fingerprint refresh")
    if organization_id is None:
        raise ValueError("Organization context is required for vendor fingerprint refresh")

    lookback_days = int(current_app.config.get("FINGERPRINT_LOOKBACK_DAYS", 365))
    cutoff = datetime.utcnow().date() - timedelta(days=lookback_days)

    query = ItemPriceHistory.query.filter(ItemPriceHistory.vendor_gst == cleaned)
    query = query.filter(ItemPriceHistory.organization_id == organization_id)
    query = query.filter(
        (ItemPriceHistory.invoice_date.is_(None))
        | (ItemPriceHistory.invoice_date >= cutoff)
    )
    entries: List[ItemPriceHistory] = (
        query.order_by(ItemPriceHistory.invoice_date.desc().nullslast(), ItemPriceHistory.created_at.desc()).all()
    )

    summary_text = _build_text_summary(entries)
    vector: list[float] | None = None
    if summary_text:
        try:
            vector = embeddings.embed_text(summary_text, force_remote=True)
        except Exception as exc:  # pragma: no cover - network/SDK dependent
            current_app.logger.exception(
                "Finvela embedding failed for vendor fingerprint",
                extra={"vendor_gst": cleaned, "error": str(exc)},
            )

    prices = [Decimal(entry.unit_price) for entry in entries if entry.unit_price is not None]
    avg_price = sum(prices) / Decimal(len(prices)) if prices else None
    mad_price = None
    if prices:
        med = median(prices)
        deviations = [abs(price - med) for price in prices]
        mad_price = median(deviations) if deviations else Decimal(0)

    profile = VendorProfile.query.filter_by(vendor_gst=cleaned, organization_id=organization_id).first()
    if profile is None:
        profile = VendorProfile(vendor_gst=cleaned, organization_id=organization_id)
        db.session.add(profile)
    else:
        profile.organization_id = organization_id

    profile.text_norm_summary = summary_text
    profile.n_samples = len(entries)
    profile.avg_unit_price = avg_price
    profile.price_mad = mad_price
    profile.last_updated = datetime.utcnow()
    if vector:
        profile.update_vector(vector)

    db.session.flush()
    return profile


def _build_text_summary(entries: Iterable[ItemPriceHistory]) -> str:
    """Concatenate normalized descriptions within a bounded window."""
    buffer: List[str] = []
    total_chars = 0
    char_limit = 3500
    for entry in entries:
        norm = entry.text_norm or ""
        if not norm:
            continue
        if total_chars + len(norm) > char_limit:
            break
        buffer.append(norm)
        total_chars += len(norm)
    return " ".join(buffer)
