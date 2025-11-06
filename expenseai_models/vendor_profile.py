"""Vendor fingerprint embeddings and aggregate statistics."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Float, Index, Integer, JSON, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - hints only
    from expenseai_models.organization import Organization


class VendorProfile(db.Model):
    """Stores long-term vendor behaviour fingerprints."""

    __tablename__ = "vendor_profiles"
    __table_args__ = (Index("ix_vendor_profiles_org_vendor", "organization_id", "vendor_gst", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=True, index=True)
    vendor_gst: Mapped[str] = mapped_column(String(64), nullable=False)
    text_norm_summary: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    vector: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    n_samples: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_unit_price: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    price_mad: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    organization: Mapped["Organization | None"] = relationship("Organization")

    def vector_values(self) -> list[float] | None:
        """Return the vendor embedding vector if stored."""
        payload = self.vector or {}
        values = payload.get("values") if isinstance(payload, dict) else None
        if isinstance(values, list):
            return [float(x) for x in values]
        return None

    def update_vector(self, values: list[float]) -> None:
        """Persist the embedding vector as JSON."""
        self.vector = {"values": [float(x) for x in values]}
