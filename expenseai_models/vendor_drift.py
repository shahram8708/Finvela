"""Rolling vendor drift scores computed from embeddings."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class VendorDrift(db.Model):
    """Represents drift metrics for vendors across time windows."""

    __tablename__ = "vendor_drift"
    __table_args__ = (
        Index("ix_vendor_drift_org_vendor_window", "organization_id", "vendor_gst", "window_start", "window_end"),
        Index("ix_vendor_drift_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=True, index=True)
    vendor_gst: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    window_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    window_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    vector: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    drift_score: Mapped[float] = mapped_column(Float, nullable=False)
    n_samples: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    def vector_values(self) -> list[float] | None:
        payload = self.vector or {}
        values = payload.get("values") if isinstance(payload, dict) else None
        if isinstance(values, list):
            return [float(x) for x in values]
        return None

    def update_vector(self, values: list[float]) -> None:
        self.vector = {"values": [float(x) for x in values]}
