"""Persistence models for compliance check summaries."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

from sqlalchemy import JSON, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from expenseai_models.invoice import Invoice

COMPLIANCE_CHECK_TYPES = ("GST_VENDOR", "GST_COMPANY", "HSN_RATE", "ARITHMETIC")
COMPLIANCE_STATUSES = ("PASS", "FAIL", "WARN", "NEEDS_API", "ERROR")


class ComplianceCheck(db.Model):
    """Aggregated outcome for a single compliance check type."""

    __tablename__ = "compliance_checks"
    __table_args__ = (
        Index("ix_compliance_invoice_type", "invoice_id", "check_type", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    check_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    details_json: Mapped[Dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="compliance_checks")

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON serializable representation."""
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "check_type": self.check_type,
            "status": self.status,
            "score": self.score,
            "summary": self.summary,
            "details": self.details_json or {},
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }
