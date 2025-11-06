"""Structured header fields extracted from invoices by the AI parser."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import JSON, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class ExtractedField(db.Model):
    """Represents a single key/value field extracted from an invoice."""

    __tablename__ = "extracted_fields"
    __table_args__ = (
        UniqueConstraint("invoice_id", "field_name", name="uq_extracted_field_invoice_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    field_name: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    source_bbox: Mapped[Dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False, index=True)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="extracted_fields")

    def as_dict(self) -> Dict[str, Any]:
        """Serialize the extracted field for JSON responses."""
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "field_name": self.field_name,
            "value": self.value,
            "confidence": self.confidence,
            "source_bbox": self.source_bbox,
            "raw_text": self.raw_text,
            "created_at": self.created_at.isoformat() + "Z",
        }
