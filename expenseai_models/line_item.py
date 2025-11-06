"""Line-item level invoice data captured from AI extraction."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from sqlalchemy import Float, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - typing only
    from expenseai_models.invoice import Invoice
    from expenseai_models.price_benchmark import PriceBenchmark


class LineItem(db.Model):
    """Represents a single invoice line item extracted by the parser."""

    __tablename__ = "line_items"
    __table_args__ = (
        Index("ix_line_items_invoice_line", "invoice_id", "line_no", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    line_no: Mapped[int] = mapped_column(Integer, nullable=False)
    description_raw: Mapped[str] = mapped_column(Text, nullable=False)
    description_norm: Mapped[str | None] = mapped_column(Text, nullable=True)
    hsn_sac: Mapped[str | None] = mapped_column(String(64), nullable=True)
    qty: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    gst_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    line_subtotal: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    line_tax: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    line_total: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False, index=True)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="line_items")
    price_benchmark: Mapped["PriceBenchmark | None"] = relationship(
        "PriceBenchmark",
        back_populates="line_item",
        uselist=False,
    )

    def as_dict(self) -> Dict[str, Any]:
        """Serialize the line item to a JSON-friendly mapping."""
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "line_no": self.line_no,
            "description_raw": self.description_raw,
            "description_norm": self.description_norm,
            "hsn_sac": self.hsn_sac,
            "qty": float(self.qty) if self.qty is not None else None,
            "unit_price": float(self.unit_price) if self.unit_price is not None else None,
            "gst_rate": float(self.gst_rate) if self.gst_rate is not None else None,
            "line_subtotal": float(self.line_subtotal) if self.line_subtotal is not None else None,
            "line_tax": float(self.line_tax) if self.line_tax is not None else None,
            "line_total": float(self.line_total) if self.line_total is not None else None,
            "confidence": self.confidence,
            "created_at": self.created_at.isoformat() + "Z",
        }
