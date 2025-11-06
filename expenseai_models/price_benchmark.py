"""Market price benchmark results for invoice line items."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List

from sqlalchemy import JSON, Float, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - import only for typing hints
    from expenseai_models.invoice import Invoice
    from expenseai_models.line_item import LineItem


class PriceBenchmark(db.Model):
    """Stores AI-grounded market price comparisons for an invoice line item."""

    __tablename__ = "price_benchmarks"
    __table_args__ = (
        Index("ix_price_benchmarks_invoice_line", "invoice_id", "line_item_id", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    line_item_id: Mapped[int] = mapped_column(db.ForeignKey("line_items.id", ondelete="CASCADE"), nullable=False, index=True)
    product_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    search_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    billed_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    billed_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    market_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    market_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    price_low: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    price_high: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    delta_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    sources_json: Mapped[List[Dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    raw_response: Mapped[Dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="price_benchmarks")
    line_item: Mapped["LineItem"] = relationship("LineItem", back_populates="price_benchmark")

    def as_dict(self) -> Dict[str, Any]:
        """Serialize the benchmark to a JSON-friendly structure."""

        def serialize_decimal(value: Decimal | None) -> float | None:
            return float(value) if value is not None else None

        line_no = self.line_item.line_no if getattr(self, "line_item", None) else None
        created_iso = self.created_at.isoformat() + "Z" if getattr(self, "created_at", None) else None
        updated_iso = self.updated_at.isoformat() + "Z" if getattr(self, "updated_at", None) else None
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "line_item_id": self.line_item_id,
            "line_no": line_no,
            "product_name": self.product_name,
            "search_query": self.search_query,
            "billed_price": serialize_decimal(self.billed_price),
            "billed_currency": self.billed_currency,
            "market_price": serialize_decimal(self.market_price),
            "market_currency": self.market_currency,
            "price_low": serialize_decimal(self.price_low),
            "price_high": serialize_decimal(self.price_high),
            "delta_percent": self.delta_percent,
            "summary": self.summary,
            "confidence": self.confidence,
            "sources": list(self.sources_json or []),
            "created_at": created_iso,
            "updated_at": updated_iso,
        }
