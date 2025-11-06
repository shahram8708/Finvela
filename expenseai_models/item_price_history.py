"""Historical item prices used for benchmarking."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class ItemPriceHistory(db.Model):
    """Persists historical invoice pricing information by normalized text."""

    __tablename__ = "item_price_history"
    __table_args__ = (
        Index("ix_item_price_history_text", "text_norm"),
        Index("ix_item_price_history_text_currency", "text_norm", "currency"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=True, index=True)
    text_norm: Mapped[str] = mapped_column(Text, nullable=False)
    vendor_gst: Mapped[str | None] = mapped_column(String(64), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    qty: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    invoice_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    invoice_id: Mapped[int | None] = mapped_column(
        db.ForeignKey("invoices.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<ItemPriceHistory {self.id} {self.text_norm[:24]}>"
