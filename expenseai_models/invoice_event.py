"""Event log entries specific to invoice lifecycle actions."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import JSON, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class InvoiceEvent(db.Model):
    """Represents an event emitted for an invoice (status change, action, etc.)."""

    __tablename__ = "invoice_events"
    __table_args__ = (
        Index("ix_invoice_events_invoice_created", "invoice_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[Dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False, index=True)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="events")

    @classmethod
    def record(cls, invoice: "Invoice", event_type: str, payload: Dict[str, Any] | None = None) -> "InvoiceEvent":
        """Persist a new event for the given invoice."""
        event = cls(invoice=invoice, event_type=event_type, payload=payload or {})
        db.session.add(event)
        db.session.flush()  # ensure event has an id for SSE before commit
        return event

    def as_dict(self) -> Dict[str, Any]:
        """Serialize event into a JSON-friendly dict."""
        return {
            "event_id": self.id,
            "invoice_id": self.invoice_id,
            "event_type": self.event_type,
            "payload": self.payload or {},
            "created_at": self.created_at.isoformat() + "Z",
        }
