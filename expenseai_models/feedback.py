"""Reviewer feedback persisted for adaptive risk learning."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

_FEEDBACK_LABEL = Enum("approve", "reject", "false_positive", name="feedback_label")


class Feedback(db.Model):
    """Stores reviewer feedback for invoices."""

    __tablename__ = "feedback"
    __table_args__ = (
        Index("ix_feedback_invoice", "invoice_id"),
        Index("ix_feedback_user", "user_id"),
        Index("ix_feedback_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    label: Mapped[str] = mapped_column(_FEEDBACK_LABEL, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    invoice = relationship("Invoice", back_populates="feedback_entries")
    user = relationship("User", back_populates="feedback")

    def as_dict(self) -> dict[str, object | None]:
        """Return a serializable representation of the feedback entry."""
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "user_id": self.user_id,
            "label": self.label,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() + "Z",
        }
