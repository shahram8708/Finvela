"""Training examples captured for the contextual bandit."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

_LABEL_SOURCE_ENUM = ("user", "system")


class BanditExample(db.Model):
    """Captures invoice contexts and rewards used for policy updates."""

    __tablename__ = "bandit_examples"
    __table_args__ = (
        Index("ix_bandit_examples_invoice", "invoice_id"),
        Index("ix_bandit_examples_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False)
    context_json: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    reward: Mapped[float] = mapped_column(Float, nullable=False)
    label_source: Mapped[str] = mapped_column(String(16), nullable=False, default="user")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    invoice = relationship("Invoice", back_populates="bandit_examples")

    def as_tuple(self) -> tuple[dict[str, float], float]:
        """Return context and reward pair for training."""
        return self.context_json, float(self.reward)
