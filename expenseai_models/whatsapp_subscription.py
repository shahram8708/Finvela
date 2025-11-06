"""Opt-in tokens for linking WhatsApp contacts to users."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class WhatsAppSubscription(db.Model):
    """Tracks pending WhatsApp opt-ins with verification tokens."""

    __tablename__ = "whatsapp_subscriptions"
    __table_args__ = (
        Index("ix_whatsapp_subscriptions_token", "token", unique=True),
        Index("ix_whatsapp_subscriptions_user", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(db.ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    phone_e164: Mapped[str] = mapped_column(String(32), nullable=False)
    token: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    user = relationship("User", back_populates="whatsapp_subscriptions")

    def mark_confirmed(self) -> None:
        self.confirmed_at = datetime.utcnow()

    def is_active(self) -> bool:
        return self.confirmed_at is None and self.expires_at > datetime.utcnow()
