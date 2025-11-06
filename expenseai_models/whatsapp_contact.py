"""WhatsApp contact records linking phone numbers to users."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class WhatsAppContact(db.Model):
    """Persisted WhatsApp contact identifiers for outbound messaging."""

    __tablename__ = "whatsapp_contacts"
    __table_args__ = (
        Index("ix_whatsapp_contacts_phone", "phone_e164", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    phone_e164: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    user = relationship("User", back_populates="whatsapp_contacts")
    message_logs = relationship(
        "WhatsAppMessageLog",
        back_populates="contact",
        cascade="all, delete-orphan",
        order_by="WhatsAppMessageLog.created_at.desc()",
    )

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "phone_e164": self.phone_e164,
            "display_name": self.display_name,
            "verified": self.verified,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }
