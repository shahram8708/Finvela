"""Persistence for inbound and outbound WhatsApp message payloads."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


DIRECTION_ENUM = Enum("in", "out", name="whatsapp_direction")
MESSAGE_TYPE_ENUM = Enum("text", "interactive", "template", "status", name="whatsapp_msg_type")
STATUS_ENUM = Enum("queued", "sent", "delivered", "read", "failed", name="whatsapp_delivery_status")


class WhatsAppMessageLog(db.Model):
    """Audit trail of WhatsApp message exchanges."""

    __tablename__ = "whatsapp_message_logs"
    __table_args__ = (
        Index("ix_whatsapp_logs_contact_created", "contact_id", "created_at"),
        Index("ix_whatsapp_logs_msgid", "whatsapp_msg_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contact_id: Mapped[int | None] = mapped_column(
        ForeignKey("whatsapp_contacts.id", ondelete="SET NULL"), nullable=True, index=True
    )
    direction: Mapped[str] = mapped_column(DIRECTION_ENUM, nullable=False)
    msg_type: Mapped[str] = mapped_column(MESSAGE_TYPE_ENUM, nullable=False)
    whatsapp_msg_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(STATUS_ENUM, nullable=False, default="queued")
    payload_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    contact = relationship("WhatsAppContact", back_populates="message_logs")

    def update_status(self, status: str, *, error_code: str | None = None, error_message: str | None = None) -> None:
        self.status = status
        if error_code:
            self.error_code = error_code
        if error_message:
            self.error_message = error_message

    def as_dict(self) -> dict[str, object | None]:
        return {
            "id": self.id,
            "contact_id": self.contact_id,
            "direction": self.direction,
            "msg_type": self.msg_type,
            "whatsapp_msg_id": self.whatsapp_msg_id,
            "status": self.status,
            "payload_json": self.payload_json,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() + "Z",
        }
