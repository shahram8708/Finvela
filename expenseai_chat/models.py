"""Database models for the organization chat module."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class ChatMessage(db.Model):
    """Represents a private message between two organization members."""

    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    sender_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    receiver_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    sender = relationship("User", foreign_keys=[sender_id])
    receiver = relationship("User", foreign_keys=[receiver_id])

    __table_args__ = (
        Index(
            "ix_chat_messages_org_participants_created",
            "organization_id",
            "sender_id",
            "receiver_id",
            "created_at",
        ),
    )

    def as_dict(self, current_user_id: int) -> dict[str, object]:
        """Serialize the message for JSON responses."""
        return {
            "id": self.id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "organization_id": self.organization_id,
            "message": self.message,
            "timestamp": self.created_at.isoformat() + "Z",
            "sender_is_self": self.sender_id == current_user_id,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<ChatMessage {self.id} org={self.organization_id}>"


class GroupMessage(db.Model):
    """Represents a message posted in an organization-wide room."""

    __tablename__ = "group_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    sender_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    sender = relationship("User", foreign_keys=[sender_id])

    __table_args__ = (
        Index("ix_group_messages_org_created", "organization_id", "created_at"),
    )

    def as_dict(self, current_user_id: int) -> dict[str, object]:
        """Serialize the message for JSON responses."""
        return {
            "id": self.id,
            "sender_id": self.sender_id,
            "organization_id": self.organization_id,
            "message": self.message,
            "timestamp": self.created_at.isoformat() + "Z",
            "sender_is_self": self.sender_id == current_user_id,
            "sender_name": self.sender.full_name if self.sender else "",
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<GroupMessage {self.id} org={self.organization_id}>"
