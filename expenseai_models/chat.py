"""AI chat session and message models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class AiChatSession(db.Model):
    """Stores a single AI chat session for a user."""

    __tablename__ = "ai_chat_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="Untitled chat")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    file_mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False, default="gemini-2.5-flash-lite")

    messages: Mapped[list["AiChatMessage"]] = relationship(
        "AiChatMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="AiChatMessage.created_at.asc()",
    )
    user: Mapped["User"] = relationship("User", back_populates="ai_chat_sessions")

    def ensure_title(self) -> None:
        """Ensure the session has a human friendly title."""
        if self.title and self.title.strip():
            return
        base = self.file_name or "New chat"
        self.title = base[:250]


class AiChatMessage(db.Model):
    """Individual message stored for a chat session."""

    __tablename__ = "ai_chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("ai_chat_sessions.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)  # "user" or "assistant"
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    session: Mapped[AiChatSession] = relationship("AiChatSession", back_populates="messages")

    def as_dict(self) -> dict[str, str]:
        """Serialize the message for JSON responses."""
        return {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "created_at": self.created_at.isoformat() + "Z",
        }


class ContextualChatSession(db.Model):
    """Context-aware chat session that bootstraps from prior invoice chats."""

    __tablename__ = "contextual_chat_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="Context chat")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False, default="gemini-2.5-flash-lite")
    is_initialized: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    seed_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_session_ids: Mapped[list[int]] = mapped_column(db.JSON, nullable=False, default=list)

    messages: Mapped[list["ContextualChatMessage"]] = relationship(
        "ContextualChatMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ContextualChatMessage.created_at.asc()",
    )
    user: Mapped["User"] = relationship("User", back_populates="contextual_chat_sessions")

    def as_dict(self) -> dict[str, object]:
        source_ids = list(self.source_session_ids or [])
        return {
            "id": self.id,
            "user_id": self.user_id,
            "title": self.title,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
            "model_name": self.model_name,
            "is_initialized": bool(self.is_initialized),
            "source_session_ids": source_ids,
        }


class ContextualChatMessage(db.Model):
    """Individual message stored for a context-aware chat session."""

    __tablename__ = "contextual_chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("contextual_chat_sessions.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    session: Mapped[ContextualChatSession] = relationship("ContextualChatSession", back_populates="messages")

    def as_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "created_at": self.created_at.isoformat() + "Z",
        }
