"""Idempotency key storage for safe request retries."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class IdempotencyKey(db.Model):
    """Persisted responses for idempotent operations."""

    __tablename__ = "idempotency_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(db.ForeignKey("users.id"), nullable=True)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    request_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(db.JSON, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(nullable=True)

    __table_args__ = (
        Index("ix_idempotency_scope", "scope"),
        Index("ix_idempotency_expires", "expires_at"),
    )

    def mark_used(self) -> None:
        self.used_at = datetime.utcnow()
        db.session.add(self)
        db.session.commit()
