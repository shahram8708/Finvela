"""One-time passcode persistence model."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class OneTimePasscode(db.Model):
    """Store hashed OTPs with expiry and attempt tracking."""

    __tablename__ = "otps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(db.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    code_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(nullable=False, index=True)
    attempts_remaining: Mapped[int] = mapped_column(Integer, nullable=False)
    metadata_json: Mapped[dict[str, str] | None] = mapped_column("metadata", db.JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("User", back_populates="otps")

    __table_args__ = (
        Index("ix_otps_user_purpose", "user_id", "purpose"),
    )
