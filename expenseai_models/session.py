"""Persistent session tokens for audit and security reviews."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta

from flask import current_app
from itsdangerous import TimestampSigner
from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class SessionToken(db.Model):
    """Tracks user login sessions with metadata."""

    __tablename__ = "session_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(db.ForeignKey("users.id"), nullable=False, index=True)
    token: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    @classmethod
    def issue(
        cls,
        user_id: int,
        ip: str | None,
        user_agent: str | None,
        remember_me: bool = False,
    ) -> "SessionToken":
        """Create and store a new session token with expiry."""
        lifetime = timedelta(days=30 if remember_me else 1)
        raw_token = secrets.token_urlsafe(32)
        signer = TimestampSigner(current_app.config.get("SECRET_KEY", "session-token"))
        signed_token = signer.sign(raw_token).decode("utf-8")
        record = cls(
            user_id=user_id,
            token=signed_token,
            ip=ip,
            user_agent=user_agent,
            expires_at=datetime.utcnow() + lifetime,
        )
        db.session.add(record)
        db.session.commit()
        return record
