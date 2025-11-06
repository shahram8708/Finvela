"""Privacy export/delete request tracking."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Enum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


PrivacyRequestType = Enum("export", "delete", name="privacy_request_type")
PrivacyRequestStatus = Enum("queued", "processing", "done", "error", name="privacy_request_status")


class PrivacyRequest(db.Model):
    """Track lifecycle of user privacy requests."""

    __tablename__ = "privacy_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(db.ForeignKey("users.id"), nullable=False, index=True)
    type: Mapped[str] = mapped_column(PrivacyRequestType, nullable=False)
    status: Mapped[str] = mapped_column(PrivacyRequestStatus, nullable=False, default="queued")
    result_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_text: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    def mark(self, status: str, *, error: str | None = None, result_path: str | None = None) -> None:
        self.status = status
        self.error_text = error
        self.result_path = result_path
        self.updated_at = datetime.utcnow()
        db.session.add(self)
        db.session.commit()
