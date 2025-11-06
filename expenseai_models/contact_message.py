"""Contact form submissions recorded for follow-up."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class ContactMessage(db.Model):
    """Represents a message submitted via the public contact form."""

    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    def as_dict(self) -> dict[str, object]:
        """Serialize the message for JSON responses or emails."""
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "subject": self.subject,
            "category": self.category,
            "message": self.message,
            "submitted_at": self.submitted_at.isoformat() + "Z" if self.submitted_at else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<ContactMessage {self.email} {self.submitted_at:%Y-%m-%d}>"
