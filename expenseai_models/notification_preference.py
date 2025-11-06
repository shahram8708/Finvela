"""Channel-specific notification preferences for users."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String, Time, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class NotificationPreference(db.Model):
    """User-configurable notification preferences across channels."""

    __tablename__ = "notification_preferences"
    __table_args__ = (
        UniqueConstraint("user_id", "channel", name="uq_notification_user_channel"),
        Index("ix_notification_preferences_user", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(db.ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    risk_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    quiet_hours_start: Mapped[datetime.time | None] = mapped_column(Time, nullable=True)
    quiet_hours_end: Mapped[datetime.time | None] = mapped_column(Time, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    user = relationship("User", back_populates="notification_preferences")

    def risk_threshold_effective(self, default_threshold: float) -> float:
        return self.risk_threshold if self.risk_threshold is not None else default_threshold

    def as_dict(self) -> dict[str, object | None]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "channel": self.channel,
            "enabled": self.enabled,
            "risk_threshold": self.risk_threshold,
            "quiet_hours_start": self.quiet_hours_start.isoformat() if self.quiet_hours_start else None,
            "quiet_hours_end": self.quiet_hours_end.isoformat() if self.quiet_hours_end else None,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }
