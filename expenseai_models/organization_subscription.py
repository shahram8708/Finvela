"""Subscription transactions for organization upgrades."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - typing only
    from expenseai_models.organization import Organization
    from expenseai_models.user import User

try:
    from sqlalchemy.dialects.postgresql import JSONB
except ImportError:  # pragma: no cover - optional dependency
    JSONB = None  # type: ignore[assignment]

if JSONB is not None:
    JSONType = JSON().with_variant(JSONB, "postgresql")
else:
    JSONType = JSON


class OrganizationSubscription(db.Model):
    """Represents a lifetime upgrade purchase for an organization's seat limit."""

    __tablename__ = "organization_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    created_by_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    payment_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    signature: Mapped[str] = mapped_column(String(255), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="INR")
    amount_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    per_user_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    previous_user_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    purchased_user_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    additional_users: Mapped[int] = mapped_column(Integer, nullable=False)
    notes = mapped_column(JSONType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    organization: Mapped["Organization"] = relationship("Organization", back_populates="subscriptions")
    created_by: Mapped["User"] = relationship("User", back_populates="subscription_events")

    def as_dict(self) -> dict[str, object]:
        """Serialize subscription metadata for display layers."""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "payment_id": self.payment_id,
            "currency": self.currency,
            "amount_minor": self.amount_minor,
            "per_user_price_minor": self.per_user_price_minor,
            "previous_user_limit": self.previous_user_limit,
            "purchased_user_limit": self.purchased_user_limit,
            "additional_users": self.additional_users,
            "created_at": self.created_at.isoformat(),
            "notes": self.notes or {},
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<OrganizationSubscription order={self.order_id}>"
