"""Organization and invitation models supporting tenant-aware onboarding."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - hints only
    from expenseai_models.invoice import Invoice
    from expenseai_models.organization_subscription import OrganizationSubscription
    from expenseai_models.user import User


class Organization(db.Model):
    """Represents a customer tenant that groups users under a shared admin."""

    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    slug: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    user_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    is_premium: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    premium_since: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_payment_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    users: Mapped[list["User"]] = relationship("User", back_populates="organization")
    invites = relationship("RegistrationInvite", back_populates="organization", cascade="all, delete-orphan")
    invoices = relationship("Invoice", back_populates="organization", cascade="all, delete-orphan")
    subscriptions: Mapped[list["OrganizationSubscription"]] = relationship(
        "OrganizationSubscription",
        back_populates="organization",
        cascade="all, delete-orphan",
        order_by="OrganizationSubscription.created_at.desc()",
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Organization {self.slug}>"


class RegistrationInvite(db.Model):
    """Invitation codes that allow employees to join an organization."""

    __tablename__ = "registration_invites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    organization_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=False, index=True)
    created_by_id: Mapped[int] = mapped_column(Integer, db.ForeignKey("users.id"), nullable=False)
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    use_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    organization = relationship("Organization", back_populates="invites")
    created_by = relationship("User", foreign_keys=[created_by_id], back_populates="invites_created")

    @property
    def is_expired(self) -> bool:
        """Determine whether the invite can no longer be used due to expiry."""
        return bool(self.expires_at and self.expires_at <= datetime.utcnow())

    @property
    def remaining_uses(self) -> int | None:
        """Returns remaining uses when capped, otherwise ``None`` for unlimited."""
        if self.max_uses is None:
            return None
        return max(self.max_uses - self.use_count, 0)

    def can_be_used(self) -> bool:
        """Check whether the invite is active, unexpired and within its quota."""
        if not self.is_active:
            return False
        if self.is_expired:
            return False
        if self.max_uses is not None and self.use_count >= self.max_uses:
            return False
        return True

    def mark_used(self) -> None:
        """Record a successful registration against this invite."""
        self.use_count += 1
        self.last_used_at = datetime.utcnow()
        if self.max_uses is not None and self.use_count >= self.max_uses:
            self.is_active = False

    def as_dict(self) -> dict[str, object]:
        """Serialize key properties for UI rendering or JSON responses."""
        return {
            "id": self.id,
            "code": self.code,
            "organization_id": self.organization_id,
            "is_active": self.is_active,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "max_uses": self.max_uses,
            "use_count": self.use_count,
            "remaining_uses": self.remaining_uses,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<RegistrationInvite {self.code}>"
