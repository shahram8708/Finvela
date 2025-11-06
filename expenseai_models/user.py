"""User model and related helpers."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from flask import current_app
from flask_login import UserMixin
from passlib.context import CryptContext
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Table, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

# Use pbkdf2_sha256 as the default hashing scheme (no 72-byte limit).
# Keep bcrypt in the list so passlib can still verify legacy bcrypt hashes if
# the bcrypt backend is available, but mark it deprecated so new hashes use pbkdf2.
_password_context = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt"],
    default="pbkdf2_sha256",
    deprecated="auto",
)

user_roles = Table(
    "user_roles",
    db.Model.metadata,
    Column("user_id", Integer, db.ForeignKey("users.id"), primary_key=True),
    Column("role_id", Integer, db.ForeignKey("roles.id"), primary_key=True),
    UniqueConstraint("user_id", "role_id", name="uq_user_role"),
)


if TYPE_CHECKING:  # pragma: no cover - used for type hints only
    from expenseai_models.feedback import Feedback
    from expenseai_models.chat import AiChatSession, ContextualChatSession
    from expenseai_models.organization import Organization, RegistrationInvite
    from expenseai_models.organization_subscription import OrganizationSubscription
    from expenseai_models.invoice import Invoice
    from expenseai_models.role import Role
    from expenseai_models.otp import OneTimePasscode
    from expenseai_models.whatsapp_contact import WhatsAppContact
    from expenseai_models.whatsapp_subscription import WhatsAppSubscription


class User(UserMixin, db.Model):
    """Application user persisted in the database."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    _is_active: Mapped[bool] = mapped_column("is_active", Boolean, default=True, nullable=False)
    organization_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=True, index=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    organization: Mapped[Organization | None] = relationship("Organization", back_populates="users")
    roles: Mapped[list["Role"]] = relationship("Role", secondary=user_roles, back_populates="users")
    invites_created: Mapped[list["RegistrationInvite"]] = relationship(
        "RegistrationInvite",
        back_populates="created_by",
        foreign_keys="RegistrationInvite.created_by_id",
    )
    feedback: Mapped[list["Feedback"]] = relationship(
        "Feedback",
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="Feedback.created_at.desc()",
    )
    whatsapp_contacts: Mapped[list["WhatsAppContact"]] = relationship(
        "WhatsAppContact",
        back_populates="user",
    )
    whatsapp_subscriptions: Mapped[list["WhatsAppSubscription"]] = relationship(
        "WhatsAppSubscription",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    ai_chat_sessions: Mapped[list["AiChatSession"]] = relationship(
        "AiChatSession",
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="AiChatSession.updated_at.desc()",
    )
    contextual_chat_sessions: Mapped[list["ContextualChatSession"]] = relationship(
        "ContextualChatSession",
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="ContextualChatSession.updated_at.desc()",
    )
    subscription_events: Mapped[list["OrganizationSubscription"]] = relationship(
        "OrganizationSubscription",
        back_populates="created_by",
        foreign_keys="OrganizationSubscription.created_by_id",
    )
    assigned_invoices: Mapped[list["Invoice"]] = relationship(
        "Invoice",
        back_populates="assignee",
        foreign_keys="Invoice.assignee_id",
    )
    otps: Mapped[list["OneTimePasscode"]] = relationship(
        "OneTimePasscode",
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="OneTimePasscode.created_at.desc()",
    )

    def set_password(self, password: str) -> None:
        """Hash and store a new password for the user using pbkdf2_sha256 by default."""
        self.password_hash = _password_context.hash(password)

    def verify_password(self, password: str) -> bool:
        """
        Check whether the provided password matches the stored hash.

        - Uses passlib CryptContext for verification.
        - If verification succeeds and the current hash is deprecated (bcrypt), rehash
          the password with the default (pbkdf2_sha256) and persist the new hash.
        - If passlib raises an unexpected exception (for example due to a broken
          bcrypt backend), attempt a minimal fallback using the low-level bcrypt
          module if available. If fallback succeeds, re-hash to pbkdf2 and persist.
        """
        stored = (self.password_hash or "")

        # Attempt primary verification using passlib context.
        try:
            valid = _password_context.verify(password, stored)
        except Exception as exc:
            # Log the failure for debugging.
            current_app.logger.exception("Password verification raised an exception in passlib", exc_info=exc)

            # Fallback: try direct bcrypt verify if stored hash looks like bcrypt.
            # This only runs if the bcrypt package is installed and working.
            try:
                if stored.startswith("$2"):
                    import bcrypt as _bcrypt  # may raise if bcrypt package broken
                    ok = _bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
                    if ok:
                        # migrate to the default scheme (pbkdf2_sha256)
                        try:
                            self.password_hash = _password_context.hash(password)
                            db.session.add(self)
                            db.session.commit()
                        except Exception:
                            # If commit fails, do not block login; log and continue.
                            current_app.logger.exception("Failed to persist migrated password hash")
                        return True
                    return False
            except Exception:
                # bcrypt fallback not available or failed.
                current_app.logger.exception("bcrypt fallback verification failed or bcrypt not installed")
                return False

        # If passlib verification succeeded:
        if valid:
            # If the stored hash needs to be updated to the default scheme, do it now.
            try:
                if _password_context.needs_update(stored):
                    self.password_hash = _password_context.hash(password)
                    db.session.add(self)
                    db.session.commit()
            except Exception:
                # Don't block login on persistence problems; log and continue.
                current_app.logger.exception("Failed to persist rehashed password after login")
            return True

        return False

    def has_role(self, role: str) -> bool:
        """Determine whether the user has a specific role."""
        return any(r.name == role for r in self.roles)

    def has_any_role(self, roles: list[str] | tuple[str, ...] | set[str]) -> bool:
        """Determine whether the user has any role from the provided collection."""
        role_names = {r.name for r in self.roles}
        sought = {str(role) for role in roles}
        return bool(role_names.intersection(sought))

    @property
    def is_active(self) -> bool:  # type: ignore[override]
        """Expose the activation flag for Flask-Login."""
        return bool(self._is_active)

    @is_active.setter
    def is_active(self, value: bool) -> None:
        self._is_active = bool(value)

    def get_id(self) -> str:  # type: ignore[override]
        return str(self.id)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<User {self.email}>"
