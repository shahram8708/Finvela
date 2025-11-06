"""Domain services that encapsulate auth-related persistence logic."""
from __future__ import annotations

import re
import secrets
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

from flask import current_app

from expenseai_ext.db import db
from expenseai_models.audit import AuditLog
from expenseai_models.organization import Organization, RegistrationInvite
from expenseai_models.role import Role
from expenseai_models.user import User


class UserService:
    """CRUD helpers focused on the user lifecycle."""

    @staticmethod
    def get_by_email(email: str) -> Optional[User]:
        return User.query.filter_by(email=email.lower().strip()).first()

    @staticmethod
    def verify_password(user: User, password: str) -> bool:
        return user.verify_password(password)

    @staticmethod
    def create_user(
        full_name: str,
        email: str,
        password: str,
        roles: Iterable[str] | None = None,
        organization: Organization | None = None,
        is_active: bool = True,
        approved_at: datetime | None = None,
    ) -> User:
        if UserService.get_by_email(email):
            raise ValueError("Email is already registered")
        if organization is not None:
            OrganizationService.ensure_can_add_members(organization)
        user = User(full_name=full_name.strip(), email=email.lower().strip(), is_active=is_active)
        user.is_active = is_active
        if organization is not None:
            user.organization = organization
        if is_active:
            user.approved_at = approved_at or datetime.utcnow()
        user.set_password(password)
        if roles:
            for role_name in roles:
                role = Role.query.filter_by(name=role_name).first()
                if not role:
                    role = Role(name=role_name, description=f"Auto-created role: {role_name}")
                    db.session.add(role)
                user.roles.append(role)
        db.session.add(user)
        db.session.commit()
        current_app.logger.info("New user created", extra={"user_id": user.id})
        return user

    @staticmethod
    def ensure_role(name: str, description: str | None = None) -> Role:
        role = Role.query.filter_by(name=name).first()
        if role:
            return role
        role = Role(name=name, description=description or name.title())
        db.session.add(role)
        db.session.commit()
        return role


@dataclass(frozen=True)
class OrganizationUsageSummary:
    """Aggregated view of membership usage for an organization."""

    total: int
    active: int
    pending: int
    limit: int
    remaining: int
    free_limit: int

    @property
    def limit_reached(self) -> bool:
        return self.remaining <= 0


class OrganizationService:
    """Helpers for tenant organizations, invites and member approvals."""

    _SLUG_PATTERN = re.compile(r"[^a-z0-9]+")
    LIMIT_REACHED_MESSAGE = "You’ve reached the free user limit. Please upgrade your plan to add more users."

    @staticmethod
    def create_organization(name: str, admin: User, *, activate_admin: bool = True) -> Organization:
        """Create a new organization and assign the admin as its first member."""
        cleaned_name = name.strip()
        if not cleaned_name:
            raise ValueError("Organization name is required")
        if admin.organization is not None:
            raise ValueError("Admin already belongs to an organization")
        if Organization.query.filter_by(name=cleaned_name).first():
            raise ValueError("Organization name is already in use")
        slug = OrganizationService._generate_unique_slug(cleaned_name)
        organization = Organization(
            name=cleaned_name,
            slug=slug,
            user_limit=OrganizationService.default_user_limit(),
            is_premium=False,
        )
        db.session.add(organization)
        db.session.flush()
        admin.organization = organization
        if activate_admin:
            admin.is_active = True
            admin.approved_at = admin.approved_at or datetime.utcnow()
        db.session.add(admin)
        db.session.commit()
        AuditLog.log(
            action="organization_created",
            entity="organization",
            entity_id=organization.id,
            data={"name": organization.name, "admin_id": admin.id},
        )
        return organization

    @staticmethod
    def list_invites(organization: Organization) -> list[RegistrationInvite]:
        """Return the organization's invites ordered by creation time."""
        return (
            RegistrationInvite.query.filter_by(organization_id=organization.id)
            .order_by(RegistrationInvite.created_at.desc())
            .all()
        )

    @staticmethod
    def list_pending_members(organization: Organization) -> list[User]:
        """Users awaiting admin approval within the organization."""
        return (
            User.query.filter(
                User.organization_id == organization.id,
                User._is_active.is_(False),
                User.approved_at.is_(None),
            )
            .order_by(User.created_at.asc())
            .all()
        )

    @staticmethod
    def list_active_members(organization: Organization) -> list[User]:
        """Users already approved and active within the organization."""
        return (
            User.query.filter(
                User.organization_id == organization.id,
                User._is_active.is_(True),
                User.approved_at.is_not(None),
            )
            .order_by(User.full_name.asc())
            .all()
        )

    @staticmethod
    def get_member(organization: Organization, user_id: int) -> User | None:
        """Fetch a member belonging to the organization by identifier."""
        return (
            User.query.filter(
                User.id == user_id,
                User.organization_id == organization.id,
            )
            .limit(1)
            .one_or_none()
        )

    @staticmethod
    def approve_member(admin: User, member: User) -> None:
        """Mark a pending member as active, recording audit metadata."""
        if not admin.organization or member.organization_id != admin.organization.id:
            raise ValueError("Member does not belong to this organization")
        if member.is_active:
            raise ValueError("Member is already active")
        member.is_active = True
        member.approved_at = datetime.utcnow()
        db.session.add(member)
        db.session.commit()
        AuditLog.log(
            action="user_approved",
            entity="user",
            entity_id=member.id,
            data={"organization_id": member.organization_id, "approved_by": admin.id},
        )

    @staticmethod
    def issue_invite(
        admin: User,
        *,
        expires_in_hours: int | None = None,
        max_uses: int | None = None,
    ) -> RegistrationInvite:
        """Generate a new invitation code for the admin's organization."""
        if not admin.organization:
            raise ValueError("Admin must belong to an organization to issue invites")
        OrganizationService.ensure_can_add_members(admin.organization)
        length = max(6, int(current_app.config.get("INVITE_CODE_LENGTH", 12)))
        code = OrganizationService._generate_unique_code(length)
        if expires_in_hours is None:
            default_hours = current_app.config.get("INVITE_CODE_EXPIRY_HOURS")
            expires_in_hours = int(default_hours) if default_hours else None
        expiry = None
        if expires_in_hours:
            expiry = datetime.utcnow() + timedelta(hours=expires_in_hours)
        if max_uses is None:
            configured_max = current_app.config.get("INVITE_CODE_MAX_USES")
            max_uses = int(configured_max) if configured_max else None
        invite = RegistrationInvite(
            code=code,
            organization=admin.organization,
            created_by=admin,
            expires_at=expiry,
            max_uses=max_uses,
        )
        db.session.add(invite)
        db.session.commit()
        AuditLog.log(
            action="invite_created",
            entity="registration_invite",
            entity_id=invite.id,
            data={"organization_id": invite.organization_id, "code": invite.code},
        )
        return invite

    @staticmethod
    def validate_invite(code: str) -> RegistrationInvite | None:
        """Validate and return an invite if it is currently usable."""
        normalized = code.strip().upper()
        invite = RegistrationInvite.query.filter_by(code=normalized).first()
        if not invite or not invite.can_be_used():
            return None
        if invite.organization and not OrganizationService.can_add_members(invite.organization):
            return None
        return invite

    @staticmethod
    def consume_invite(invite: RegistrationInvite) -> None:
        """Record usage of an invite and persist it."""
        invite.mark_used()
        db.session.add(invite)
        db.session.commit()
        AuditLog.log(
            action="invite_consumed",
            entity="registration_invite",
            entity_id=invite.id,
            data={"organization_id": invite.organization_id, "use_count": invite.use_count},
        )

    @staticmethod
    def _generate_unique_slug(name: str) -> str:
        base = OrganizationService._SLUG_PATTERN.sub("-", name.lower()).strip("-") or "org"
        slug = base
        suffix = 1
        while Organization.query.filter_by(slug=slug).first() is not None:
            slug = f"{base}-{suffix}"
            suffix += 1
        return slug

    @staticmethod
    def _generate_unique_code(length: int) -> str:
        alphabet = string.ascii_uppercase + string.digits
        while True:
            candidate = "".join(secrets.choice(alphabet) for _ in range(length))
            if not RegistrationInvite.query.filter_by(code=candidate).first():
                return candidate

    @staticmethod
    def default_user_limit() -> int:
        return int(current_app.config.get("ORG_FREE_USER_LIMIT", 5))

    @staticmethod
    def member_usage(organization: Organization) -> OrganizationUsageSummary:
        """Compute membership utilisation and remaining seats for an organization."""
        total = (
            User.query.filter(User.organization_id == organization.id)
            .with_entities(User.id)
            .count()
        )
        active = (
            User.query.filter(
                User.organization_id == organization.id,
                User._is_active.is_(True),
            )
            .with_entities(User.id)
            .count()
        )
        pending = max(total - active, 0)
        limit = organization.user_limit or OrganizationService.default_user_limit()
        remaining = max(limit - total, 0)
        free_limit = OrganizationService.default_user_limit()
        return OrganizationUsageSummary(
            total=total,
            active=active,
            pending=pending,
            limit=limit,
            remaining=remaining,
            free_limit=free_limit,
        )

    @staticmethod
    def can_add_members(organization: Organization, additional_users: int = 1) -> bool:
        usage = OrganizationService.member_usage(organization)
        return usage.remaining >= max(additional_users, 0)

    @staticmethod
    def ensure_can_add_members(organization: Organization, additional_users: int = 1) -> None:
        if not OrganizationService.can_add_members(organization, additional_users):
            raise ValueError(OrganizationService.LIMIT_REACHED_MESSAGE)
