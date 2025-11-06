"""Role model supporting RBAC."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


if TYPE_CHECKING:  # pragma: no cover - used only during type checking
    from expenseai_models.user import User


class Role(db.Model):
    """Roles assign fine-grained permissions to users."""

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    users: Mapped[list["User"]] = relationship(
        "User",
        secondary="user_roles",
        back_populates="roles",
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Role {self.name}>"
