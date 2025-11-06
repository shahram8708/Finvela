"""Contextual bandit policy persistence."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class BanditPolicy(db.Model):
    """Stores serialized LinUCB weight vectors."""

    __tablename__ = "bandit_policies"
    __table_args__ = (Index("ix_bandit_policy_version", "version", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    weights_json: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    alpha: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    def weights(self) -> dict[str, float]:
        """Return weights as a float mapping."""
        raw = self.weights_json or {}
        return {str(key): float(value) for key, value in raw.items()}
