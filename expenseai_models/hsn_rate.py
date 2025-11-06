"""HSN/SAC rate table managed by administrators."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict

from sqlalchemy import Date, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class HsnRate(db.Model):
    """Represents a tax rate entry for a specific HSN/SAC code and period."""

    __tablename__ = "hsn_rates"
    __table_args__ = (
        Index("ix_hsn_code_effective", "code", "effective_from", unique=True),
        Index("ix_hsn_code", "code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    gst_rate: Mapped[Decimal] = mapped_column(Numeric(7, 3), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def as_dict(self) -> Dict[str, object]:
        """Serialize for JSON responses."""
        return {
            "id": self.id,
            "code": self.code,
            "description": self.description,
            "gst_rate": float(self.gst_rate),
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
        }
