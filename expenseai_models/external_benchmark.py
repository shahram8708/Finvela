"""External benchmark catalog entries uploaded by administrators."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class ExternalBenchmark(db.Model):
    """Median price baselines sourced from external catalogs."""

    __tablename__ = "external_benchmarks"
    __table_args__ = (
        Index("ix_external_benchmark_text", "text_norm"),
        Index("ix_external_benchmark_effective", "text_norm", "effective_from"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    text_norm: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    median_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    mad: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    n: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    def is_active(self, target: date | None) -> bool:
        """Return whether the benchmark applies to the target date."""
        if target is None:
            return True
        if self.effective_from and target < self.effective_from:
            return False
        if self.effective_to and target > self.effective_to:
            return False
        return True
