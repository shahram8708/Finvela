"""Composite risk scoring persistence models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db


class RiskScore(db.Model):
    """Aggregate risk score for a given invoice."""

    __tablename__ = "risk_scores"
    __table_args__ = (Index("ix_risk_scores_invoice", "invoice_id", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(
        db.ForeignKey("invoices.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    composite: Mapped[float] = mapped_column(Float, nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    policy_version: Mapped[str] = mapped_column(String(32), nullable=False, default="seed")
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    invoice = relationship("Invoice", back_populates="risk_score", uselist=False)
    contributors = relationship(
        "RiskContributor",
        back_populates="risk_score",
        cascade="all, delete-orphan",
        order_by="RiskContributor.contribution.desc()",
    )

    def as_dict(self) -> dict[str, object]:
        """Serialize score and contributors."""
        return {
            "invoice_id": self.invoice_id,
            "composite": self.composite,
            "version": self.version,
            "policy_version": self.policy_version,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
            "contributors": [contrib.as_dict() for contrib in self.contributors],
        }


class RiskContributor(db.Model):
    """Individual risk contributor entries stored alongside the composite score."""

    __tablename__ = "risk_contributors"
    __table_args__ = (
        Index("ix_risk_contributors_score_name", "risk_score_id", "name", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    risk_score_id: Mapped[int] = mapped_column(
        db.ForeignKey("risk_scores.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False)
    raw_score: Mapped[float] = mapped_column(Float, nullable=False)
    contribution: Mapped[float] = mapped_column(Float, nullable=False)
    details_json: Mapped[dict[str, object] | None] = mapped_column(db.JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    risk_score = relationship("RiskScore", back_populates="contributors")

    def as_dict(self) -> dict[str, object]:
        """Return JSON-friendly representation."""
        return {
            "name": self.name,
            "weight": self.weight,
            "raw_score": self.raw_score,
            "contribution": self.contribution,
            "details": self.details_json or {},
        }
