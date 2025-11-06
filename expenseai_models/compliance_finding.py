"""Granular compliance findings captured for each invoice."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

from sqlalchemy import JSON, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover
    from expenseai_models.invoice import Invoice


class ComplianceFinding(db.Model):
    """Detailed compliance finding for a specific rule violation or info."""

    __tablename__ = "compliance_findings"
    __table_args__ = (
        Index("ix_compliance_findings_invoice", "invoice_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(db.ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    check_type: Mapped[str] = mapped_column(String(32), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_json: Mapped[Dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    invoice: Mapped["Invoice"] = relationship("Invoice", back_populates="compliance_findings")

    def as_dict(self) -> Dict[str, Any]:
        """Serialize the finding to a JSON friendly mapping."""
        return {
            "id": self.id,
            "invoice_id": self.invoice_id,
            "check_type": self.check_type,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "evidence": self.evidence_json or {},
            "created_at": self.created_at.isoformat() + "Z",
        }
