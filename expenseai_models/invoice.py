"""Invoice placeholder model for future enrichment."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

from sqlalchemy import BigInteger, Date, DateTime, Float, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from expenseai_ext.db import db

if TYPE_CHECKING:  # pragma: no cover - import only for type checking
    from expenseai_models.invoice_event import InvoiceEvent
    from expenseai_models.compliance_check import ComplianceCheck
    from expenseai_models.compliance_finding import ComplianceFinding
    from expenseai_models.extracted_field import ExtractedField
    from expenseai_models.feedback import Feedback
    from expenseai_models.line_item import LineItem
    from expenseai_models.bandit_example import BanditExample
    from expenseai_models.risk_score import RiskScore
    from expenseai_models.price_benchmark import PriceBenchmark
    from expenseai_models.organization import Organization
    from expenseai_models.user import User


INVOICE_STATUSES = ("UPLOADED", "QUEUED", "PARSING", "READY", "ERROR")
COMPLIANCE_STATUSES = ("PENDING", "IN_PROGRESS", "READY", "ERROR")
RISK_STATUSES = ("PENDING", "IN_PROGRESS", "READY", "ERROR")


class Invoice(db.Model):
    """Invoice representation with helper methods for workflow state."""

    __tablename__ = "invoices"
    __table_args__ = (
        Index("ix_invoice_vendor_invoice", "vendor_gst", "invoice_no"),
        Index("ix_invoice_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    vendor_gst: Mapped[str | None] = mapped_column(String(64), nullable=True)
    company_gst: Mapped[str | None] = mapped_column(String(64), nullable=True)
    invoice_no: Mapped[str | None] = mapped_column(String(64), nullable=True)
    invoice_date: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    subtotal: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    tax_total: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    grand_total: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    source_path: Mapped[str | None] = mapped_column(String(255), nullable=True)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    stored_filename: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    mime_type: Mapped[str] = mapped_column(String(64), nullable=False)
    filesize_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    thumbnail_path: Mapped[str | None] = mapped_column(String(255), nullable=True)
    processing_status: Mapped[str] = mapped_column(String(32), nullable=False, default="UPLOADED")
    processing_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False, index=True)
    extraction_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    extracted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    pages_parsed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    compliance_status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    compliance_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    risk_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    organization_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("organizations.id"), nullable=True, index=True)
    organization: Mapped["Organization | None"] = relationship("Organization", back_populates="invoices")
    assignee_id: Mapped[int | None] = mapped_column(Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    assignee: Mapped["User | None"] = relationship("User", back_populates="assigned_invoices", foreign_keys="Invoice.assignee_id")

    events: Mapped[list["InvoiceEvent"]] = relationship(
        "InvoiceEvent",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="InvoiceEvent.created_at.desc()",
    )
    extracted_fields: Mapped[list["ExtractedField"]] = relationship(
        "ExtractedField",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="ExtractedField.field_name.asc()",
    )
    line_items: Mapped[list["LineItem"]] = relationship(
        "LineItem",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="LineItem.line_no.asc()",
    )
    compliance_checks: Mapped[list["ComplianceCheck"]] = relationship(
        "ComplianceCheck",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="ComplianceCheck.check_type.asc()",
    )
    price_benchmarks: Mapped[list["PriceBenchmark"]] = relationship(
        "PriceBenchmark",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="PriceBenchmark.created_at.desc()",
    )
    compliance_findings: Mapped[list["ComplianceFinding"]] = relationship(
        "ComplianceFinding",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="ComplianceFinding.created_at.desc()",
    )
    risk_score: Mapped["RiskScore | None"] = relationship(
        "RiskScore",
        back_populates="invoice",
        cascade="all, delete-orphan",
        uselist=False,
    )

    feedback_entries: Mapped[list["Feedback"]] = relationship(
        "Feedback",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="Feedback.created_at.desc()",
    )
    bandit_examples: Mapped[list["BanditExample"]] = relationship(
        "BanditExample",
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="BanditExample.created_at.desc()",
    )

    def set_status(self, status: str, notes: str | None = None, emit_event: bool = True) -> None:
        """Update the processing status and optionally emit an event."""
        status = status.upper()
        if status not in INVOICE_STATUSES:
            raise ValueError(f"Invalid invoice status: {status}")
        previous = self.processing_status
        self.processing_status = status
        if notes:
            self.processing_notes = notes
        if emit_event:
            from expenseai_models.invoice_event import InvoiceEvent

            payload: Dict[str, Any] = {"from": previous, "to": status}
            if notes:
                payload["notes"] = notes
            InvoiceEvent.record(self, "STATUS_CHANGED", payload)

    def set_compliance_status(self, status: str, notes: str | None = None, emit_event: bool = True) -> None:
        """Update compliance status with validation and optional event emission."""
        status = status.upper()
        if status not in COMPLIANCE_STATUSES:
            raise ValueError(f"Invalid compliance status: {status}")
        previous = self.compliance_status
        self.compliance_status = status
        if notes:
            self.compliance_notes = notes
        if emit_event:
            from expenseai_models.invoice_event import InvoiceEvent

            payload: Dict[str, Any] = {"from": previous, "to": status}
            if notes:
                payload["notes"] = notes
            InvoiceEvent.record(self, "STATUS_CHANGED", payload | {"category": "compliance"})

    def set_risk_status(self, status: str, notes: str | None = None, emit_event: bool = True) -> None:
        """Update the risk scoring status for this invoice."""
        status = status.upper()
        if status not in RISK_STATUSES:
            raise ValueError(f"Invalid risk status: {status}")
        previous = self.risk_status
        self.risk_status = status
        if notes:
            self.risk_notes = notes
        if emit_event:
            from expenseai_models.invoice_event import InvoiceEvent

            payload: Dict[str, Any] = {"from": previous, "to": status, "category": "risk"}
            if notes:
                payload["notes"] = notes
            InvoiceEvent.record(self, "RISK_STATUS_CHANGED", payload)

    def public_url(self) -> str:
        """Return the URL clients can use to download the original file."""
        from flask import url_for

        return url_for("expenseai_invoices.get_invoice_file", stored=self.stored_filename)

    def thumbnail_url(self) -> str | None:
        """Return a URL for the thumbnail if present."""
        if not self.thumbnail_path:
            return None
        from flask import url_for

        return url_for("expenseai_invoices.get_invoice_thumbnail", stored=self.thumbnail_path)

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Invoice {self.id} {self.processing_status}>"
