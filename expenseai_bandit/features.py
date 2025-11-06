"""Construct feature vectors for contextual bandit updates."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from flask import current_app

from expenseai_benchmark import service as benchmark_service
from expenseai_models.compliance_check import ComplianceCheck
from expenseai_models.invoice import Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_models.risk_score import RiskScore
from expenseai_models.vendor_drift import VendorDrift
from expenseai_models.vendor_profile import VendorProfile

CONTRIBUTOR_KEYS: tuple[str, ...] = (
    "market_outlier",
    "arithmetic",
    "hsn_rate",
    "gst_vendor",
    "gst_company",
    "duplicate",
)

EXTRA_FEATURE_KEYS: tuple[str, ...] = (
    "extraction_confidence",
    "subtotal_scaled",
    "tax_total_scaled",
    "grand_total_scaled",
    "top_outlier_rz",
    "vendor_drift_score",
    "vendor_history_n",
    "invoice_age_days",
    "has_counterfactual",
    "whatsapp_alert_sent",
)

FEATURE_ORDER: tuple[str, ...] = CONTRIBUTOR_KEYS + EXTRA_FEATURE_KEYS


@dataclass(slots=True)
class BanditContext:
    """Named feature mapping with deterministic order."""

    version: str
    names: List[str]
    values: List[float]
    mapping: Dict[str, float]


def build_context(invoice: Invoice) -> BanditContext:
    """Construct context features for the supplied invoice."""
    context_version = current_app.config.get("BANDIT_CONTEXT_VERSION", "v1")
    mapping: Dict[str, float] = {key: 0.0 for key in FEATURE_ORDER}

    compliance = {
        check.check_type: check
        for check in ComplianceCheck.query.filter_by(invoice_id=invoice.id).all()
    }

    def _compliance_flag(key: str) -> float:
        check = compliance.get(key)
        status = (check.status or "").upper() if check else ""
        if status in {"FAIL", "ERROR"}:
            return 1.0
        if status in {"WARN", "NEEDS_API"}:
            return 0.5
        return 0.0

    mapping["arithmetic"] = _compliance_flag("ARITHMETIC")
    mapping["hsn_rate"] = _compliance_flag("HSN_RATE")
    mapping["gst_vendor"] = _compliance_flag("GST_VENDOR")
    mapping["gst_company"] = _compliance_flag("GST_COMPANY")
    mapping["duplicate"] = 0.0  # placeholder until duplicate detection implemented

    summary = None
    try:
        summary = benchmark_service.benchmark_invoice(invoice.id)
    except Exception:  # pragma: no cover - benchmark may raise during tests
        summary = None
    if summary:
        mapping["market_outlier"] = float(summary.get("avg_outlier_score") or 0.0)
        lines = summary.get("lines") or []
        if lines:
            mapping["top_outlier_rz"] = float(lines[0].get("robust_z") or 0.0)

    if invoice.extraction_confidence is not None:
        mapping["extraction_confidence"] = float(invoice.extraction_confidence)

    def _scale(value) -> float:
        if value in (None, ""):
            return 0.0
        try:
            magnitude = float(value)
        except (TypeError, ValueError):
            return 0.0
        scale = max(1.0, abs(magnitude))
        return magnitude / scale

    mapping["subtotal_scaled"] = _scale(invoice.subtotal)
    mapping["tax_total_scaled"] = _scale(invoice.tax_total)
    mapping["grand_total_scaled"] = _scale(invoice.grand_total)

    profile = None
    if invoice.vendor_gst:
        profile = VendorProfile.query.filter_by(
            vendor_gst=invoice.vendor_gst,
            organization_id=invoice.organization_id,
        ).first()
    if profile is not None:
        mapping["vendor_history_n"] = float(profile.n_samples or 0)

    if invoice.vendor_gst:
        drift_record = (
            VendorDrift.query.filter_by(
                vendor_gst=invoice.vendor_gst,
                organization_id=invoice.organization_id,
            )
            .order_by(VendorDrift.created_at.desc())
            .first()
        )
        if drift_record is not None:
            mapping["vendor_drift_score"] = float(drift_record.drift_score)

    if invoice.invoice_date:
        mapping["invoice_age_days"] = float((datetime.utcnow().date() - invoice.invoice_date).days)

    event_exists = (
        InvoiceEvent.query.with_entities(InvoiceEvent.id)
        .filter(InvoiceEvent.invoice_id == invoice.id)
        .filter(InvoiceEvent.event_type == "COUNTERFACT_EVALUATED")
        .first()
    )
    mapping["has_counterfactual"] = 1.0 if event_exists else 0.0

    whatsapp_event = (
        InvoiceEvent.query.with_entities(InvoiceEvent.id)
        .filter(InvoiceEvent.invoice_id == invoice.id)
        .filter(InvoiceEvent.event_type.like("WHATSAPP%"))
        .first()
    )
    mapping["whatsapp_alert_sent"] = 1.0 if whatsapp_event else 0.0

    names = list(FEATURE_ORDER)
    values = [float(mapping[name]) for name in names]
    return BanditContext(version=context_version, names=names, values=values, mapping=mapping)


def vector_from_payload(payload: dict[str, object]) -> tuple[List[float], List[str]]:
    """Convert stored context payload back into an ordered vector."""
    features = payload.get("features") if isinstance(payload, dict) else None
    order = payload.get("order") if isinstance(payload, dict) else None
    if isinstance(features, dict):
        if isinstance(order, list) and order:
            return [float(features.get(name, 0.0)) for name in order], list(order)
        return [float(features.get(name, 0.0)) for name in FEATURE_ORDER], list(FEATURE_ORDER)
    return [0.0 for _ in FEATURE_ORDER], list(FEATURE_ORDER)
