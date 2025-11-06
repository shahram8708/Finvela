"""Compliance orchestration pipeline coordinating GST, HSN, and arithmetic checks."""
from __future__ import annotations

import threading
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple

from flask import current_app

from expenseai_compliance import arithmetic, gst_provider, hsn_service
from expenseai_compliance.models import CheckResult, CheckStatus, CheckType, Finding, FindingSeverity
from expenseai_ext.db import db
from expenseai_models import AuditLog, ComplianceCheck, ComplianceFinding, Invoice, InvoiceEvent


def run_compliance_async(invoice_id: int, actor: str = "system") -> None:
    """Fire-and-forget background compliance run."""
    app = current_app._get_current_object()
    thread = threading.Thread(target=_run_with_context, args=(app, invoice_id, actor), name="compliance-runner", daemon=True)
    thread.start()


def _run_with_context(app, invoice_id: int, actor: str) -> None:
    with app.app_context():
        run_compliance(invoice_id, actor=actor)


def run_compliance(invoice_id: int, actor: str = "system") -> None:
    """Execute the compliance pipeline for a single invoice."""
    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        current_app.logger.warning("Compliance run invoked for missing invoice", extra={"invoice_id": invoice_id})
        return

    try:
        invoice.set_compliance_status("IN_PROGRESS", notes=None)
        InvoiceEvent.record(
            invoice,
            "COMPLIANCE_STARTED",
            {"invoice_id": invoice.id, "actor": actor, "timestamp": datetime.utcnow().isoformat() + "Z"},
        )
        db.session.commit()

        AuditLog.log(action="compliance_run_started", entity="invoice", entity_id=invoice.id, data={"actor": actor})

        # Clear previous results before recomputing
        ComplianceCheck.query.filter_by(invoice_id=invoice.id).delete()
        ComplianceFinding.query.filter_by(invoice_id=invoice.id).delete()
        db.session.commit()

        checks: List[CheckResult] = []
        checks.append(_run_gst_check(invoice, kind=CheckType.GST_VENDOR, value=invoice.vendor_gst))
        checks.append(_run_gst_check(invoice, kind=CheckType.GST_COMPANY, value=invoice.company_gst))
        checks.append(_run_hsn_check(invoice))
        checks.append(_run_arithmetic_check(invoice))

        overall_score = _persist_results(invoice, checks)
        invoice.set_compliance_status("READY")
        invoice.compliance_notes = f"Composite compliance score: {overall_score:.2f}"
        summary_payload = {
            "invoice_id": invoice.id,
            "score": overall_score,
            "checks": [check.check_type.value for check in checks],
        }
        InvoiceEvent.record(invoice, "COMPLIANCE_RESULT_SUMMARY", summary_payload)
        db.session.commit()
        AuditLog.log(action="compliance_run_completed", entity="invoice", entity_id=invoice.id, data=summary_payload)
    except Exception as exc:  # pragma: no cover - defensive path
        db.session.rollback()
        invoice = db.session.get(Invoice, invoice_id)
        if invoice:
            invoice.set_compliance_status("ERROR", notes=str(exc))
            InvoiceEvent.record(
                invoice,
                "COMPLIANCE_RESULT_SUMMARY",
                {"invoice_id": invoice.id, "error": str(exc)},
            )
            db.session.commit()
        current_app.logger.exception("Compliance run failed", extra={"invoice_id": invoice_id})


def _run_gst_check(invoice: Invoice, *, kind: CheckType, value: str | None) -> CheckResult:
    provider = gst_provider.get_provider()
    findings: List[Finding] = []
    details: Dict[str, object] = {}
    if not value:
        findings.append(
            Finding(
                check_type=kind,
                severity=FindingSeverity.WARN,
                code="GST_MISSING",
                message="GSTIN not provided",
            )
        )
        return CheckResult(check_type=kind, status=CheckStatus.WARN, summary="Missing GSTIN", findings=findings, details=details)

    gstin = gst_provider.normalize_gstin(value)

    details.update({"gstin": gstin})
    response = provider.validate_gstin(gstin)
    details.update({"provider_status": response.get("status"), "raw": response.get("raw", {})})
    status = gst_provider.classify_provider_status(response)
    summary = "GSTIN validation completed"
    score = 1.0 if status == CheckStatus.PASS else (0.5 if status == CheckStatus.WARN else 0.0)

    if status == CheckStatus.PASS and response.get("legal_name"):
        details["legal_name"] = response.get("legal_name")

    if status == CheckStatus.FAIL:
        findings.append(
            Finding(
                check_type=kind,
                severity=FindingSeverity.FAIL,
                code="GST_PORTAL_MISMATCH",
                message="GST portal reported the GSTIN as inactive or invalid",
                evidence={"gstin": gstin, "provider_status": response.get("status")},
            )
        )
    elif status == CheckStatus.NEEDS_API:
        findings.append(
            Finding(
                check_type=kind,
                severity=FindingSeverity.WARN,
                code="GST_PROVIDER_UNCONFIGURED",
                message="Configure GST provider API credentials to enable live validation",
                evidence={},
            )
        )
        summary = "GST provider credentials required"
    elif status == CheckStatus.WARN:
        findings.append(
            Finding(
                check_type=kind,
                severity=FindingSeverity.WARN,
                code="GST_PROVIDER_WARN",
                message=f"Provider returned status {response.get('reason', 'unknown')}",
                evidence={"gstin": gstin},
            )
        )
    return CheckResult(check_type=kind, status=status, summary=summary, score=score, details=details, findings=findings)


def _run_hsn_check(invoice: Invoice) -> CheckResult:
    findings: List[Finding] = []
    details: Dict[str, object] = {"lines": []}
    if not invoice.line_items:
        return CheckResult(CheckType.HSN_RATE, CheckStatus.WARN, "No line items available for rate check")

    loaded_stats = hsn_service.stats()
    if loaded_stats.get("count", 0) == 0:
        findings.append(
            Finding(
                check_type=CheckType.HSN_RATE,
                severity=FindingSeverity.WARN,
                code="HSN_TABLE_EMPTY",
                message="HSN rate table is empty. Upload a CSV under Admin > HSN Rates.",
            )
        )
        return CheckResult(CheckType.HSN_RATE, CheckStatus.WARN, "HSN rate table empty", findings=findings)

    mismatches: List[Dict[str, object]] = []
    counterfactual_lines: List[Dict[str, Decimal]] = []
    invoice_date = invoice.invoice_date or datetime.utcnow().date()
    for item in invoice.line_items:
        expected_rate_entry = hsn_service.get_rate(item.hsn_sac, invoice_date)
        record = {
            "line_no": item.line_no,
            "hsn_sac": item.hsn_sac,
            "expected_rate": float(expected_rate_entry.gst_rate) if expected_rate_entry else None,
            "billed_rate": float(item.gst_rate) if item.gst_rate is not None else None,
        }
        details["lines"].append(record)
        if not expected_rate_entry or item.gst_rate is None:
            continue
        expected_rate = Decimal(expected_rate_entry.gst_rate)
        billed_rate = Decimal(item.gst_rate)
        if abs(expected_rate - billed_rate) >= Decimal("0.01"):
            findings.append(
                Finding(
                    check_type=CheckType.HSN_RATE,
                    severity=FindingSeverity.WARN,
                    code="HSN_RATE_MISMATCH",
                    message=f"Line {item.line_no} billed GST {billed_rate}% expected {expected_rate}%",
                    evidence={
                        "line_no": item.line_no,
                        "expected_rate": float(expected_rate),
                        "billed_rate": float(billed_rate),
                    },
                )
            )
            mismatches.append(record)
            counterfactual_lines.append(
                {
                    "line_no": item.line_no,
                    "qty": item.qty,
                    "unit_price": item.unit_price,
                    "gst_rate": expected_rate,
                    "line_subtotal": item.line_subtotal,
                    "line_tax": item.line_tax,
                    "line_total": item.line_total,
                }
            )

    status = CheckStatus.PASS if not mismatches else CheckStatus.WARN
    summary = "HSN rates aligned" if not mismatches else f"{len(mismatches)} line(s) differ from expected HSN rates"
    details["mismatches"] = mismatches

    if counterfactual_lines:
        subtotal, tax_total, grand_total, _diffs = arithmetic.recompute_invoice_totals(counterfactual_lines)
        corrected = {
            "subtotal": float(subtotal),
            "tax_total": float(tax_total),
            "grand_total": float(grand_total),
        }
        current_totals = {
            "subtotal": float(invoice.subtotal or 0),
            "tax_total": float(invoice.tax_total or 0),
            "grand_total": float(invoice.grand_total or 0),
        }
        details["counterfactual"] = {
            "corrected_totals": corrected,
            "delta": {
                "tax_total_delta": corrected["tax_total"] - current_totals["tax_total"],
                "grand_total_delta": corrected["grand_total"] - current_totals["grand_total"],
            },
        }
    return CheckResult(CheckType.HSN_RATE, status, summary, score=1.0 if status == CheckStatus.PASS else 0.6, details=details, findings=findings)


def _run_arithmetic_check(invoice: Invoice) -> CheckResult:
    if not invoice.line_items:
        return CheckResult(CheckType.ARITHMETIC, CheckStatus.WARN, "No line items available for arithmetic check")

    epsilon = Decimal(str(current_app.config.get("ARITH_EPSILON", 0.01)))
    line_payload = [
        {
            "line_no": item.line_no,
            "qty": item.qty,
            "unit_price": item.unit_price,
            "gst_rate": item.gst_rate,
            "line_subtotal": item.line_subtotal,
            "line_tax": item.line_tax,
            "line_total": item.line_total,
        }
        for item in invoice.line_items
    ]
    subtotal, tax_total, grand_total, diffs = arithmetic.recompute_invoice_totals(line_payload)
    findings: List[Finding] = []
    for diff in diffs["lines"]:
        if abs(diff["subtotal_diff"]) >= epsilon or abs(diff["tax_diff"]) >= epsilon or abs(diff["total_diff"]) >= epsilon:
            findings.append(
                Finding(
                    check_type=CheckType.ARITHMETIC,
                    severity=FindingSeverity.FAIL,
                    code="ARITH_LINE_MISMATCH",
                    message=f"Line {diff['line_no']} totals differ from recomputed values",
                    evidence={
                        "expected_subtotal": float(diff["expected_subtotal"]),
                        "stored_subtotal": float(diff["expected_subtotal"] + diff["subtotal_diff"]),
                        "expected_tax": float(diff["expected_tax"]),
                        "stored_tax": float(diff["expected_tax"] + diff["tax_diff"]),
                    },
                )
            )
    subtotal_diff = (invoice.subtotal or Decimal(0)) - subtotal
    tax_diff = (invoice.tax_total or Decimal(0)) - tax_total
    grand_diff = (invoice.grand_total or Decimal(0)) - grand_total
    header_diffs_dec = {
        "subtotal_diff": subtotal_diff,
        "tax_total_diff": tax_diff,
        "grand_total_diff": grand_diff,
    }
    if any(abs(value) >= epsilon for value in header_diffs_dec.values()):
        findings.append(
            Finding(
                check_type=CheckType.ARITHMETIC,
                severity=FindingSeverity.FAIL,
                code="ARITH_SUM_MISMATCH",
                message="Invoice totals differ from recomputed totals",
                evidence={k: float(v) for k, v in header_diffs_dec.items()},
            )
        )

    status = CheckStatus.PASS if not findings else CheckStatus.FAIL
    summary = "Arithmetic verified" if status == CheckStatus.PASS else "Arithmetic discrepancies detected"
    details = {
        "expected_totals": {
            "subtotal": float(subtotal),
            "tax_total": float(tax_total),
            "grand_total": float(grand_total),
        },
    "header_diffs": {k: float(v) for k, v in header_diffs_dec.items()},
        "line_diffs": [
            {
                "line_no": diff["line_no"],
                "subtotal_diff": float(diff["subtotal_diff"]),
                "tax_diff": float(diff["tax_diff"]),
                "total_diff": float(diff["total_diff"]),
            }
            for diff in diffs["lines"]
        ],
    }
    return CheckResult(CheckType.ARITHMETIC, status, summary, score=1.0 if status == CheckStatus.PASS else 0.2, details=details, findings=findings)


def _persist_results(invoice: Invoice, results: Iterable[CheckResult]) -> float:
    scores: List[float] = []
    for result in results:
        check_record = (
            ComplianceCheck.query.filter_by(invoice_id=invoice.id, check_type=result.check_type.value)
            .order_by(ComplianceCheck.id.asc())
            .first()
        )
        if not check_record:
            check_record = ComplianceCheck(invoice=invoice, check_type=result.check_type.value)
        check_record.status = result.status.value
        check_record.summary = result.summary
        check_record.score = result.score
        check_record.details_json = result.details
        db.session.add(check_record)

        ComplianceFinding.query.filter_by(invoice_id=invoice.id, check_type=result.check_type.value).delete()
        for finding in result.findings:
            db.session.add(
                ComplianceFinding(
                    invoice=invoice,
                    check_type=finding.check_type.value,
                    severity=finding.severity.value,
                    code=finding.code,
                    message=finding.message,
                    evidence_json=finding.evidence,
                )
            )
        if result.score is not None:
            scores.append(result.score)

    db.session.commit()
    if not scores:
        return 1.0
    return sum(scores) / len(scores)
