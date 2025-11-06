"""Core risk scoring computations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from flask import current_app

from expenseai_benchmark import service as benchmark_service
from expenseai_ext.db import db
from expenseai_models.compliance_check import ComplianceCheck
from expenseai_models.invoice import Invoice
from expenseai_models.risk_score import RiskContributor, RiskScore
from expenseai_risk.weights import resolve_weights


@dataclass(slots=True)
class Contributor:
    name: str
    raw_score: float
    details: Dict[str, Any]


STATUS_FAIL = {"FAIL", "ERROR"}
STATUS_WARN = {"WARN", "NEEDS_API"}


def collect_contributors(invoice_id: int, *, benchmark_summary: dict[str, Any] | None = None) -> List[Contributor]:
    """Gather contributor inputs for the composite risk score."""
    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        raise ValueError(f"Invoice {invoice_id} not found")

    summary = benchmark_summary or benchmark_service.benchmark_invoice(invoice_id)
    max_lines = current_app.config.get("RISK_WATERFALL_MAX_CONTRIBS", 8)

    lines = summary.get("lines", [])
    sorted_lines = sorted(lines, key=lambda item: item.get("outlier_score", 0.0), reverse=True)
    top_lines = sorted_lines[: max_lines // 2 or 1]
    contributors: List[Contributor] = [
        Contributor(
            name="market_outlier",
            raw_score=float(summary.get("avg_outlier_score", 0.0)),
            details={
                "top_outliers": top_lines,
                "currency": summary.get("currency"),
                "computed_at": summary.get("computed_at"),
            },
        )
    ]

    checks = {
        check.check_type: check
        for check in ComplianceCheck.query.filter_by(invoice_id=invoice_id).all()
    }

    def score_from_check(check: ComplianceCheck | None, *, fail_value: float = 1.0, warn_value: float = 0.5) -> float:
        if check is None:
            return 0.0
        status = (check.status or "").upper()
        if status in STATUS_FAIL:
            return fail_value
        if status in STATUS_WARN:
            return warn_value
        if status == "PASS":
            return 0.0
        return warn_value

    contributors.append(
        Contributor(
            name="arithmetic",
            raw_score=score_from_check(checks.get("ARITHMETIC")),
            details=checks.get("ARITHMETIC").details_json if checks.get("ARITHMETIC") else {},
        )
    )
    contributors.append(
        Contributor(
            name="hsn_rate",
            raw_score=score_from_check(checks.get("HSN_RATE")),
            details=checks.get("HSN_RATE").details_json if checks.get("HSN_RATE") else {},
        )
    )
    contributors.append(
        Contributor(
            name="gst_vendor",
            raw_score=score_from_check(checks.get("GST_VENDOR"), warn_value=0.5),
            details=checks.get("GST_VENDOR").details_json if checks.get("GST_VENDOR") else {},
        )
    )
    contributors.append(
        Contributor(
            name="gst_company",
            raw_score=score_from_check(checks.get("GST_COMPANY"), warn_value=0.5),
            details=checks.get("GST_COMPANY").details_json if checks.get("GST_COMPANY") else {},
        )
    )
    contributors.append(
        Contributor(
            name="duplicate",
            raw_score=0.0,
            details={"message": "Duplicate detection pending implementation."},
        )
    )
    return contributors


def compute_composite(contributors: Iterable[Contributor]) -> tuple[float, list[dict[str, Any]], str]:
    """Combine contributors into a composite score and return waterfall details with policy version."""
    weights, policy_version = resolve_weights(current_app)
    max_items = current_app.config.get("RISK_WATERFALL_MAX_CONTRIBS", 8)

    waterfall: list[dict[str, Any]] = []
    total = 0.0
    for contrib in contributors:
        weight = max(weights.get(contrib.name, 0.0), 0.0)
        raw = max(0.0, min(contrib.raw_score, 1.0))
        contribution = weight * raw
        total += contribution
        waterfall.append(
            {
                "name": contrib.name,
                "weight": weight,
                "raw_score": raw,
                "contribution": contribution,
                "details_json": contrib.details,
            }
        )
    waterfall.sort(key=lambda item: abs(item["contribution"]), reverse=True)
    if len(waterfall) > max_items:
        waterfall = waterfall[:max_items]
    composite = min(1.0, max(0.0, total))
    return composite, waterfall, policy_version


def persist_risk(
    invoice_id: int,
    composite: float,
    waterfall: list[dict[str, Any]],
    *,
    version: str = "v1",
    policy_version: str = "seed",
) -> RiskScore:
    """Persist the composite score and its contributors."""
    score = RiskScore.query.filter_by(invoice_id=invoice_id).first()
    if score is None:
        score = RiskScore(
            invoice_id=invoice_id,
            composite=composite,
            version=version,
            policy_version=policy_version,
        )
        db.session.add(score)
        db.session.flush()
    else:
        score.composite = composite
        score.version = version
        score.policy_version = policy_version
    # Remove existing contributors before inserting fresh ones.
    RiskContributor.query.filter_by(risk_score_id=score.id).delete()
    db.session.flush()

    for entry in waterfall:
        contributor = RiskContributor(
            risk_score_id=score.id,
            name=entry["name"],
            weight=float(entry.get("weight", 0.0)),
            raw_score=float(entry.get("raw_score", 0.0)),
            contribution=float(entry.get("contribution", 0.0)),
            details_json=entry.get("details_json"),
        )
        db.session.add(contributor)
    db.session.flush()
    return score


__all__ = ["collect_contributors", "compute_composite", "persist_risk", "Contributor"]
