"""Business logic for counterfactual what-if evaluations."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, List

from flask import current_app

from expenseai_benchmark import service as benchmark_service
from expenseai_compliance import arithmetic, hsn_service
from expenseai_counterfactual.schemas import (
    CounterfactContributor,
    CounterfactLineChange,
    CounterfactRequest,
    CounterfactResponse,
    CounterfactRiskSnapshot,
    CounterfactTotals,
)
from expenseai_ext.db import db
from expenseai_models import AuditLog
from expenseai_models.invoice import Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_models.line_item import LineItem
from expenseai_risk.engine import Contributor, collect_contributors, compute_composite


@dataclass(slots=True)
class _LineSnapshot:
    """Mutable representation of an invoice line used for in-memory what-if."""

    line_no: int
    description_raw: str
    description_norm: str | None
    hsn_sac: str | None
    qty: Decimal | None
    unit_price: Decimal | None
    gst_rate: Decimal | None


def evaluate(invoice_id: int, payload: CounterfactRequest, *, actor: str) -> CounterfactResponse:
    """Perform counterfactual evaluation for the supplied invoice and request."""
    invoice: Invoice | None = db.session.get(Invoice, invoice_id)
    if invoice is None:
        raise ValueError(f"Invoice {invoice_id} not found")

    max_lines = int(current_app.config.get("COUNTERFACT_MAX_LINES", 200))
    if len(payload.line_changes) > max_lines:
        raise ValueError(f"Counterfactual limited to {max_lines} line adjustments")

    max_delta_pct = Decimal(str(current_app.config.get("COUNTERFACT_MAX_DELTA_PCT", 0.5)))
    epsilon = Decimal(str(current_app.config.get("ARITH_EPSILON", 0.01)))

    line_map: dict[int, LineItem] = {line.line_no: line for line in invoice.line_items}
    if not line_map:
        raise ValueError("Invoice has no line items to adjust")

    # Freeze current totals for delta computation.
    before_totals = CounterfactTotals(
        subtotal=Decimal(invoice.subtotal or 0),
        tax_total=Decimal(invoice.tax_total or 0),
        grand_total=Decimal(invoice.grand_total or 0),
    )

    changed_map = {change.line_no: change for change in payload.line_changes}

    working_lines: List[_LineSnapshot] = []
    notes: List[str] = []

    for line_no, model in sorted(line_map.items()):
        snapshot = _LineSnapshot(
            line_no=line_no,
            description_raw=model.description_raw,
            description_norm=model.description_norm,
            hsn_sac=model.hsn_sac,
            qty=Decimal(model.qty or 0),
            unit_price=Decimal(model.unit_price or 0),
            gst_rate=Decimal(model.gst_rate or 0),
        )
        change = changed_map.get(line_no)
        if change:
            snapshot = _apply_change(snapshot, change, max_delta_pct, notes)
        working_lines.append(snapshot)

    recomputed_lines = _recompute_line_totals(working_lines)
    subtotal_after, tax_after, grand_after = _aggregate_totals(recomputed_lines)

    totals_after = CounterfactTotals(
        subtotal=subtotal_after,
        tax_total=tax_after,
        grand_total=grand_after,
    )
    totals_delta = CounterfactTotals(
        subtotal=subtotal_after - before_totals.subtotal,
        tax_total=tax_after - before_totals.tax_total,
        grand_total=grand_after - before_totals.grand_total,
    )

    contributors_before = collect_contributors(invoice.id)
    composite_before, waterfall_before, policy_version = compute_composite(contributors_before)

    contributors_after, hsn_mismatches = _build_counterfactual_contributors(
        invoice,
        recomputed_lines,
        contributors_before,
        epsilon,
    )
    composite_after, waterfall_after, policy_version_after = compute_composite(contributors_after)

    delta_composite = composite_after - composite_before

    if totals_delta.grand_total.copy_abs() > epsilon:
        notes.append(
            f"Grand total delta {totals_delta.grand_total:+.2f} indicates arithmetic change."
        )
    if delta_composite < 0:
        notes.append(f"Risk decreased by {abs(delta_composite) * 100:.1f}% under proposed changes.")
    elif delta_composite > 0:
        notes.append(f"Risk increased by {delta_composite * 100:.1f}% under proposed changes.")
    if hsn_mismatches:
        notes.append(f"{hsn_mismatches} line(s) still violate expected HSN rate tables.")

    InvoiceEvent.record(
        invoice,
        "COUNTERFACT_EVALUATED",
        {
            "invoice_id": invoice.id,
            "delta_grand_total": float(totals_delta.grand_total),
            "delta_composite": float(delta_composite),
            "notes": notes,
        },
    )
    AuditLog.log(
        action="counterfact_evaluated",
        entity="invoice",
        entity_id=invoice.id,
        data={
            "actor": actor,
            "changes": [change.model_dump() for change in payload.line_changes],
            "delta_totals": {
                "subtotal": float(totals_delta.subtotal),
                "tax_total": float(totals_delta.tax_total),
                "grand_total": float(totals_delta.grand_total),
            },
            "delta_composite": float(delta_composite),
        },
    )

    return CounterfactResponse(
        invoice_id=invoice.id,
        totals_before=before_totals,
        totals_after=totals_after,
        totals_delta=totals_delta,
        risk_before=_to_risk_snapshot(composite_before, waterfall_before, policy_version),
        risk_after=_to_risk_snapshot(composite_after, waterfall_after, policy_version_after),
        delta_composite=float(delta_composite),
        notes=notes,
    )


def _apply_change(
    snapshot: _LineSnapshot,
    change: CounterfactLineChange,
    max_delta_pct: Decimal,
    notes: List[str],
) -> _LineSnapshot:
    source_line = snapshot.line_no
    if change.qty is not None:
        _validate_delta(snapshot.qty, change.qty, max_delta_pct, f"qty line {source_line}")
        snapshot.qty = Decimal(change.qty)
    if change.unit_price is not None:
        _validate_delta(snapshot.unit_price, change.unit_price, max_delta_pct, f"unit price line {source_line}")
        snapshot.unit_price = Decimal(change.unit_price)
    if change.gst_rate is not None:
        snapshot.gst_rate = Decimal(change.gst_rate)
    if change.hsn_sac is not None:
        snapshot.hsn_sac = change.hsn_sac.strip() or None
        notes.append(f"Line {source_line} HSN updated to {snapshot.hsn_sac or 'blank'}")
    return snapshot


def _validate_delta(existing: Decimal | None, new_value: Decimal, limit_pct: Decimal, label: str) -> None:
    if existing is None:
        return
    if existing == 0:
        # Allow adjustments from zero but guard against runaway values using limit as absolute multiplier.
        if new_value.copy_abs() > limit_pct * Decimal(1000):  # arbitrary safety for zero baselines
            raise ValueError(f"Change for {label} exceeds guardrail for zero baseline")
        return
    delta = (Decimal(new_value) - existing).copy_abs()
    pct = delta / existing.copy_abs()
    if pct > limit_pct:
        raise ValueError(f"Change for {label} exceeds {limit_pct * 100:.0f}% limit")


def _recompute_line_totals(lines: Iterable[_LineSnapshot]) -> list[dict[str, Decimal | int | str | None]]:
    recomputed: list[dict[str, Decimal | int | str | None]] = []
    for entry in lines:
        subtotal, tax, total = arithmetic.recompute_line_totals(entry.qty, entry.unit_price, entry.gst_rate)
        recomputed.append(
            {
                "line_no": entry.line_no,
                "description_raw": entry.description_raw,
                "description_norm": entry.description_norm,
                "hsn_sac": entry.hsn_sac,
                "qty": entry.qty,
                "unit_price": entry.unit_price,
                "gst_rate": entry.gst_rate,
                "line_subtotal": subtotal,
                "line_tax": tax,
                "line_total": total,
            }
        )
    return recomputed


def _aggregate_totals(lines: Iterable[dict[str, Decimal | None]]) -> tuple[Decimal, Decimal, Decimal]:
    subtotal = Decimal(0)
    tax = Decimal(0)
    grand = Decimal(0)
    for line in lines:
        subtotal += Decimal(line.get("line_subtotal") or 0)
        tax += Decimal(line.get("line_tax") or 0)
        grand += Decimal(line.get("line_total") or 0)
    return subtotal, tax, grand


def _build_counterfactual_contributors(
    invoice: Invoice,
    lines: list[dict[str, Decimal | None]],
    baseline: List[Contributor],
    epsilon: Decimal,
) -> tuple[List[Contributor], int]:
    by_name = {contrib.name: contrib for contrib in baseline}

    currency = invoice.currency
    lookback = int(current_app.config.get("BENCH_LOOKBACK_DAYS", 365))
    outlier_scores: list[dict[str, object]] = []
    scores: list[float] = []

    for line in lines:
        qty = Decimal(line.get("qty") or 0)
        unit_price = Decimal(line.get("unit_price") or 0)
        text_norm = str(line.get("description_norm") or line.get("description_raw") or "")
        baseline_result = benchmark_service.build_baseline(
            text_norm,
            currency,
            lookback,
            organization_id=invoice.organization_id,
        )
        if unit_price:
            median_value = baseline_result.median or unit_price
            mad_value = baseline_result.mad or Decimal(str(current_app.config.get("OUTLIER_EPSILON", 0.01)))
            denominator = max(mad_value.copy_abs(), Decimal(str(current_app.config.get("OUTLIER_EPSILON", 0.01))))
            if denominator == 0:
                denominator = Decimal("1")
            robust_z = float((Decimal("0.6745") * (unit_price - median_value) / denominator))
            score = benchmark_service.outlier_score(unit_price, median_value, mad_value, epsilon=float(current_app.config.get("OUTLIER_EPSILON", 0.01)))
            scores.append(score)
            outlier_scores.append(
                {
                    "line_no": line["line_no"],
                    "unit_price": float(unit_price),
                    "median": float(median_value) if median_value is not None else None,
                    "mad": float(mad_value),
                    "robust_z": robust_z,
                    "outlier_score": score,
                    "qty": float(qty),
                }
            )

    avg_outlier = sum(scores) / len(scores) if scores else 0.0
    outlier_scores.sort(key=lambda item: item.get("robust_z", 0), reverse=True)

    market_contrib = Contributor(
        name="market_outlier",
        raw_score=min(1.0, max(0.0, avg_outlier)),
        details={
            "top_outliers": outlier_scores[:3],
            "currency": currency,
        },
    )

    totals_expected = _aggregate_totals(lines)
    stored_subtotal = Decimal(invoice.subtotal or 0)
    stored_tax = Decimal(invoice.tax_total or 0)
    stored_grand = Decimal(invoice.grand_total or 0)
    arithmetic_flag = 1.0 if (
        (totals_expected[0] - stored_subtotal).copy_abs() > epsilon
        or (totals_expected[1] - stored_tax).copy_abs() > epsilon
        or (totals_expected[2] - stored_grand).copy_abs() > epsilon
    ) else 0.0
    arithmetic_contrib = Contributor(
        "arithmetic",
        arithmetic_flag,
        by_name.get("arithmetic", Contributor("arithmetic", 0.0, {})).details,
    )

    lines_list = list(lines)
    hsn_flag, mismatches = _hsn_stats(invoice, lines_list)
    hsn_contrib = Contributor(
        "hsn_rate",
        hsn_flag,
        by_name.get("hsn_rate", Contributor("hsn_rate", 0.0, {})).details,
    )

    contributors_after: List[Contributor] = []
    for contrib in baseline:
        if contrib.name == "market_outlier":
            contributors_after.append(market_contrib)
        elif contrib.name == "arithmetic":
            contributors_after.append(arithmetic_contrib)
        elif contrib.name == "hsn_rate":
            contributors_after.append(hsn_contrib)
        else:
            contributors_after.append(Contributor(contrib.name, contrib.raw_score, contrib.details))

    return contributors_after, mismatches


def _hsn_stats(invoice: Invoice, lines: Iterable[dict[str, Decimal | None]]) -> tuple[float, int]:
    lines_list = list(lines)
    invoice_date = invoice.invoice_date
    mismatches = 0
    for line in lines_list:
        code = line.get("hsn_sac")
        rate = Decimal(line.get("gst_rate") or 0)
        expected = hsn_service.get_rate(str(code) if code else None, invoice_date)
        if expected is None:
            continue
        expected_rate = Decimal(expected.tax_rate or 0)
        if expected_rate != rate:
            mismatches += 1
    if mismatches == 0:
        return 0.0, 0
    total_lines = len(lines_list) or 1
    return min(1.0, mismatches / total_lines), mismatches


def _to_risk_snapshot(composite: float, waterfall: list[dict[str, object]], policy_version: str) -> CounterfactRiskSnapshot:
    contributors = [
        CounterfactContributor(
            name=str(item.get("name")),
            weight=float(item.get("weight", 0.0)),
            raw_score=float(item.get("raw_score", 0.0)),
            contribution=float(item.get("contribution", 0.0)),
            details=dict(item.get("details_json") or {}),
        )
        for item in waterfall
    ]
    return CounterfactRiskSnapshot(
        composite=float(composite),
        policy_version=policy_version,
        contributors=contributors,
    )
