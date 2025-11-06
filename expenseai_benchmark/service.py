"""Services building price benchmarks and outlier scores."""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import List

from flask import current_app
from sqlalchemy import and_

from expenseai_ai.embeddings import get_or_create_item_embedding, normalize_for_embedding
from expenseai_benchmark.models import BaselineResult
from expenseai_ext.db import db
from expenseai_models.external_benchmark import ExternalBenchmark
from expenseai_models.invoice import Invoice
from expenseai_models.item_price_history import ItemPriceHistory


def ingest_invoice_line_items(invoice_id: int) -> None:
    """Persist invoice line pricing for future benchmarking."""
    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        current_app.logger.warning("Benchmark ingest missing invoice", extra={"invoice_id": invoice_id})
        return

    org_id = invoice.organization_id
    if org_id is None:
        current_app.logger.warning(
            "Skipping benchmark ingest for invoice without organization",
            extra={"invoice_id": invoice.id},
        )
        return

    invoice_currency = invoice.currency
    invoice_date = invoice.invoice_date or datetime.utcnow().date()
    for item in invoice.line_items:
        normalized = normalize_for_embedding(item.description_norm or item.description_raw or "")
        if not normalized:
            continue
        try:
            get_or_create_item_embedding(normalized)
        except Exception as exc:  # pragma: no cover - network dependent
            current_app.logger.exception(
                "Embedding lookup failed",
                extra={"invoice_id": invoice.id, "line_no": item.line_no, "error": str(exc)},
            )
            continue
        if item.unit_price is None:
            continue
        existing = (
            ItemPriceHistory.query.filter(
                and_(
                    ItemPriceHistory.invoice_id == invoice.id,
                    ItemPriceHistory.organization_id == org_id,
                    ItemPriceHistory.text_norm == normalized,
                    ItemPriceHistory.unit_price == item.unit_price,
                    ItemPriceHistory.qty == item.qty,
                )
            )
            .limit(1)
            .first()
        )
        if existing:
            continue
        record = ItemPriceHistory(
            text_norm=normalized,
            vendor_gst=invoice.vendor_gst,
            currency=invoice_currency,
            unit_price=item.unit_price,
            qty=item.qty,
            invoice_date=invoice_date,
            invoice_id=invoice.id,
            organization_id=org_id,
        )
        db.session.add(record)
    db.session.flush()


def build_baseline(
    text_norm: str,
    currency: str | None,
    lookback_days: int,
    *,
    as_of: date | None = None,
    organization_id: int | None = None,
) -> BaselineResult:
    """Compute median and MAD for the requested item within the lookback window."""
    if not text_norm:
        return BaselineResult(median=None, mad=None, sample_count=0)

    as_of = as_of or datetime.utcnow().date()
    window_start = as_of - timedelta(days=lookback_days)
    query = ItemPriceHistory.query.filter(ItemPriceHistory.text_norm == text_norm)
    if organization_id is not None:
        query = query.filter(ItemPriceHistory.organization_id == organization_id)
    if currency:
        query = query.filter(ItemPriceHistory.currency == currency)
    query = query.filter(
        (ItemPriceHistory.invoice_date.is_(None))
        | and_(ItemPriceHistory.invoice_date >= window_start, ItemPriceHistory.invoice_date <= as_of)
    )

    prices: List[Decimal] = []
    for entry in query.all():
        if entry.unit_price is not None:
            prices.append(Decimal(entry.unit_price))
    if not prices:
        baseline = BaselineResult(median=None, mad=None, sample_count=0)
    else:
        prices.sort()
        med = Decimal(median(prices))
        abs_devs = [abs(p - med) for p in prices]
        mad = Decimal(median(abs_devs)) if abs_devs else Decimal(0)
        baseline = BaselineResult(median=med, mad=mad, sample_count=len(prices))

    external = (
        ExternalBenchmark.query.filter(ExternalBenchmark.text_norm == text_norm)
        .filter((ExternalBenchmark.currency == currency) | (ExternalBenchmark.currency.is_(None)))
        .order_by(ExternalBenchmark.effective_from.desc().nullslast())
        .limit(5)
        .all()
    )
    active_external = next((record for record in external if record.is_active(as_of)), None)
    if active_external and active_external.median_price is not None:
        baseline.used_external = True
        baseline.external_source = active_external.source
        ext_n = active_external.n or 0
        if baseline.median is None:
            baseline.median = Decimal(active_external.median_price)
            baseline.sample_count = ext_n
        else:
            total_n = max(baseline.sample_count, 0) + ext_n
            if total_n > 0 and ext_n:
                baseline.median = (
                    baseline.median * baseline.sample_count + Decimal(active_external.median_price) * ext_n
                ) / Decimal(total_n)
                baseline.sample_count = total_n
        if active_external.mad is not None:
            if baseline.mad is None or baseline.mad == 0:
                baseline.mad = Decimal(active_external.mad)
            else:
                baseline.mad = (baseline.mad + Decimal(active_external.mad)) / Decimal(2)
    return baseline


def outlier_score(price: Decimal, median_value: Decimal, mad_value: Decimal, *, epsilon: float) -> float:
    """Return logistic-scaled robust z-score as risk contribution."""
    if mad_value is None:
        mad_value = Decimal(0)
    if median_value is None:
        return 0.0
    denominator = max(abs(mad_value), Decimal(str(epsilon)))
    rz = float((Decimal("0.6745") * (price - median_value) / denominator))
    exponent = -(abs(rz) - 2.0)
    logistic = 1 / (1 + math.exp(exponent))
    return max(0.0, min(logistic, 1.0))


def benchmark_invoice(invoice_id: int) -> dict[str, object]:
    """Compute per-line benchmarking metrics and aggregate score for the invoice."""
    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        raise ValueError(f"Invoice {invoice_id} not found")

    epsilon = float(current_app.config.get("OUTLIER_EPSILON", 0.01))
    lookback = int(current_app.config.get("BENCH_LOOKBACK_DAYS", 365))
    currency = invoice.currency
    as_of = invoice.invoice_date or datetime.utcnow().date()

    per_line: list[dict[str, object]] = []
    scores: list[float] = []

    for item in invoice.line_items:
        normalized = normalize_for_embedding(item.description_norm or item.description_raw or "")
        baseline = build_baseline(
            normalized,
            currency,
            lookback,
            as_of=as_of,
            organization_id=invoice.organization_id,
        )
        unit_price = Decimal(item.unit_price) if item.unit_price is not None else None
        mad = baseline.mad or Decimal(0)
        median_value = baseline.median or (unit_price if unit_price is not None else None)
        rz = 0.0
        score = 0.0
        if unit_price is not None and median_value is not None:
            denominator = max(abs(mad), Decimal(str(epsilon)))
            rz = float((Decimal("0.6745") * (unit_price - median_value) / denominator))
            score = outlier_score(unit_price, median_value, mad, epsilon=epsilon)
            scores.append(score)
        per_line.append(
            {
                "line_no": item.line_no,
                "description": item.description_raw,
                "text_norm": normalized,
                "unit_price": float(unit_price) if unit_price is not None else None,
                "qty": float(item.qty) if item.qty is not None else None,
                "currency": currency,
                "median": float(baseline.median) if baseline.median is not None else None,
                "mad": float(baseline.mad) if baseline.mad is not None else None,
                "sample_count": baseline.sample_count,
                "used_external": baseline.used_external,
                "external_source": baseline.external_source,
                "robust_z": rz,
                "outlier_score": score,
            }
        )

    avg_score = sum(scores) / len(scores) if scores else 0.0
    return {
        "invoice_id": invoice.id,
        "avg_outlier_score": avg_score,
        "lines": per_line,
        "currency": currency,
        "computed_at": datetime.utcnow().isoformat() + "Z",
    }