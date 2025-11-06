"""Background orchestration for risk scoring pipeline."""
from __future__ import annotations

import threading
from datetime import datetime

from flask import current_app

from expenseai_benchmark import service as benchmark_service
from expenseai_ext.db import db
from expenseai_models import AuditLog
from expenseai_models.invoice import Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_risk.engine import collect_contributors, compute_composite, persist_risk

RISK_VERSION = "v1"


def run_risk_async(invoice_id: int, actor: str = "system") -> None:
    """Spawn a background worker to compute risk scores."""
    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_with_context,
        args=(app, invoice_id, actor),
        name="risk-runner",
        daemon=True,
    )
    thread.start()


def _run_with_context(app, invoice_id: int, actor: str) -> None:
    with app.app_context():
        run_risk_pipeline(invoice_id, actor=actor)


def run_risk_pipeline(invoice_id: int, actor: str = "system") -> None:
    """Execute the risk scoring workflow synchronously."""
    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        current_app.logger.warning("Risk pipeline invoked for missing invoice", extra={"invoice_id": invoice_id})
        return

    try:
        invoice.set_risk_status("IN_PROGRESS", notes=None)
        InvoiceEvent.record(
            invoice,
            "RISK_STARTED",
            {"invoice_id": invoice.id, "actor": actor, "timestamp": datetime.utcnow().isoformat() + "Z"},
        )
        db.session.commit()
        AuditLog.log(action="risk_run_started", entity="invoice", entity_id=invoice.id, data={"actor": actor})

        benchmark_service.ingest_invoice_line_items(invoice.id)
        db.session.commit()

        summary = benchmark_service.benchmark_invoice(invoice.id)
        contributors = collect_contributors(invoice.id, benchmark_summary=summary)
        composite, waterfall, policy_version = compute_composite(contributors)
        score = persist_risk(
            invoice.id,
            composite,
            waterfall,
            version=RISK_VERSION,
            policy_version=policy_version,
        )

        invoice.set_risk_status("READY")
        invoice.risk_notes = f"Composite risk score {composite:.2f}"

        top_contribs = [
            {
                "name": entry["name"],
                "weight": entry["weight"],
                "raw_score": entry["raw_score"],
                "contribution": entry["contribution"],
            }
            for entry in waterfall
        ]
        payload = {
            "invoice_id": invoice.id,
            "composite": composite,
            "avg_outlier_score": summary.get("avg_outlier_score"),
            "contributors": top_contribs,
        }
        InvoiceEvent.record(invoice, "RISK_SUMMARY", payload)
        InvoiceEvent.record(
            invoice,
            "RISK_READY",
            {
                "invoice_id": invoice.id,
                "composite": composite,
                "version": score.version,
                "policy_version": score.policy_version,
            },
        )
        db.session.commit()
        AuditLog.log(
            action="risk_run_completed",
            entity="invoice",
            entity_id=invoice.id,
            data={"composite": composite, "contributors": top_contribs},
        )
    except Exception as exc:  # pragma: no cover - defensive path
        current_app.logger.exception("Risk pipeline failed", extra={"invoice_id": invoice_id})
        db.session.rollback()
        invoice = db.session.get(Invoice, invoice_id)
        if invoice:
            invoice.set_risk_status("ERROR", notes=str(exc), emit_event=False)
            InvoiceEvent.record(
                invoice,
                "RISK_ERROR",
                {"invoice_id": invoice.id, "error": str(exc)},
            )
            db.session.commit()
    else:
        current_app.logger.info(
            "Risk pipeline completed",
            extra={"invoice_id": invoice_id, "status": invoice.risk_status},
        )