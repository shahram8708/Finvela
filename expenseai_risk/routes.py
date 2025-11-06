"""HTTP routes exposing risk scoring operations."""
from __future__ import annotations

from flask import Blueprint, current_app, jsonify
from flask_login import current_user, login_required

from expenseai_ext.idempotency import idempotent
from expenseai_ext.security import limiter, user_or_ip_rate_limit
from expenseai_models.invoice import Invoice
from expenseai_models.risk_score import RiskScore
from expenseai_risk import orchestrator
from expenseai_risk.weights import resolve_weights

risk_bp = Blueprint("expenseai_risk", __name__)


@risk_bp.route("/invoices/<int:invoice_id>/risk/run", methods=["POST"])
@login_required
@idempotent("risk")
@limiter.limit("10 per minute", key_func=user_or_ip_rate_limit())
def run_risk(invoice_id: int):
    """Trigger asynchronous risk computation for an invoice."""
    invoice = Invoice.query.get_or_404(invoice_id)
    actor = current_user.get_id() or "user"
    orchestrator.run_risk_async(invoice.id, actor=str(actor))
    return jsonify({"queued": True, "invoice_id": invoice.id})


@risk_bp.route("/invoices/<int:invoice_id>/risk", methods=["GET"])
@login_required
def get_risk(invoice_id: int):
    """Return the latest computed risk score and contributors."""
    invoice = Invoice.query.get_or_404(invoice_id)
    score = invoice.risk_score
    weights, policy_version = resolve_weights(current_app)
    weights = {key: float(value) for key, value in weights.items()}
    if score is None:
        return jsonify(
            {
                "invoice_id": invoice.id,
                "computed": False,
                "risk_status": invoice.risk_status,
                "risk_notes": invoice.risk_notes,
                "weights": weights,
                "policy_version": policy_version,
            }
        )

    payload = {
        "invoice_id": invoice.id,
        "computed": True,
        "risk_status": invoice.risk_status,
        "risk_notes": invoice.risk_notes,
        "composite": float(score.composite),
        "version": score.version,
        "policy_version": score.policy_version,
        "weights": weights,
        "contributors": [
            {
                "name": contrib.name,
                "weight": float(contrib.weight),
                "raw_score": float(contrib.raw_score),
                "contribution": float(contrib.contribution),
                "details": contrib.details_json or {},
            }
            for contrib in score.contributors
        ],
    }
    return jsonify(payload)
