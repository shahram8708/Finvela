"""HTTP endpoints for counterfactual what-if analysis."""
from __future__ import annotations

from http import HTTPStatus

from flask import Blueprint, current_app, jsonify, request
from flask_login import current_user, login_required
from pydantic import ValidationError

from expenseai_counterfactual import counterfactual_bp
from expenseai_counterfactual import schemas
from expenseai_counterfactual import service
from expenseai_ext.db import db
from expenseai_ext.security import limiter, user_or_ip_rate_limit


@counterfactual_bp.route("/<int:invoice_id>/counterfactual", methods=["POST"])
@login_required
@limiter.limit("8 per minute", key_func=user_or_ip_rate_limit())
def run_counterfactual(invoice_id: int):
    """Evaluate a proposed set of edits against invoice risk and totals."""
    payload = request.get_json(silent=True) or {}
    payload["invoice_id"] = invoice_id
    try:
        request_model = schemas.CounterfactRequest.model_validate(payload)
    except ValidationError as exc:  # pragma: no cover - validation handled uniformly
        return jsonify({"status": "error", "errors": exc.errors()}), HTTPStatus.BAD_REQUEST

    try:
        result = service.evaluate(invoice_id, request_model, actor=current_user.get_id() or "user")
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        current_app.logger.warning("Counterfactual evaluation rejected", extra={"invoice_id": invoice_id, "error": str(exc)})
        return jsonify({"status": "error", "message": str(exc)}), HTTPStatus.BAD_REQUEST
    except Exception as exc:  # pragma: no cover - defensive path
        db.session.rollback()
        current_app.logger.exception("Counterfactual evaluation failed", extra={"invoice_id": invoice_id})
        return jsonify({"status": "error", "message": "Counterfactual evaluation failed"}), HTTPStatus.INTERNAL_SERVER_ERROR

    return jsonify({"status": "ok", "result": result.model_dump()})
