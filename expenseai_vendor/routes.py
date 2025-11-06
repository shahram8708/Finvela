"""Routes exposing vendor fingerprint data, drift metrics, and directory views."""
from __future__ import annotations

from flask import abort, current_app, jsonify, render_template, request
from flask_login import current_user, login_required
from sqlalchemy import or_

from expenseai_ext import cache
from expenseai_ext.db import db
from expenseai_vendor import vendor_bp
from expenseai_vendor import fingerprints, drift
from expenseai_models.vendor_drift import VendorDrift
from expenseai_models.vendor_profile import VendorProfile


def _vendor_profile_cache_key() -> str:
    org_id = getattr(current_user, "organization_id", None)
    return f"vendor_profile:{org_id}:{request.full_path}"


def _vendor_drift_cache_key() -> str:
    org_id = getattr(current_user, "organization_id", None)
    return f"vendor_drift:{org_id}:{request.full_path}"


def _require_org_id() -> int:
    org_id = getattr(current_user, "organization_id", None)
    if org_id is None:
        abort(403)
    return org_id


@vendor_bp.route("/", methods=["GET"])
@login_required
def list_vendors():
    """Render a directory of vendors with fingerprint summaries."""
    org_id = _require_org_id()
    search = (request.args.get("q") or "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = min(max(request.args.get("per_page", 20, type=int), 5), 100)

    query = VendorProfile.query.filter(VendorProfile.organization_id == org_id)
    if search:
        pattern = f"%{search.lower()}%"
        query = query.filter(
            or_(
                VendorProfile.vendor_gst.ilike(pattern),
                VendorProfile.text_norm_summary.ilike(pattern),
            )
        )

    query = query.order_by(VendorProfile.last_updated.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    vendors = pagination.items

    drift_map: dict[str, VendorDrift] = {}
    if vendors:
        vendor_ids = [vendor.vendor_gst for vendor in vendors]
        drift_rows = (
            VendorDrift.query.filter(
                VendorDrift.vendor_gst.in_(vendor_ids),
                VendorDrift.organization_id == org_id,
            )
            .order_by(VendorDrift.vendor_gst.asc(), VendorDrift.created_at.desc())
            .all()
        )
        for record in drift_rows:
            drift_map.setdefault(record.vendor_gst, record)

    return render_template(
        "vendors/index.html",
        vendors=vendors,
        pagination=pagination,
        latest_drift=drift_map,
        search_term=search,
    )


@vendor_bp.route("/<vendor_gst>", methods=["GET"])
@login_required
def vendor_detail(vendor_gst: str):
    """Detailed view combining profile and recent drift observations."""
    org_id = _require_org_id()
    cleaned = (vendor_gst or "").strip().upper()
    profile = VendorProfile.query.filter_by(vendor_gst=cleaned, organization_id=org_id).first()
    if profile is None or (profile.n_samples or 0) == 0:
        try:
            profile = fingerprints.refresh_vendor_profile(cleaned, organization_id=org_id)
            drift.evaluate_drift(cleaned, organization_id=org_id)
            db.session.commit()
            cache.delete_memoized(get_drift)
            cache.delete_memoized(get_drift, cleaned)
            cache.delete_memoized(get_profile)
            cache.delete_memoized(get_profile, cleaned)
        except ValueError:
            abort(404)
        except Exception as exc:  # pragma: no cover - defensive logging for UI refresh
            db.session.rollback()
            current_app.logger.exception(
                "Unable to refresh vendor profile for detail view",
                extra={"vendor_gst": cleaned, "error": str(exc)},
            )
            if profile is None:
                abort(404)
    if profile is None:
        abort(404)
    drift_rows = (
        VendorDrift.query.filter_by(vendor_gst=cleaned, organization_id=org_id)
        .order_by(VendorDrift.created_at.desc())
        .limit(30)
        .all()
    )

    latest_drift = drift_rows[0] if drift_rows else None
    drift_stats = {
        "count": len(drift_rows),
        "max": max((row.drift_score for row in drift_rows), default=None),
        "min": min((row.drift_score for row in drift_rows), default=None),
        "avg": (
            sum(row.drift_score for row in drift_rows) / len(drift_rows)
            if drift_rows
            else None
        ),
    }

    return render_template(
        "vendors/detail.html",
        profile=profile,
        drift_rows=drift_rows,
        latest_drift=latest_drift,
        drift_stats=drift_stats,
    )


@vendor_bp.route("/<vendor_gst>/profile", methods=["GET"])
@login_required
@cache.cached(timeout=300, key_prefix=_vendor_profile_cache_key)
def get_profile(vendor_gst: str):
    """Return the vendor fingerprint profile, refreshing if requested."""
    org_id = _require_org_id()
    cleaned = (vendor_gst or "").strip().upper()
    refresh = request.args.get("refresh") == "true"

    profile = VendorProfile.query.filter_by(vendor_gst=cleaned, organization_id=org_id).first()
    if refresh or profile is None:
        try:
            profile = fingerprints.refresh_vendor_profile(cleaned, organization_id=org_id)
            drift.evaluate_drift(cleaned, organization_id=org_id)
            db.session.commit()
            cache.delete_memoized(get_drift)
            cache.delete_memoized(get_drift, cleaned)
        except ValueError:
            return jsonify({"status": "error", "message": "Vendor GST required."}), 400
    if profile is None:
        return jsonify({"status": "error", "message": "Vendor not found."}), 404

    vector = profile.vector_values() or []
    payload = {
        "vendor_gst": cleaned,
        "n_samples": profile.n_samples,
        "avg_unit_price": float(profile.avg_unit_price) if profile.avg_unit_price is not None else None,
        "price_mad": float(profile.price_mad) if profile.price_mad is not None else None,
        "last_updated": profile.last_updated.isoformat() + "Z" if profile.last_updated else None,
        "vector_length": len(vector),
    }
    return jsonify({"status": "ok", "profile": payload})


@vendor_bp.route("/<vendor_gst>/drift", methods=["GET"])
@login_required
@cache.cached(timeout=120, key_prefix=_vendor_drift_cache_key)
def get_drift(vendor_gst: str):
    """Return recent drift observations for the vendor."""
    org_id = _require_org_id()
    cleaned = (vendor_gst or "").strip().upper()
    limit = min(int(request.args.get("limit", 12)), 50)

    records = (
        VendorDrift.query.filter_by(vendor_gst=cleaned, organization_id=org_id)
        .order_by(VendorDrift.created_at.desc())
        .limit(limit)
        .all()
    )
    if not records:
        drift_record = drift.evaluate_drift(cleaned, organization_id=org_id)
        if drift_record:
            db.session.commit()
            records = [drift_record]
    payload = [
        {
            "id": record.id,
            "window_start": record.window_start.isoformat() if record.window_start else None,
            "window_end": record.window_end.isoformat() if record.window_end else None,
            "drift_score": float(record.drift_score),
            "n_samples": record.n_samples,
            "created_at": record.created_at.isoformat() + "Z",
        }
        for record in records
    ]
    return jsonify({"status": "ok", "drift": payload})
