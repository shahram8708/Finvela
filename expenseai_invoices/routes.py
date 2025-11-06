"""Routes handling invoice upload, browsing, and actions."""
from __future__ import annotations

import io
from datetime import datetime
from decimal import Decimal
from statistics import median
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

import filetype
from flask import (
    Response,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from flask_login import current_user, login_required
from sqlalchemy import asc, desc, or_

from expenseai_ai import embeddings, parser_service
from expenseai_ai import market_price as market_price_service
from expenseai_benchmark import service as benchmark_service
from expenseai_compliance import gst_provider
from expenseai_compliance.models import CheckStatus, CheckType
from expenseai_ext import cache
from expenseai_ext.db import db
from expenseai_ext.idempotency import idempotent
from expenseai_ext.security import limiter, user_or_ip_rate_limit
from expenseai_invoices import invoices_bp
from expenseai_invoices.forms import InvoiceActionForm, InvoiceUploadForm
from expenseai_models import AuditLog
from expenseai_models.invoice import INVOICE_STATUSES, Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_models.compliance_check import ComplianceCheck
from expenseai_models.user import User
from expenseai_models.vendor_drift import VendorDrift
from expenseai_models.price_benchmark import PriceBenchmark
from expenseai_vendor import drift as vendor_drift, fingerprints

try:
    from PIL import Image
except ImportError:  # pragma: no cover - Pillow is required via requirements.txt
    Image = None  # type: ignore[assignment]

ALLOWED_IMAGE_MIME_PREFIX = "image/"


def _storage_root() -> Path:
    return Path(current_app.instance_path) / current_app.config["UPLOAD_STORAGE_DIR"]


def _thumbnail_root() -> Path:
    return Path(current_app.instance_path) / current_app.config["THUMBNAIL_DIR"]


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _validate_extension(filename: str) -> None:
    ext = Path(filename).suffix.lower().lstrip(".")
    if ext not in current_app.config["UPLOAD_ALLOWED_EXTENSIONS"]:
        raise ValueError("Unsupported file extension")


def _detect_mime(data: bytes, fallback: str | None = None) -> str:
    kind = filetype.guess(data)
    if kind is not None:
        return kind.mime
    return fallback or "application/octet-stream"


def _enforce_mime(mime: str) -> None:
    if mime not in current_app.config["UPLOAD_ALLOWED_MIME_TYPES"]:
        raise ValueError("Unsupported MIME type")


def _generate_storage_paths(original_name: str) -> tuple[str, Path]:
    now = datetime.utcnow()
    ext = Path(original_name).suffix.lower()
    rel_dir = Path(str(now.year), f"{now.month:02d}")
    stored_name = f"{uuid4().hex}{ext}"
    relative_path = rel_dir / stored_name
    full_path = _storage_root() / relative_path
    _ensure_directory(full_path.parent)
    return str(relative_path).replace("\\", "/"), full_path


def _save_file(data: bytes, destination: Path) -> None:
    destination.write_bytes(data)


def _create_thumbnail_if_needed(invoice: Invoice, data: bytes) -> str | None:
    if not invoice.mime_type.startswith(ALLOWED_IMAGE_MIME_PREFIX):
        return None
    if Image is None:  # pragma: no cover - should not happen with Pillow installed
        current_app.logger.warning("Pillow not available; skipping thumbnail generation")
        return None
    image = Image.open(io.BytesIO(data))  # type: ignore[name-defined]
    image.thumbnail((512, 512))
    now = datetime.utcnow()
    rel_dir = Path(str(now.year), f"{now.month:02d}")
    thumb_name = f"{invoice.id}_{uuid4().hex}.jpg"
    thumb_rel_path = rel_dir / thumb_name
    thumb_full_path = _thumbnail_root() / thumb_rel_path
    _ensure_directory(thumb_full_path.parent)
    image.convert("RGB").save(thumb_full_path, format="JPEG", quality=85)
    return str(thumb_rel_path).replace("\\", "/")


def _audit(action: str, entity_id: int | None, data: dict | None = None) -> None:
    AuditLog.log(action=action, entity="invoice", entity_id=entity_id, data=data or {})


def _current_org_id() -> int | None:
    return getattr(current_user, "organization_id", None)


def _invoice_query_for_user():
    org_id = _current_org_id()
    if org_id is None:
        abort(403)
    return Invoice.query.filter(Invoice.organization_id == org_id)


def _get_invoice_for_user(invoice_id: int) -> Invoice:
    invoice = _invoice_query_for_user().filter(Invoice.id == invoice_id).first()
    if invoice is None:
        abort(404)
    return invoice


def _invoice_to_dict(invoice: Invoice) -> dict:
    return {
        "id": invoice.id,
        "vendor_gst": invoice.vendor_gst,
        "invoice_no": invoice.invoice_no,
        "created_at": invoice.created_at.isoformat() + "Z",
        "processing_status": invoice.processing_status,
        "public_url": invoice.public_url(),
        "thumbnail_url": invoice.thumbnail_url(),
        "mime_type": invoice.mime_type,
        "filesize_bytes": invoice.filesize_bytes,
        "assigned_at": invoice.assigned_at.isoformat() + "Z" if invoice.assigned_at else None,
        "assigned_at_display": invoice.assigned_at.strftime("%b %d, %Y %H:%M") if invoice.assigned_at else None,
        "assignee": (
            {
                "id": invoice.assignee.id,
                "name": invoice.assignee.full_name,
                "email": invoice.assignee.email,
            }
            if invoice.assignee
            else None
        ),
    }


def _vendor_profile_to_dict(profile) -> dict[str, Any]:
    vector = profile.vector_values() or []
    return {
        "vendor_gst": profile.vendor_gst,
        "n_samples": profile.n_samples,
        "avg_unit_price": float(profile.avg_unit_price) if profile.avg_unit_price is not None else None,
        "price_mad": float(profile.price_mad) if profile.price_mad is not None else None,
        "last_updated": profile.last_updated.isoformat() + "Z" if profile.last_updated else None,
        "vector_length": len(vector),
    }


def _refresh_vendor_profile(invoice_id: int, vendor_gst: str) -> dict[str, Any] | None:
    """Refresh vendor profile and drift metrics, returning serialized payloads."""
    cleaned = (vendor_gst or "").strip().upper()
    if not cleaned:
        return None

    invoice = db.session.get(Invoice, invoice_id)
    if invoice is None:
        return None
    org_id = invoice.organization_id
    if org_id is None:
        return None

    try:
        benchmark_service.ingest_invoice_line_items(invoice.id)
        profile = fingerprints.refresh_vendor_profile(cleaned, organization_id=org_id)

        if (profile.n_samples or 0) == 0 and invoice.line_items:
            fallback_text_parts: list[str] = []
            prices: list[Decimal] = []
            char_limit = 3500
            total_chars = 0
            for item in invoice.line_items:
                description = (item.description_norm or item.description_raw or "").strip()
                if description:
                    remaining = char_limit - total_chars
                    if remaining <= 0:
                        break
                    snippet = description[:remaining]
                    fallback_text_parts.append(snippet)
                    total_chars += len(snippet)
                if item.unit_price is not None:
                    prices.append(Decimal(item.unit_price))

            if prices:
                profile.n_samples = len(prices)
                profile.avg_unit_price = sum(prices) / Decimal(len(prices))
                med = median(prices)
                deviations = [abs(price - med) for price in prices]
                profile.price_mad = median(deviations) if deviations else Decimal(0)

            if fallback_text_parts:
                summary_text = " ".join(fallback_text_parts)
                profile.text_norm_summary = summary_text
                try:
                    vector = embeddings.embed_text(summary_text, force_remote=True)
                except ValueError:
                    vector = None
                if vector:
                    profile.update_vector(vector)

            profile.last_updated = datetime.utcnow()
            db.session.flush()

        drift_record = vendor_drift.evaluate_drift(
            cleaned,
            invoice_date=invoice.invoice_date if invoice.invoice_date else None,
            invoice_id=invoice.id,
            organization_id=org_id,
        )

        payload = _vendor_profile_to_dict(profile)
        drift_rows = (
            VendorDrift.query.filter_by(vendor_gst=cleaned, organization_id=org_id)
            .order_by(VendorDrift.created_at.desc())
            .limit(12)
            .all()
        )
        if drift_record is not None:
            known_ids = {row.id for row in drift_rows if row.id is not None}
            if drift_record.id not in known_ids:
                drift_rows.insert(0, drift_record)
        drift_payload = [
            {
                "id": row.id,
                "window_start": row.window_start.isoformat() if row.window_start else None,
                "window_end": row.window_end.isoformat() if row.window_end else None,
                "drift_score": float(row.drift_score),
                "n_samples": row.n_samples,
                "created_at": row.created_at.isoformat() + "Z",
            }
            for row in drift_rows
        ]
        db.session.commit()

        vendor_get_drift = None
        vendor_get_profile = None
        try:
            from expenseai_vendor.routes import (  # noqa: WPS433
                get_drift as vendor_get_drift,
                get_profile as vendor_get_profile,
            )
        except Exception:  # pragma: no cover - avoid circular import issues at runtime
            pass
        if vendor_get_drift:
            cache.delete_memoized(vendor_get_drift)
            cache.delete_memoized(vendor_get_drift, cleaned)
        if vendor_get_profile:
            cache.delete_memoized(vendor_get_profile)
            cache.delete_memoized(vendor_get_profile, cleaned)
        return {"profile": payload, "drift": drift_payload}
    except Exception as exc:  # pragma: no cover - safety net for downstream services
        db.session.rollback()
        current_app.logger.exception(
            "Failed to refresh vendor profile after GST verification",
            extra={"invoice_id": invoice_id, "vendor_gst": cleaned, "error": str(exc)},
        )
        return None


@invoices_bp.route("/upload", methods=["POST"])
@login_required
@limiter.limit("20 per minute")
@limiter.limit("5 per minute", key_func=user_or_ip_rate_limit())
def upload_invoice() -> Response:
    """Handle secure invoice uploads via drag-and-drop or traditional input."""
    form = InvoiceUploadForm()
    if not form.validate_on_submit():
        return jsonify({"status": "error", "errors": form.errors}), 400

    org_id = _current_org_id()
    if org_id is None:
        return jsonify({"status": "error", "message": "Organization membership required."}), 403

    uploaded_file = request.files.get("file")
    if uploaded_file is None:
        return jsonify({"status": "error", "message": "No file supplied."}), 400

    try:
        _validate_extension(uploaded_file.filename or "")
    except ValueError as exc:
        _audit("upload_rejected_extension", None, {"filename": uploaded_file.filename})
        return jsonify({"status": "error", "message": str(exc)}), 400

    uploaded_file.stream.seek(0)
    data = uploaded_file.read()
    if not data:
        return jsonify({"status": "error", "message": "Empty file."}), 400

    max_bytes = current_app.config["MAX_CONTENT_LENGTH"]
    if len(data) > max_bytes:
        _audit("upload_rejected_size", None, {"filename": uploaded_file.filename, "size": len(data)})
        return jsonify({"status": "error", "message": "File exceeds maximum size."}), 413

    mime = _detect_mime(data, uploaded_file.mimetype)
    try:
        _enforce_mime(mime)
    except ValueError as exc:
        _audit("upload_rejected_mime", None, {"filename": uploaded_file.filename, "mime": mime})
        return jsonify({"status": "error", "message": str(exc)}), 400

    stored_rel_path, full_path = _generate_storage_paths(uploaded_file.filename or "invoice")
    _save_file(data, full_path)

    invoice = Invoice(
        original_filename=uploaded_file.filename or stored_rel_path,
        stored_filename=stored_rel_path,
        mime_type=mime,
        filesize_bytes=len(data),
        source_path=stored_rel_path,
        processing_status="UPLOADED",
        vendor_gst=None,
        invoice_no=None,
        organization_id=org_id,
    )
    db.session.add(invoice)
    db.session.flush()  # get invoice.id

    thumbnail_rel = _create_thumbnail_if_needed(invoice, data)
    if thumbnail_rel:
        invoice.thumbnail_path = thumbnail_rel

    InvoiceEvent.record(invoice, "CREATED", {"uploaded_by": current_user.get_id()})
    InvoiceEvent.record(
        invoice,
        "STATUS_CHANGED",
        {"from": None, "to": invoice.processing_status},
    )

    db.session.commit()
    _audit("upload_success", invoice.id, {"mime": mime, "size": len(data)})

    parse_meta: dict[str, Any] | None = None
    if current_app.config.get("AUTO_PARSE_ON_UPLOAD", True):
        try:
            mode, summary = parser_service.request_parse(
                invoice.id,
                actor=current_user.get_id(),
                prefer_async=True,
            )
            refreshed = db.session.get(Invoice, invoice.id)
            if refreshed is not None:
                invoice = refreshed
            parse_meta = {"mode": mode}
            if summary is not None:
                parse_meta["summary"] = summary
        except Exception:  # pragma: no cover - defensive logging only
            current_app.logger.exception("Failed to auto-parse invoice", extra={"invoice_id": invoice.id})

    response = {
        "status": "ok",
        "invoice": _invoice_to_dict(invoice),
        "detail_url": url_for("expenseai_invoices.invoice_detail", invoice_id=invoice.id),
    }
    if parse_meta:
        response["parse"] = parse_meta

    prefers_json = request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json"
    prefers_json = prefers_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if prefers_json:
        return jsonify(response), 201

    success_message = "Invoice uploaded successfully."
    if parse_meta and parse_meta.get("mode") == "inline":
        success_message += " Parsed with Gemini."
    else:
        success_message += " Parsing will continue in the background."
    flash(success_message, "success")
    return redirect(response["detail_url"])


def _apply_filters(query, search: str | None, status: str | None, mime: str | None, date_range: tuple[datetime | None, datetime | None]):
    if search:
        pattern = f"%{search.lower()}%"
        query = query.filter(
            or_(
                Invoice.vendor_gst.ilike(pattern),
                Invoice.invoice_no.ilike(pattern),
                Invoice.original_filename.ilike(pattern),
            )
        )
    if status and status in INVOICE_STATUSES:
        query = query.filter(Invoice.processing_status == status)
    if mime:
        query = query.filter(Invoice.mime_type == mime)
    start, end = date_range
    if start:
        query = query.filter(Invoice.created_at >= start)
    if end:
        query = query.filter(Invoice.created_at <= end)
    return query


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@invoices_bp.route("/")
@login_required
def list_invoices() -> str:
    """Render the invoice browser with filters and pagination."""
    page = request.args.get("page", 1, type=int)
    search = request.args.get("q", "").strip() or None
    status = request.args.get("status") or None
    mime = request.args.get("mime") or None
    start = _parse_date(request.args.get("start"))
    end = _parse_date(request.args.get("end"))
    sort = request.args.get("sort", "created_at")
    direction = request.args.get("dir", "desc")

    query = _invoice_query_for_user()
    query = _apply_filters(query, search, status, mime, (start, end))

    sort_columns = {
        "created_at": Invoice.created_at,
        "processing_status": Invoice.processing_status,
    }
    order = sort_columns.get(sort, Invoice.created_at)
    if direction == "asc":
        query = query.order_by(asc(order))
    else:
        query = query.order_by(desc(order))

    pagination = query.paginate(page=page, per_page=12, error_out=False)

    return render_template(
        "invoices/index.html",
        pagination=pagination,
        invoices=pagination.items,
        statuses=INVOICE_STATUSES,
        filter_values={
            "q": search or "",
            "status": status or "",
            "mime": mime or "",
            "start": request.args.get("start", ""),
            "end": request.args.get("end", ""),
            "sort": sort,
            "dir": direction,
        },
    )


@invoices_bp.route("/search")
@login_required
def search_invoices() -> Response:
    """Return JSON payload used for live search results."""
    search = request.args.get("q", "").strip() or None
    query = _invoice_query_for_user().order_by(Invoice.created_at.desc()).limit(20)
    query = _apply_filters(query, search, None, None, (None, None))
    results = [_invoice_to_dict(invoice) for invoice in query]
    return jsonify({"results": results})


@invoices_bp.route("/<int:invoice_id>")
@login_required
def invoice_detail(invoice_id: int) -> str:
    """Display a tri-panel detail view for an invoice."""
    invoice = _get_invoice_for_user(invoice_id)
    events = invoice.events

    form = InvoiceActionForm()
    form.status.choices = [(status, status.title()) for status in INVOICE_STATUSES]
    org_id = invoice.organization_id or getattr(current_user, "organization_id", None)
    user_query = User.query.order_by(User.full_name.asc())
    if org_id is not None:
        user_query = user_query.filter(User.organization_id == org_id)
    user_query = user_query.filter(User.approved_at.isnot(None))
    users = user_query.all()
    if invoice.assignee and all(user.id != invoice.assignee.id for user in users):
        users.append(invoice.assignee)
        users.sort(key=lambda user: user.full_name.lower())
    assignee_choices = [(0, "Select reviewer…")] + [(u.id, u.full_name) for u in users]
    form.assignee_id.choices = assignee_choices
    form.assignee_id.data = invoice.assignee_id or 0

    header_fields = sorted(invoice.extracted_fields, key=lambda f: f.field_name)
    line_items = sorted(invoice.line_items, key=lambda li: li.line_no)
    risk_payload = invoice.risk_score.as_dict() if invoice.risk_score else None
    summary_event = next((evt for evt in events if evt.event_type == "PARSING_RESULT_SUMMARY" and evt.payload), None)
    parse_summary = summary_event.payload if summary_event else None
    price_benchmark_records = [
        benchmark.as_dict()
        for benchmark in invoice.price_benchmarks
    ]
    price_benchmark_records.sort(key=lambda item: ((item.get("line_no") or 0), item.get("updated_at", "")))

    check_lookup = {check.check_type: check for check in invoice.compliance_checks}
    gst_checks: Dict[str, Dict[str, Any] | None] = {}
    for key, check_type in (("vendor", CheckType.GST_VENDOR.value), ("company", CheckType.GST_COMPANY.value)):
        check = check_lookup.get(check_type)
        if check is None:
            gst_checks[key] = None
            continue
        details = check.details_json or {}
        updated_at = check.updated_at
        gst_checks[key] = {
            "status": check.status,
            "status_label": check.status.replace("_", " ").title(),
            "summary": check.summary,
            "score": check.score,
            "details": details,
            "updated_at": updated_at.isoformat() + "Z" if updated_at else None,
            "updated_at_display": updated_at.strftime("%b %d, %Y %H:%M") if updated_at else None,
        }

    assignment_state = None
    if invoice.assignee:
        assignment_state = {
            "assignee_id": invoice.assignee.id,
            "assignee_name": invoice.assignee.full_name,
            "assignee_email": invoice.assignee.email,
            "assigned_at": invoice.assigned_at.isoformat() + "Z" if invoice.assigned_at else None,
            "assigned_at_display": invoice.assigned_at.strftime("%b %d, %Y %H:%M") if invoice.assigned_at else None,
        }

    return render_template(
        "invoices/detail.html",
        invoice=invoice,
        events=events,
        form=form,
        users=users,
        header_fields=header_fields,
        line_items=line_items,
        risk_payload=risk_payload,
        parse_summary=parse_summary,
        gst_checks=gst_checks,
        assignment_state=assignment_state,
        price_benchmarks=price_benchmark_records,
    )


@invoices_bp.route("/<int:invoice_id>/price-benchmarks", methods=["GET"])
@login_required
def invoice_price_benchmarks(invoice_id: int) -> Response:
    """Return persisted market price benchmarks for the invoice."""
    invoice = _get_invoice_for_user(invoice_id)
    records = [record.as_dict() for record in invoice.price_benchmarks]
    return jsonify({"status": "ok", "benchmarks": records})


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed:
                return float(trimmed)
    except (TypeError, ValueError):
        return None
    return None


@invoices_bp.route("/<int:invoice_id>/price-benchmarks", methods=["POST"])
@login_required
@limiter.limit("5 per minute", key_func=user_or_ip_rate_limit())
def run_invoice_price_benchmarks(invoice_id: int) -> Response:
    """Invoke Finvela grounding to benchmark invoice line item prices."""

    invoice = _get_invoice_for_user(invoice_id)
    payload = request.get_json(silent=True) or {}
    requested_ids = payload.get("line_item_ids")

    line_items = sorted((item for item in invoice.line_items if item.unit_price is not None), key=lambda li: li.line_no)
    if requested_ids:
        try:
            requested_set = {int(value) for value in requested_ids}
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "line_item_ids must be integers."}), 400
        line_items = [item for item in line_items if item.id in requested_set]

    if not line_items:
        return jsonify({"status": "error", "message": "No line items with a unit price are available for benchmarking."}), 400

    max_items = current_app.config.get("MARKET_PRICE_MAX_ITEMS", 5)
    try:
        max_items_int = int(max_items)
    except (TypeError, ValueError):
        max_items_int = 5
    if max_items_int > 0:
        line_items = line_items[:max_items_int]

    currency = (invoice.currency or "INR").upper()
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for item in line_items:
        description = item.description_norm or item.description_raw or ""
        billed_price = item.unit_price
        quantity = item.qty
        try:
            benchmark = market_price_service.benchmark_line_item(
                description=description,
                billed_price=billed_price,
                currency=currency,
                quantity=quantity,
                app=current_app,
            )
        except Exception as exc:  # pragma: no cover - resiliency against API issues
            current_app.logger.exception(
                "Market price benchmark failed",
                extra={"invoice_id": invoice.id, "line_item_id": item.id, "error": str(exc)},
            )
            errors.append({"line_item_id": item.id, "message": str(exc)})
            continue

        record = (
            PriceBenchmark.query.filter_by(invoice_id=invoice.id, line_item_id=item.id).one_or_none()
        )
        if record is None:
            record = PriceBenchmark(invoice_id=invoice.id, line_item_id=item.id)
            db.session.add(record)

        record.product_name = benchmark.get("product_name")
        record.search_query = benchmark.get("search_query")
        record.billed_price = billed_price
        record.billed_currency = currency
        record.market_price = benchmark.get("market_price")
        record.market_currency = (benchmark.get("market_currency") or currency).upper()
        record.price_low = benchmark.get("price_low")
        record.price_high = benchmark.get("price_high")
        record.delta_percent = benchmark.get("delta_percent")
        record.summary = benchmark.get("summary")
        record.confidence = _coerce_float(benchmark.get("confidence"))

        sources_payload = []
        for source in benchmark.get("sources", []):
            if not isinstance(source, dict):
                continue
            price_value = source.get("price")
            if isinstance(price_value, Decimal):
                price_value = float(price_value)
            elif price_value is not None:
                price_value = _coerce_float(price_value)
            sources_payload.append(
                {
                    "title": str(source.get("title", "Source")),
                    "url": str(source.get("url", "")),
                    "price": price_value,
                    "currency": str(source.get("currency") or record.market_currency or currency).upper(),
                }
            )
        record.sources_json = sources_payload
        record.raw_response = benchmark.get("raw_response")

        results.append(record.as_dict())

    if not results and errors:
        return jsonify({"status": "error", "message": "Market price lookup failed for all requested line items.", "errors": errors}), 502

    run_timestamp = datetime.utcnow().isoformat() + "Z"

    if results:
        event_payload = {
            "line_items": [
                {
                    "line_item_id": entry["line_item_id"],
                    "line_no": entry.get("line_no"),
                    "delta_percent": entry.get("delta_percent"),
                    "market_price": entry.get("market_price"),
                    "market_currency": entry.get("market_currency"),
                }
                for entry in results
            ],
            "run_at": run_timestamp,
            "errors": errors,
        }
        InvoiceEvent.record(invoice, "PRICE_BENCHMARK", event_payload)

    db.session.commit()

    refreshed_records = [record.as_dict() for record in invoice.price_benchmarks]

    response_body = {
        "status": "ok",
        "benchmarks": refreshed_records,
        "errors": errors,
        "run_at": run_timestamp,
    }
    return jsonify(response_body)


@invoices_bp.route("/<int:invoice_id>/extracted")
@login_required
def invoice_extracted(invoice_id: int) -> Response:
    """Return JSON representation of extracted header fields and line items."""
    invoice = _get_invoice_for_user(invoice_id)
    header_payload = [
        {
            "field_name": field.field_name,
            "value": field.value,
            "confidence": field.confidence,
        }
        for field in sorted(invoice.extracted_fields, key=lambda f: f.field_name)
    ]
    line_payload = [item.as_dict() for item in sorted(invoice.line_items, key=lambda li: li.line_no)]
    payload = {
        "invoice_id": invoice.id,
        "processing_status": invoice.processing_status,
        "extraction_confidence": invoice.extraction_confidence,
        "extracted_at": invoice.extracted_at.isoformat() + "Z" if invoice.extracted_at else None,
        "pages_parsed": invoice.pages_parsed,
        "header": header_payload,
        "line_items": line_payload,
    }
    return jsonify(payload)


@invoices_bp.route("/<int:invoice_id>/parse", methods=["POST"])
@login_required
@idempotent("parse")
@limiter.limit("10 per minute", key_func=user_or_ip_rate_limit())
def invoice_parse(invoice_id: int) -> Response:
    """Trigger asynchronous AI parsing for the specified invoice."""
    invoice = _get_invoice_for_user(invoice_id)
    status = (invoice.processing_status or "").upper()
    if status in {"QUEUED", "PARSING"}:
        return jsonify({
            "status": "ok",
            "mode": "queued",
            "queued": True,
            "message": "Invoice is already queued for Finvela parsing.",
            "invoice": _invoice_to_dict(invoice),
        })
    if status == "READY":
        return jsonify({
            "status": "ok",
            "mode": "ready",
            "queued": False,
            "message": "Invoice has already been parsed.",
            "invoice": _invoice_to_dict(invoice),
        })

    try:
        mode, summary = parser_service.request_parse(
            invoice.id,
            actor=current_user.get_id(),
            prefer_async=True,
        )
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 404
    except Exception as exc:  # pragma: no cover - defensive logging
        current_app.logger.exception("Failed to initiate Finvela parsing", extra={"invoice_id": invoice.id})
        return jsonify({"status": "error", "message": "Unable to start parsing at this time."}), 400

    refreshed = db.session.get(Invoice, invoice.id)
    if refreshed is not None:
        invoice = refreshed

    payload: dict[str, Any] = {
        "status": "ok",
        "mode": mode,
        "queued": mode == "queued",
        "invoice": _invoice_to_dict(invoice),
    }
    if summary is not None:
        payload["summary"] = summary
    return jsonify(payload)


@invoices_bp.route("/<int:invoice_id>/gst/<string:subject>/verify", methods=["POST"])
@login_required
@limiter.limit("10 per minute", key_func=user_or_ip_rate_limit())
def verify_gst(invoice_id: int, subject: str) -> Response:
    """Validate a GSTIN and persist the compliance result."""
    invoice = _get_invoice_for_user(invoice_id)
    normalized = subject.strip().lower()
    if normalized not in {"vendor", "company"}:
        abort(400, description="Unknown GST verification target.")

    gst_value = invoice.vendor_gst if normalized == "vendor" else invoice.company_gst
    if not gst_value:
        return jsonify({"status": "error", "message": "GST number missing on invoice."}), 400

    gstin = gst_provider.normalize_gstin(gst_value)
    if normalized == "vendor" and gstin != (invoice.vendor_gst or ""):
        invoice.vendor_gst = gstin
    elif normalized == "company" and gstin != (invoice.company_gst or ""):
        invoice.company_gst = gstin

    provider = gst_provider.get_provider()
    result = provider.validate_gstin(gstin)
    status_enum = gst_provider.classify_provider_status(result)
    provider_name = provider.__class__.__name__
    verified_at = datetime.utcnow()
    status_label = status_enum.value.replace("_", " ").title()

    if status_enum == CheckStatus.PASS:
        score = 1.0
    elif status_enum == CheckStatus.WARN:
        score = 0.5
    else:
        score = 0.0

    check_type = CheckType.GST_VENDOR if normalized == "vendor" else CheckType.GST_COMPANY
    check = (
        ComplianceCheck.query.filter_by(invoice_id=invoice.id, check_type=check_type.value)
        .one_or_none()
    )
    if check is None:
        check = ComplianceCheck(invoice_id=invoice.id, check_type=check_type.value)
        db.session.add(check)

    summary_text = f"{normalized.title()} GSTIN {gstin} {status_label}"
    check.status = status_enum.value
    check.score = score
    check.summary = summary_text
    check.details_json = {
        "gstin": gstin,
        "subject": normalized,
        "provider": provider_name,
        "result": result,
        "mode": "manual",
        "verified_at": verified_at.isoformat() + "Z",
        "verified_by": current_user.get_id(),
    }
    check.updated_at = verified_at

    db.session.flush()

    event_payload = {
        "subject": normalized,
        "gstin": gstin,
        "status": status_enum.value,
        "status_label": status_label,
        "provider": provider_name,
        "summary": summary_text,
        "result": result,
        "mode": "manual",
        "verified_by": current_user.get_id(),
        "verified_at": verified_at.isoformat() + "Z",
    }
    InvoiceEvent.record(invoice, "GST_VALIDATION", event_payload)

    AuditLog.log(
        action="gst_manual_verification",
        entity="invoice",
        entity_id=invoice.id,
        data={
            "subject": normalized,
            "status": status_enum.value,
            "provider": provider_name,
            "gstin": gstin,
        },
    )

    invoice_id_value = invoice.id
    response_payload = {
        "check_type": check.check_type,
        "status": check.status,
        "status_label": status_label,
        "summary": check.summary,
        "score": check.score,
        "details": dict(check.details_json or {}),
        "updated_at": verified_at.isoformat() + "Z",
        "updated_at_display": verified_at.strftime("%b %d, %Y %H:%M"),
    }

    db.session.commit()

    vendor_profile_payload: dict[str, Any] | None = None
    vendor_drift_payload: list[dict[str, Any]] | None = None
    if normalized == "vendor":
        refresh_payload = _refresh_vendor_profile(invoice_id_value, gstin)
        if refresh_payload:
            vendor_profile_payload = refresh_payload.get("profile")
            drift_data = refresh_payload.get("drift")
            if isinstance(drift_data, list):
                vendor_drift_payload = drift_data

    response_body: dict[str, Any] = {"status": "ok", "check": response_payload}
    if vendor_profile_payload:
        response_body["vendor_profile"] = vendor_profile_payload
    if vendor_drift_payload:
        response_body["vendor_drift"] = vendor_drift_payload

    return jsonify(response_body)


@invoices_bp.route("/<int:invoice_id>/assignees")
@login_required
def invoice_assignees(invoice_id: int) -> Response:
    """Return JSON list of users available for assignment."""
    invoice = _get_invoice_for_user(invoice_id)
    org_id = invoice.organization_id or getattr(current_user, "organization_id", None)
    query = User.query.order_by(User.full_name.asc())
    if org_id is not None:
        query = query.filter(User.organization_id == org_id)
    query = query.filter(User.approved_at.isnot(None))
    users = query.all()
    payload = [{"id": user.id, "name": user.full_name, "email": user.email} for user in users]
    return jsonify({"assignees": payload})


@invoices_bp.route("/<int:invoice_id>/action", methods=["POST"])
@login_required
@limiter.limit("30 per minute", key_func=user_or_ip_rate_limit())
def invoice_action(invoice_id: int) -> Response:
    """Handle actions (assignments, status updates, notes) taken on an invoice."""
    invoice = _get_invoice_for_user(invoice_id)
    form = InvoiceActionForm()
    form.status.choices = [(status, status.title()) for status in INVOICE_STATUSES]
    org_id = invoice.organization_id or getattr(current_user, "organization_id", None)
    user_query = User.query.order_by(User.full_name.asc())
    if org_id is not None:
        user_query = user_query.filter(User.organization_id == org_id)
    user_query = user_query.filter(User.approved_at.isnot(None))
    users = user_query.all()
    if invoice.assignee and all(user.id != invoice.assignee.id for user in users):
        users.append(invoice.assignee)
        users.sort(key=lambda user: user.full_name.lower())
    form.assignee_id.choices = [(0, "Select reviewer…")] + [(u.id, u.full_name) for u in users]

    if not form.validate_on_submit():
        return jsonify({"status": "error", "errors": form.errors}), 400

    action = form.action.data
    note_value = form.note.data.strip() if form.note.data else None
    payload = {"action": action, "user_id": current_user.get_id()}
    if note_value:
        payload["note"] = note_value
        payload["notes"] = note_value

    assignment_payload: dict[str, Any] | None = None

    if action == "assign":
        assignee_id = form.assignee_id.data or 0
        if assignee_id <= 0:
            return jsonify({"status": "error", "message": "Select a reviewer to assign."}), 400
        assignee = db.session.get(User, assignee_id)
        if not assignee or (org_id is not None and assignee.organization_id != org_id):
            return jsonify({"status": "error", "message": "Assignee not found"}), 404
        invoice.assignee_id = assignee.id
        invoice.assigned_at = datetime.utcnow()
        payload.update({
            "assignee_id": assignee.id,
            "assignee_name": assignee.full_name,
            "assignee_email": assignee.email,
            "assigned_at": invoice.assigned_at.isoformat() + "Z" if invoice.assigned_at else None,
        })
        assignment_payload = {
            "assignee_id": assignee.id,
            "assignee_name": assignee.full_name,
            "assignee_email": assignee.email,
            "assigned_at": invoice.assigned_at.isoformat() + "Z" if invoice.assigned_at else None,
            "assigned_at_display": invoice.assigned_at.strftime("%b %d, %Y %H:%M") if invoice.assigned_at else None,
        }
        InvoiceEvent.record(invoice, "ACTION", payload)
    elif action == "status":
        status_value = form.status.data or invoice.processing_status
        invoice.set_status(status_value, notes=note_value)
        payload["status"] = status_value
        InvoiceEvent.record(invoice, "ACTION", payload)
    elif action == "request_docs":
        InvoiceEvent.record(invoice, "ACTION", payload | {"request": "docs"})
        if form.status.data:
            invoice.set_status(form.status.data, notes=note_value)
    elif action == "approve":
        invoice.set_status("READY", notes=note_value)
        InvoiceEvent.record(invoice, "ACTION", payload | {"result": "approved"})
    elif action == "reject":
        invoice.set_status("ERROR", notes=note_value)
        InvoiceEvent.record(invoice, "ACTION", payload | {"result": "rejected"})
    else:
        return jsonify({"status": "error", "message": "Unknown action"}), 400

    db.session.commit()
    _audit(f"invoice_action_{action}", invoice.id, payload)

    latest_event = invoice.events[0]
    if assignment_payload is None and invoice.assignee:
        assignment_payload = {
            "assignee_id": invoice.assignee.id,
            "assignee_name": invoice.assignee.full_name,
            "assignee_email": invoice.assignee.email,
            "assigned_at": invoice.assigned_at.isoformat() + "Z" if invoice.assigned_at else None,
            "assigned_at_display": invoice.assigned_at.strftime("%b %d, %Y %H:%M") if invoice.assigned_at else None,
        }
    return jsonify({
        "status": "ok",
        "invoice": _invoice_to_dict(invoice),
        "event": latest_event.as_dict(),
        "assignment": assignment_payload,
    })


def _safe_path(root: Path, relative: str) -> tuple[Path, str]:
    request_path = Path(relative)
    if request_path.is_absolute() or ".." in request_path.parts:
        abort(404)
    full_path = root / request_path
    if not full_path.exists():
        abort(404)
    return full_path.parent, full_path.name


@invoices_bp.route("/file/<path:stored>")
@login_required
def get_invoice_file(stored: str):
    """Serve the stored invoice file with strict path validation."""
    invoice = _invoice_query_for_user().filter(Invoice.stored_filename == stored).first()
    if invoice is None:
        abort(404)
    directory, filename = _safe_path(_storage_root(), stored)
    response = send_from_directory(directory, filename, mimetype=invoice.mime_type, as_attachment=False)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Content-Disposition"] = f"inline; filename={invoice.original_filename}"
    return response


@invoices_bp.route("/thumb/<path:stored>")
@login_required
def get_invoice_thumbnail(stored: str):
    """Serve the thumbnail for preview cards."""
    invoice = _invoice_query_for_user().filter(Invoice.thumbnail_path == stored).first()
    if not invoice:
        abort(404)
    directory, filename = _safe_path(_thumbnail_root(), stored)
    response = send_from_directory(directory, filename, mimetype="image/jpeg", as_attachment=False)
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response
