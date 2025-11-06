"""Admin routes for managing external benchmark CSV uploads."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from flask import Blueprint, Response, current_app, flash, redirect, render_template, url_for
from flask_login import login_required

from expenseai_ai.embeddings import normalize_for_embedding
from expenseai_benchmark.forms import BenchmarkUploadForm
from expenseai_ext import auth as auth_ext
from expenseai_ext.db import db
from expenseai_ext.security import limiter, user_or_ip_rate_limit
from expenseai_models import AuditLog
from expenseai_models.external_benchmark import ExternalBenchmark

benchmark_admin_bp = Blueprint(
    "expenseai_benchmark_admin",
    __name__,
    template_folder="templates",
    url_prefix="/admin/benchmarks",
)


@benchmark_admin_bp.route("/", methods=["GET", "POST"])
@login_required
@auth_ext.roles_required("admin")
@limiter.limit("2 per minute", key_func=user_or_ip_rate_limit())
def upload_benchmarks():
    """Upload external benchmark CSVs with validation."""
    form = BenchmarkUploadForm()
    stats = _stats()
    if form.validate_on_submit():
        storage = form.file.data
        try:
            storage.stream.seek(0)
            payload = storage.stream.read()
            if isinstance(payload, str):
                text = payload
            else:
                text = payload.decode("utf-8-sig")
            inserted, updated = _ingest_csv(io.StringIO(text))
            db.session.commit()
            AuditLog.log(
                action="benchmarks_upload",
                entity="external_benchmark",
                entity_id=None,
                data={"inserted": inserted, "updated": updated},
            )
            flash(f"Benchmark catalog updated. Inserted {inserted}, updated {updated}.", "success")
            return redirect(url_for("expenseai_benchmark_admin.upload_benchmarks"))
        except UnicodeDecodeError:
            db.session.rollback()
            flash("Could not decode file. Ensure it is UTF-8 encoded.", "danger")
        except ValueError as exc:
            db.session.rollback()
            flash(str(exc), "danger")
        except Exception as exc:  # pragma: no cover - defensive logging only
            db.session.rollback()
            current_app.logger.exception("Benchmark upload failed")
            flash(f"Unexpected error: {exc}", "danger")
    elif form.errors:
        flash(next(iter(form.errors.values()))[0], "warning")

    stats = _stats()  # refresh after possible changes
    return render_template("benchmark_admin/upload.html", form=form, stats=stats)


@benchmark_admin_bp.route("/download-sample", methods=["GET"])
@login_required
@auth_ext.roles_required("admin")
def download_sample() -> Response:
    """Provide a sample CSV illustrating expected columns."""
    today = datetime.utcnow().date().isoformat()
    sample = (
        "text_norm,currency,median_price,mad,n,source,effective_from,effective_to\n"
        f"laptop sleeve,inr,999.0,120.0,7,internal,{today},\n"
    )
    response = current_app.response_class(sample, mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=external_benchmark_sample.csv"
    return response


def _ingest_csv(handle: io.StringIO) -> tuple[int, int]:
    reader = csv.DictReader(handle)
    required = {
        "text_norm",
        "currency",
        "median_price",
        "mad",
        "n",
        "source",
        "effective_from",
        "effective_to",
    }
    if not required.issubset(set(reader.fieldnames or [])):
        missing = required - set(reader.fieldnames or [])
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    inserted = 0
    updated = 0
    for row in reader:
        text_norm_raw = (row.get("text_norm") or "").strip()
        normalized = normalize_for_embedding(text_norm_raw)
        if not normalized:
            current_app.logger.info("Skipping benchmark row without usable description", extra={"row": row})
            continue
        currency = (row.get("currency") or "").strip().upper() or None
        try:
            median_price = Decimal(row.get("median_price") or "0") if row.get("median_price") else None
            mad = Decimal(row.get("mad") or "0") if row.get("mad") else None
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"Invalid numeric value for '{row.get('text_norm', '')}': {exc}") from exc
        try:
            sample_size = int(row.get("n") or 0)
        except ValueError as exc:
            raise ValueError(f"Invalid sample size for '{row.get('text_norm', '')}': {exc}") from exc
        source = (row.get("source") or "").strip() or None
        eff_from = _parse_date(row.get("effective_from"))
        eff_to = _parse_date(row.get("effective_to"))

        record = (
            ExternalBenchmark.query.filter_by(
                text_norm=normalized,
                currency=currency,
                source=source,
                effective_from=eff_from,
            )
            .limit(1)
            .first()
        )
        payload = {
            "text_norm": normalized,
            "currency": currency,
            "median_price": median_price,
            "mad": mad,
            "n": sample_size,
            "source": source,
            "effective_from": eff_from,
            "effective_to": eff_to,
        }
        if record:
            for key, value in payload.items():
                setattr(record, key, value)
            record.updated_at = datetime.utcnow()
            updated += 1
        else:
            db.session.add(ExternalBenchmark(**payload))
            inserted += 1
    return inserted, updated


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.strip()).date()
    except ValueError:
        raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from None


def _stats() -> dict[str, object]:
    total = ExternalBenchmark.query.count()
    latest = (
        db.session.query(ExternalBenchmark.updated_at)
        .order_by(ExternalBenchmark.updated_at.desc())
        .limit(1)
        .scalar()
    )
    return {
        "count": total,
        "last_updated": latest.isoformat() + "Z" if latest else None,
    }
