"""Blueprint routes for compliance administration."""
from __future__ import annotations

import io
from datetime import datetime

from flask import Response, current_app, flash, redirect, render_template, url_for
from flask_login import login_required

from expenseai_compliance import compliance_admin_bp
from expenseai_compliance.forms import HsnUploadForm
from expenseai_compliance import hsn_service
from expenseai_ext import auth as auth_ext
from expenseai_ext.db import db


@compliance_admin_bp.route("/compliance/hsn", methods=["GET", "POST"])
@login_required
@auth_ext.roles_required("admin")
def hsn_upload() -> str | Response:
    """Render the HSN upload page and handle form submissions."""
    form = HsnUploadForm()
    stats = hsn_service.stats()

    if form.validate_on_submit():
        file_storage = form.file.data
        try:
            file_storage.stream.seek(0)
            payload = file_storage.stream.read()
            if isinstance(payload, str):
                decoded = payload
            else:
                decoded = payload.decode("utf-8-sig")
            inserted, updated = hsn_service.refresh_rates(
                io.StringIO(decoded), replace_existing=form.replace_existing.data
            )
            stats = hsn_service.stats()
            flash(
                f"HSN catalog updated. Inserted {inserted} rows, updated {updated} rows.",
                "success",
            )
            return redirect(url_for("expenseai_compliance_admin.hsn_upload"))
        except UnicodeDecodeError:
            current_app.logger.exception("Failed to decode HSN CSV upload")
            db.session.rollback()
            flash("Could not decode file. Ensure it is UTF-8 encoded.", "danger")
        except ValueError as exc:
            current_app.logger.exception("Validation error while loading HSN rates")
            db.session.rollback()
            flash(str(exc), "danger")

    if form.errors:
        flash(next(iter(form.errors.values()))[0], "warning")

    return render_template("compliance_admin/hsn_upload.html", form=form, stats=stats)


@compliance_admin_bp.route("/compliance/hsn/sample", methods=["GET"])
@login_required
@auth_ext.roles_required("admin")
def sample_hsn_csv() -> Response:
    """Provide a sample HSN CSV for administrators."""
    now = datetime.utcnow().date().isoformat()
    sample = (
        "code,gst_rate,effective_from,effective_to,description\n"
        f"8523,18.0,{now},,Data processing units\n"
    )
    response = current_app.response_class(sample, mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=hsn_sample.csv"
    return response
