"""Admin routes to monitor and control ingestion services."""
from __future__ import annotations

from flask import Blueprint, Response, current_app, jsonify
from flask_login import login_required

from expenseai_ext import auth as auth_ext

ingest_admin_bp = Blueprint("expenseai_ingest_admin", __name__, url_prefix="/admin/ingest")


def _services() -> dict[str, object]:
    return current_app.extensions.setdefault("expenseai_ingest", {})


@ingest_admin_bp.route("/ping")
@login_required
@auth_ext.roles_required("admin")
def ping() -> Response:
    services = _services()
    watcher = services.get("watcher")
    emailer = services.get("email")
    payload = {
        "watcher": watcher.status() if watcher else {"running": False, "paths": []},
        "email": emailer.status() if emailer else {"enabled": False},
    }
    return jsonify(payload)


@ingest_admin_bp.route("/scan-now", methods=["POST"])
@login_required
@auth_ext.roles_required("admin")
def scan_now() -> Response:
    services = _services()
    watcher = services.get("watcher")
    emailer = services.get("email")
    files_queued = watcher.scan_now() if watcher else 0
    if emailer:
        emailer.trigger_now()
    payload = {
        "status": "ok",
        "queued_files": files_queued,
        "watcher": watcher.status() if watcher else {"running": False},
        "email": emailer.status() if emailer else {"enabled": False},
    }
    return jsonify(payload)


__all__ = ["ingest_admin_bp"]
