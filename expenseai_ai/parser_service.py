"""Background orchestration for Gemini-powered invoice parsing."""
from __future__ import annotations

import atexit
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from flask import Flask, current_app
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from expenseai_ai import gemini_client
from expenseai_ai.norm import norm_currency, norm_gst, parse_iso_date, to_decimal
from expenseai_ai.schemas import ParseResult
from expenseai_ext.db import db
from expenseai_models import ExtractedField, Invoice, InvoiceEvent, LineItem

_WORKER_LOCK = threading.Lock()
_WORKER_THREAD: threading.Thread | None = None
_STOP_EVENT = threading.Event()
_RATE_LIMIT_UNTIL: float | None = None


def enqueue_invoice_parse(invoice_id: int, *, queued_by: Optional[str] = None) -> None:
    """Place an invoice into the parsing queue and emit audit events."""
    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            raise ValueError(f"Invoice {invoice_id} not found")
        if invoice.processing_status in {"QUEUED", "PARSING"}:
            return
        invoice.processing_notes = None
        invoice.set_status("QUEUED")
        InvoiceEvent.record(
            invoice,
            "PARSING_ENQUEUED",
            {"queued_by": queued_by or "system", "timestamp": datetime.utcnow().isoformat() + "Z"},
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise


def start_background_worker(app: Flask) -> None:
    """Launch the daemon thread responsible for processing the queue."""
    if app.config.get("APP_DISABLE_BG_PARSER", False):
        app.logger.info("Background parser disabled via configuration")
        return

    global _WORKER_THREAD
    with _WORKER_LOCK:
        if _WORKER_THREAD and _WORKER_THREAD.is_alive():
            return
        _STOP_EVENT.clear()
        _WORKER_THREAD = threading.Thread(
            target=_worker_loop,
            args=(app,),
            name="expenseai-parser",
            daemon=True,
        )
        _WORKER_THREAD.start()
        app.logger.info("Started background invoice parser thread")

    atexit.register(stop_background_worker)


def stop_background_worker() -> None:
    """Signal the worker to halt gracefully during shutdown."""
    if not _WORKER_THREAD:
        return
    _STOP_EVENT.set()


def is_worker_active() -> bool:
    """Return True if the background parsing worker thread is alive."""
    with _WORKER_LOCK:
        thread = _WORKER_THREAD
    return bool(thread and thread.is_alive())


def _worker_loop(app: Flask) -> None:
    """Continuously poll for queued invoices and process them."""
    poll_interval = 2.0
    with app.app_context():
        while not _STOP_EVENT.is_set():
            global _RATE_LIMIT_UNTIL
            if _RATE_LIMIT_UNTIL is not None:
                remaining = _RATE_LIMIT_UNTIL - time.time()
                if remaining > 0:
                    time.sleep(min(2.0, remaining))
                    continue
                _RATE_LIMIT_UNTIL = None
            invoice_id = _claim_next_invoice()
            if invoice_id is None:
                time.sleep(poll_interval)
                continue
            try:
                _process_invoice(invoice_id)
            except Exception as exc:  # pragma: no cover - runtime safeguard
                app.logger.exception("Invoice parsing failed", extra={"invoice_id": invoice_id, "error": str(exc)})
            finally:
                db.session.remove()  # ensure clean session state between iterations
            time.sleep(0.2)


def _claim_next_invoice() -> Optional[int]:
    """Select the oldest queued invoice, move it to PARSING, and return its ID."""
    invoice = (
        db.session.execute(
            select(Invoice).where(Invoice.processing_status == "QUEUED").order_by(Invoice.created_at.asc()).limit(1)
        )
        .scalars()
        .first()
    )
    if not invoice:
        return None

    invoice.set_status("PARSING")
    InvoiceEvent.record(
        invoice,
        "PARSING_STARTED",
        {"worker": "background", "timestamp": datetime.utcnow().isoformat() + "Z"},
    )
    db.session.commit()
    return invoice.id


def _process_invoice(invoice_id: int) -> None:
    """Execute parsing for the given invoice within a transactional boundary."""
    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            return
        summary = parse_and_persist(invoice)
        invoice.processing_notes = None
        invoice.set_status("READY")
        InvoiceEvent.record(invoice, "PARSING_RESULT_SUMMARY", summary)
        db.session.commit()
    except gemini_client.GeminiRateLimitError as exc:
        db.session.rollback()
        _handle_gemini_rate_limit(invoice_id, exc)
    except Exception as exc:  # pragma: no cover - handled via logging
        db.session.rollback()
        current_app.logger.exception("Error while persisting parsed invoice", extra={"invoice_id": invoice_id, "error": str(exc)})
        _mark_invoice_error(invoice_id, exc)


def _mark_invoice_error(invoice_id: int, exc: Exception) -> None:
    """Mark an invoice as errored and capture relevant telemetry."""
    truncated = str(exc)[:400]
    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            return
        invoice.processing_notes = truncated
        invoice.set_status("ERROR")
        InvoiceEvent.record(
            invoice,
            "PARSING_ERROR",
            {"message": truncated, "timestamp": datetime.utcnow().isoformat() + "Z"},
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise


def _handle_gemini_rate_limit(invoice_id: int, exc: gemini_client.GeminiRateLimitError) -> None:
    """Requeue the invoice when Finvela reports quota exhaustion."""
    app = current_app
    delay = exc.retry_after if exc.retry_after is not None else app.config.get("GEMINI_RATE_LIMIT_DELAY_SECS", 30.0)
    delay = max(0.0, float(delay))
    _schedule_rate_limit_backoff(delay)

    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            return
        note = f"Finvela quota exhausted; retrying in {int(delay)}s"
        invoice.processing_notes = note
        invoice.set_status("QUEUED")
        InvoiceEvent.record(
            invoice,
            "PARSING_RATE_LIMITED",
            {
                "message": note,
                "retry_after": delay,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        )
        db.session.commit()
        app.logger.warning(
            "Finvela rate limit hit; invoice re-queued",
            extra={"invoice_id": invoice_id, "retry_after": delay},
        )
    except SQLAlchemyError:
        db.session.rollback()
        raise


def _schedule_rate_limit_backoff(delay: float) -> None:
    """Block the worker loop from immediately retrying after rate limits."""
    global _RATE_LIMIT_UNTIL
    _RATE_LIMIT_UNTIL = time.time() + delay if delay > 0 else None


def request_parse(invoice_id: int, *, actor: Optional[str] = None, prefer_async: bool = True) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Queue or run parsing based on worker availability.

    Returns a tuple ``(mode, summary)`` where ``mode`` is ``"queued"`` when the
    background worker accepted the task or ``"inline"`` when parsing completed
    synchronously. ``summary`` is only populated for inline execution.
    """

    actor = actor or "system"
    background_allowed = prefer_async and not current_app.config.get("APP_DISABLE_BG_PARSER", False)

    if background_allowed and is_worker_active():
        enqueue_invoice_parse(invoice_id, queued_by=actor)
        return "queued", None

    summary = parse_invoice_sync(invoice_id, actor=actor)
    return "inline", summary


def parse_invoice_sync(invoice_id: int, *, actor: str = "cli") -> Dict[str, Any]:
    """Run the parsing pipeline synchronously (used by CLI operations)."""
    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            raise ValueError(f"Invoice {invoice_id} not found")
        invoice.processing_notes = None
        invoice.set_status("PARSING")
        InvoiceEvent.record(
            invoice,
            "PARSING_STARTED",
            {"worker": actor, "timestamp": datetime.utcnow().isoformat() + "Z"},
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise

    try:
        invoice = db.session.get(Invoice, invoice_id)
        if invoice is None:
            raise ValueError(f"Invoice {invoice_id} disappeared during parsing")
        summary = parse_and_persist(invoice)
        invoice.processing_notes = None
        invoice.set_status("READY")
        InvoiceEvent.record(invoice, "PARSING_RESULT_SUMMARY", summary)
        db.session.commit()
        return summary
    except Exception as exc:  # pragma: no cover - runtime safety net
        db.session.rollback()
        _mark_invoice_error(invoice_id, exc)
        raise


def parse_and_persist(invoice: Invoice) -> Dict[str, Any]:
    """Invoke Gemini, validate the payload, and store structured results."""
    app = current_app
    storage_root = Path(app.instance_path) / app.config["UPLOAD_STORAGE_DIR"]
    relative_path = invoice.source_path or invoice.stored_filename
    file_path = storage_root / Path(relative_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Invoice source file missing at {file_path}")

    file_ref = gemini_client.upload_file(str(file_path), invoice.mime_type, app=app)
    raw_payload = gemini_client.parse_invoice(
        file_ref,
        model_name=app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
        max_pages=app.config.get("PARSER_MAX_PAGES", 6),
        app=app,
    )
    result = ParseResult.from_gemini_payload(raw_payload)

    # Clear previously extracted values to keep records in sync.
    ExtractedField.query.filter_by(invoice_id=invoice.id).delete()
    LineItem.query.filter_by(invoice_id=invoice.id).delete()

    confidences = result.header.per_field_confidence.model_dump()
    header_values = {
        "invoice_no": result.header.invoice_no,
        "invoice_date": result.header.invoice_date.isoformat() if result.header.invoice_date else None,
        "vendor_gst": norm_gst(result.header.vendor_gst),
        "company_gst": norm_gst(result.header.company_gst),
        "currency": norm_currency(result.header.currency),
        "subtotal": str(result.header.subtotal) if result.header.subtotal is not None else None,
        "tax_total": str(result.header.tax_total) if result.header.tax_total is not None else None,
        "grand_total": str(result.header.grand_total) if result.header.grand_total is not None else None,
    }

    for field_name, value in header_values.items():
        db.session.add(
            ExtractedField(
                invoice=invoice,
                field_name=field_name,
                value=value,
                confidence=float(confidences[field_name]),
            )
        )

    # Update invoice metadata directly from normalized header.
    invoice.invoice_no = header_values["invoice_no"]
    invoice.invoice_date = parse_iso_date(result.header.invoice_date)  # type: ignore[arg-type]
    invoice.vendor_gst = header_values["vendor_gst"]
    invoice.company_gst = header_values["company_gst"]
    invoice.currency = header_values["currency"]
    invoice.subtotal = to_decimal(result.header.subtotal)
    invoice.tax_total = to_decimal(result.header.tax_total)
    invoice.grand_total = to_decimal(result.header.grand_total)
    invoice.extracted_at = datetime.utcnow()
    invoice.pages_parsed = min(result.pages_parsed, app.config.get("PARSER_MAX_PAGES", result.pages_parsed))
    invoice.extraction_confidence = result.critical_confidence_mean([
        "invoice_no",
        "invoice_date",
        "vendor_gst",
        "company_gst",
        "grand_total",
    ])

    line_items_created = 0
    for item in result.line_items:
        line_item = LineItem(
            invoice=invoice,
            line_no=item.line_no,
            description_raw=item.description_raw,
            description_norm=None,
            hsn_sac=item.hsn_sac,
            qty=to_decimal(item.qty),
            unit_price=to_decimal(item.unit_price),
            gst_rate=to_decimal(item.gst_rate),
            line_subtotal=to_decimal(item.line_subtotal),
            line_tax=to_decimal(item.line_tax),
            line_total=to_decimal(item.line_total),
            confidence=float(item.confidence),
        )
        db.session.add(line_item)
        line_items_created += 1

    summary = {
        "header_confidence_mean": invoice.extraction_confidence,
        "line_items": line_items_created,
        "pages_parsed": invoice.pages_parsed,
        "fields": {
            name: {
                "value": header_values[name],
                "confidence": float(confidences[name]),
            }
            for name in header_values
        },
    }
    if result.analysis:
        summary["analysis"] = result.analysis.model_dump(mode="json")
    return summary
