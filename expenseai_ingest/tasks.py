"""Celery tasks backing ingestion, storage, and downstream parsing."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from celery.utils.log import get_task_logger
from flask import current_app
from sqlalchemy.exc import IntegrityError

from expenseai.celery_app import celery
from expenseai_ai import parser_service
from expenseai_ext.db import db
from expenseai_ingest import utils
from expenseai_ingest.config import IngestSettings
from expenseai_ingest.storage import StorageError, StorageResult, get_storage
from expenseai_models.invoice import Invoice
from expenseai_models.invoice_event import InvoiceEvent

logger = get_task_logger(__name__)


def _settings() -> IngestSettings:
    return IngestSettings.from_app(current_app)


def _normalize_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    result = dict(metadata or {})
    if "source" not in result:
        result["source"] = "ingest"
    return result


def _safe_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
    return safe


def _validate_payload(*, filename: str, data: bytes, mime_type: str | None, settings: IngestSettings) -> str:
    utils.validate_extension(filename, settings.allowed_extensions)
    if len(data) > settings.max_bytes:
        raise ValueError("File exceeds ingestion size limit")
    fallback_mime = mime_type or utils.guess_mime_from_name(filename)
    detected_mime = utils.detect_mime(data, fallback_mime)
    utils.enforce_mime(detected_mime, settings.allowed_mime_types)
    return detected_mime


def _store(data: bytes, filename: str, mime_type: str) -> StorageResult:
    storage = get_storage()
    return storage.store_bytes(data=data, original_name=filename, mime_type=mime_type)


def _create_invoice(storage_result: StorageResult, metadata: dict[str, Any]) -> Invoice:
    with db.session.begin():
        invoice = Invoice(
            original_filename=storage_result.original_filename,
            stored_filename=storage_result.stored_filename,
            mime_type=storage_result.mime_type,
            filesize_bytes=storage_result.filesize_bytes,
            source_path=storage_result.source_path,
            processing_status="UPLOADED",
        )
        db.session.add(invoice)
        db.session.flush()
        payload = {
            "source": metadata.get("source", "ingest"),
            "backend": storage_result.backend,
            "uri": storage_result.uri,
            "checksum": storage_result.checksum_sha256,
            "filesize_bytes": storage_result.filesize_bytes,
        }
        ingest_payload = payload | {
            "stored": storage_result.stored_filename,
            "metadata": _safe_metadata(metadata),
        }
        InvoiceEvent.record(invoice, "CREATED", ingest_payload)
        InvoiceEvent.record(invoice, "INGESTED", ingest_payload)
        InvoiceEvent.record(invoice, "STATUS_CHANGED", {"from": None, "to": invoice.processing_status})
    return invoice


@celery.task(name="expenseai_ingest.save_original", autoretry_for=(StorageError, OSError), retry_backoff=True, retry_kwargs={"max_retries": 5})
def save_original(*, path: str | None = None, data_b64: str | None = None, filename: str | None = None, mime_type: str | None = None) -> dict[str, Any]:
    settings = _settings()
    if path:
        file_path = Path(path)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(path)
        data = file_path.read_bytes()
        name = filename or file_path.name
    elif data_b64:
        data = utils.decode_bytes(data_b64)
        name = filename or "ingest-upload"
    else:
        raise ValueError("Either path or data_b64 must be provided")
    detected_mime = _validate_payload(filename=name, data=data, mime_type=mime_type, settings=settings)
    storage_result = _store(data, name, detected_mime)
    return storage_result.to_dict()


def _ingest_from_source(*, path: str | None, data_b64: str | None, filename: str | None, mime_type: str | None, metadata: Mapping[str, Any] | None) -> tuple[StorageResult, dict[str, Any]]:
    settings = _settings()
    meta = _normalize_metadata(metadata)
    if path:
        file_path = Path(path)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(path)
        data = file_path.read_bytes()
        name = filename or file_path.name
    elif data_b64:
        data = utils.decode_bytes(data_b64)
        name = filename or "ingest-upload"
    else:
        raise ValueError("No source payload supplied")
    detected_mime = _validate_payload(filename=name, data=data, mime_type=mime_type, settings=settings)
    storage_result = _store(data, name, detected_mime)
    meta = meta | {
        "mime_type": detected_mime,
        "filesize_bytes": storage_result.filesize_bytes,
        "checksum": storage_result.checksum_sha256,
    }
    return storage_result, meta


@celery.task(name="expenseai_ingest.create_invoice_from_path", bind=True, autoretry_for=(StorageError, OSError), retry_backoff=True, retry_kwargs={"max_retries": 5})
def create_invoice_from_path(self, path: str, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
    storage_result, meta = _ingest_from_source(path=path, data_b64=None, filename=None, mime_type=None, metadata=metadata)
    new_invoice = True
    try:
        invoice = _create_invoice(storage_result, meta)
    except IntegrityError as exc:
        db.session.rollback()
        logger.warning("Invoice storage duplicate detected", exc_info=exc)
        invoice = Invoice.query.filter_by(stored_filename=storage_result.stored_filename).first()
        if not invoice:
            raise
        new_invoice = False
    payload = {"invoice_id": invoice.id, "stored_filename": storage_result.stored_filename}
    if new_invoice:
        _enqueue_parse_if_needed(invoice.id, meta)
    return payload


@celery.task(name="expenseai_ingest.create_invoice_from_bytes", bind=True, autoretry_for=(StorageError,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def create_invoice_from_bytes(self, *, data_b64: str, filename: str, metadata: Mapping[str, Any] | None = None, mime_type: str | None = None) -> dict[str, Any]:
    storage_result, meta = _ingest_from_source(path=None, data_b64=data_b64, filename=filename, mime_type=mime_type, metadata=metadata)
    new_invoice = True
    try:
        invoice = _create_invoice(storage_result, meta)
    except IntegrityError as exc:
        db.session.rollback()
        logger.warning("Invoice storage duplicate detected", exc_info=exc)
        invoice = Invoice.query.filter_by(stored_filename=storage_result.stored_filename).first()
        if not invoice:
            raise
        new_invoice = False
    payload = {"invoice_id": invoice.id, "stored_filename": storage_result.stored_filename}
    if new_invoice:
        _enqueue_parse_if_needed(invoice.id, meta)
    return payload


def _enqueue_parse_if_needed(invoice_id: int, metadata: Mapping[str, Any]) -> None:
    if metadata.get("skip_parse"):
        return
    if not current_app.config.get("AUTO_PARSE_ON_UPLOAD", True):
        return
    actor = metadata.get("source", "ingest")
    enqueue_parse.delay(invoice_id=invoice_id, metadata={"actor": actor})


@celery.task(name="expenseai_ingest.enqueue_parse", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def enqueue_parse(self, invoice_id: int, metadata: Mapping[str, Any] | None = None) -> bool:
    meta = _normalize_metadata(metadata)
    actor = meta.get("actor") or meta.get("source") or "ingest"
    try:
        parser_service.enqueue_invoice_parse(invoice_id, queued_by=actor)
    except ValueError as exc:
        logger.warning("Invoice not found while enqueueing parse", extra={"invoice_id": invoice_id, "error": str(exc)})
        return False
    except Exception as exc:  # pragma: no cover - retries handled by Celery
        raise self.retry(exc=exc)

    with db.session.begin():
        invoice = db.session.get(Invoice, invoice_id)
        if invoice:
            InvoiceEvent.record(
                invoice,
                "QUEUED_FOR_PARSE",
                {
                    "actor": actor,
                    "queued_at": datetime.utcnow().isoformat() + "Z",
                },
            )
    return True


__all__ = [
    "save_original",
    "create_invoice_from_path",
    "create_invoice_from_bytes",
    "enqueue_parse",
]
