"""Audit log entries for security-relevant events."""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Mapping, Optional, Sequence

from flask import g, has_request_context, request
from flask_login import current_user
from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


def _json_safe(value: Any) -> Any:
    """Recursively convert objects to JSON-serializable primitives."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump())
    return str(value)


class AuditLog(db.Model):
    """Store security and compliance related events."""

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(db.ForeignKey("users.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    entity: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    data: Mapped[dict[str, Any] | None] = mapped_column(db.JSON, nullable=True)
    request_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    actor_ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    actor_ua: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    route: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    http_method: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    extra_json: Mapped[dict[str, Any] | None] = mapped_column(db.JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_audit_logs_created_at", "created_at"),
        Index("ix_audit_logs_user_id", "user_id"),
        Index("ix_audit_logs_route", "route"),
    )

    @classmethod
    def log(
        cls,
        action: str,
        entity: str,
        entity_id: int | str | None,
        data: dict[str, Any] | None = None,
    ) -> "AuditLog":
        """Persist an audit entry capturing request metadata when present."""
        uid: Optional[int] = None
        ip = None
        ua = None
        route = None
        method = None
        status_code = None
        latency_ms = None
        request_id = getattr(g, "request_id", None)
        extra_ctx: dict[str, Any] | None = None
        if has_request_context():
            ip = request.remote_addr
            ua = request.user_agent.string
            route = request.path
            method = request.method
            status_code = getattr(g, "response_status_code", None)
            if getattr(current_user, "is_authenticated", False):
                uid_raw = current_user.get_id()
                try:
                    uid = int(uid_raw) if uid_raw is not None else None
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    uid = None
            latency = getattr(g, "request_latency_ms", None)
            if latency is not None:
                latency_ms = int(latency)
            if hasattr(g, "audit_extra"):
                extra_ctx = getattr(g, "audit_extra") or None
        record = cls(
            user_id=uid,
            action=action,
            entity=entity,
            entity_id=str(entity_id) if entity_id is not None else None,
            data=_json_safe(data) if data is not None else None,
            request_id=request_id,
            actor_ip=ip,
            actor_ua=ua,
            route=route,
            http_method=method,
            status_code=status_code,
            latency_ms=latency_ms,
            extra_json=_json_safe(extra_ctx) if extra_ctx is not None else None,
        )
        db.session.add(record)
        db.session.commit()
        return record
