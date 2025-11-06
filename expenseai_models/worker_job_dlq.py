"""Dead letter queue entries for background jobs."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class WorkerJobDLQ(db.Model):
    """Capture failed background jobs for later inspection."""

    __tablename__ = "worker_job_dlq"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_type: Mapped[str] = mapped_column(String(32), nullable=False)
    invoice_id: Mapped[Optional[int]] = mapped_column(db.ForeignKey("invoices.id"), nullable=True)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(db.JSON, nullable=True)
    error_text: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    last_attempt_at: Mapped[datetime | None] = mapped_column(nullable=True)
