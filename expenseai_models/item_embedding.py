"""Embeddings cached for normalized line item descriptions."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from expenseai_ext.db import db


class ItemEmbedding(db.Model):
    """Stores an embedding vector for a normalized item description."""

    __tablename__ = "item_embeddings"
    __table_args__ = (Index("ix_item_embeddings_hash_key", "hash_key", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    hash_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    text_norm: Mapped[str] = mapped_column(Text, nullable=False)
    vector: Mapped[list[float]] = mapped_column(db.JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<ItemEmbedding {self.id} {self.hash_key[:8]}>"
