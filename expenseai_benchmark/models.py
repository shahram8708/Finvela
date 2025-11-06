"""Dataclasses used by benchmarking services."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class BaselineResult:
    median: Decimal | None
    mad: Decimal | None
    sample_count: int
    used_external: bool = False
    external_source: str | None = None


@dataclass(slots=True)
class LineBenchmark:
    line_no: int
    description: str
    unit_price: Decimal | None
    qty: Decimal | None
    currency: str | None
    baseline: BaselineResult
    robust_z: float
    outlier_score: float
