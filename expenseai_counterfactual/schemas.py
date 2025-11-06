"""Pydantic schemas used by counterfactual endpoints."""
from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CounterfactLineChange(BaseModel):
    """Describes user-proposed overrides for a single invoice line."""

    line_no: int = Field(gt=0)
    hsn_sac: Optional[str] = Field(default=None, max_length=64)
    qty: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None
    gst_rate: Optional[Decimal] = None

    model_config = ConfigDict(extra="forbid")


class CounterfactRequest(BaseModel):
    """Incoming payload for a what-if evaluation."""

    invoice_id: int = Field(gt=0)
    header_overrides: dict[str, str | None] | None = None
    line_changes: List[CounterfactLineChange] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")

    @field_validator("line_changes")
    @classmethod
    def ensure_unique_lines(cls, value: List[CounterfactLineChange]) -> List[CounterfactLineChange]:
        seen: set[int] = set()
        for change in value:
            if change.line_no in seen:
                raise ValueError(f"Duplicate line_no {change.line_no} in counterfactual request")
            seen.add(change.line_no)
        return value

    @model_validator(mode="after")
    def ensure_changes(self) -> "CounterfactRequest":
        if not self.line_changes:
            raise ValueError("At least one line change is required")
        return self


class CounterfactTotals(BaseModel):
    subtotal: Decimal
    tax_total: Decimal
    grand_total: Decimal

    model_config = ConfigDict(json_encoders={Decimal: lambda v: float(v)})


class CounterfactContributor(BaseModel):
    name: str
    weight: float
    raw_score: float
    contribution: float
    details: dict[str, object]


class CounterfactRiskSnapshot(BaseModel):
    composite: float
    policy_version: str
    contributors: List[CounterfactContributor]


class CounterfactResponse(BaseModel):
    invoice_id: int
    totals_before: CounterfactTotals
    totals_after: CounterfactTotals
    totals_delta: CounterfactTotals

    risk_before: CounterfactRiskSnapshot
    risk_after: CounterfactRiskSnapshot
    delta_composite: float
    notes: List[str] = Field(default_factory=list)

    model_config = ConfigDict(json_encoders={Decimal: lambda v: float(v)})
