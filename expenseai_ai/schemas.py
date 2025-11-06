"""Pydantic models representing Finvela invoice parsing responses."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class HeaderConfidence(BaseModel):
    """Confidence scores for each header field returned by Gemini."""

    invoice_no: float = Field(..., ge=0.0, le=1.0)
    invoice_date: float = Field(..., ge=0.0, le=1.0)
    vendor_gst: float = Field(..., ge=0.0, le=1.0)
    company_gst: float = Field(..., ge=0.0, le=1.0)
    currency: float = Field(..., ge=0.0, le=1.0)
    subtotal: float = Field(..., ge=0.0, le=1.0)
    tax_total: float = Field(..., ge=0.0, le=1.0)
    grand_total: float = Field(..., ge=0.0, le=1.0)

    @field_validator("invoice_no", "invoice_date", "vendor_gst", "company_gst", "currency", "subtotal", "tax_total", "grand_total", mode="before")
    @classmethod
    def _coerce_float(cls, value: float | int | str) -> float:
        """Ensure numeric inputs are converted to bounded floats."""
        if isinstance(value, str):
            value = float(value.strip()) if value.strip() else 0.0
        return float(value)


class Header(BaseModel):
    """Structured header fields extracted from an invoice."""

    invoice_no: Optional[str]
    invoice_date: Optional[date]
    vendor_gst: Optional[str]
    company_gst: Optional[str]
    currency: Optional[str]
    subtotal: Optional[Decimal]
    tax_total: Optional[Decimal]
    grand_total: Optional[Decimal]
    per_field_confidence: HeaderConfidence

    @field_validator("invoice_no", "vendor_gst", "company_gst", mode="before")
    @classmethod
    def _strip_strings(cls, value: Optional[str]) -> Optional[str]:
        """Normalize empty strings to `None` and trim whitespace."""
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("currency", mode="before")
    @classmethod
    def _normalize_currency(cls, value: Optional[str]) -> Optional[str]:
        """Ensure currency codes are uppercase ISO 4217 strings."""
        if value is None:
            return None
        value = value.strip().upper()
        if value and len(value) == 3:
            return value
        if not value:
            return None
        raise ValueError("currency must be a 3-letter ISO 4217 code")

    @field_validator("invoice_date", mode="before")
    @classmethod
    def _parse_date(cls, value: Optional[str | date]) -> Optional[date]:
        """Accept ISO formatted strings and coerce to `date` instances."""
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        return datetime.fromisoformat(str(value)).date()

    @field_validator("subtotal", "tax_total", "grand_total", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: Optional[str | float | int | Decimal]) -> Optional[Decimal]:
        """Coerce numeric inputs to `Decimal` for precision preservation."""
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def critical_confidence_mean(self, keys: Iterable[str]) -> Optional[float]:
        """Return the arithmetic mean of the configured header confidences."""
        values: List[float] = []
        for key in keys:
            value = getattr(self.per_field_confidence, key, None)
            if value is not None:
                values.append(float(value))
        if not values:
            return None
        return sum(values) / len(values)


class LineItemModel(BaseModel):
    """Validates a single line item entry from Gemini."""

    line_no: int = Field(..., ge=1)
    description_raw: str
    hsn_sac: Optional[str] = None
    qty: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None
    gst_rate: Optional[Decimal] = None
    line_subtotal: Optional[Decimal] = None
    line_tax: Optional[Decimal] = None
    line_total: Optional[Decimal] = None
    confidence: float = Field(..., ge=0.0, le=1.0)

    @field_validator("description_raw", mode="before")
    @classmethod
    def _ensure_description(cls, value: str) -> str:
        """Guarantee line items always contain descriptive text."""
        if isinstance(value, str) and value.strip():
            return value.strip()
        raise ValueError("description_raw is required")

    @field_validator("hsn_sac", mode="before")
    @classmethod
    def _trim_optional(cls, value: Optional[str]) -> Optional[str]:
        """Normalize optional text values to `None` when empty."""
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("qty", "unit_price", "gst_rate", "line_subtotal", "line_tax", "line_total", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: Optional[str | float | int | Decimal]) -> Optional[Decimal]:
        """Convert numeric inputs to `Decimal` preserving scale."""
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class DuplicateMatch(BaseModel):
    """Represents a potential duplicate invoice reference."""

    invoice_reference: Optional[str] = None
    similarity: Optional[float] = Field(None, ge=0.0, le=1.0)
    reason: Optional[str] = None

    @field_validator("similarity", mode="before")
    @classmethod
    def _coerce_similarity(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class DuplicateCheck(BaseModel):
    """Duplicate detection outcome for the parsed invoice."""

    status: Literal["clear", "possible", "flagged"] = "possible"
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    matches: List[DuplicateMatch] = Field(default_factory=list)
    rationale: Optional[str] = None

    @field_validator("confidence", mode="before")
    @classmethod
    def _coerce_confidence(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class GSTValidationEntry(BaseModel):
    """Validation result for a single GST registration number."""

    gst_number: Optional[str] = None
    valid: Optional[bool] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    source: Optional[str] = None
    detail: Optional[str] = None

    @field_validator("gst_number", mode="before")
    @classmethod
    def _trim_gst(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("confidence", mode="before")
    @classmethod
    def _coerce_confidence(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class GSTValidationSummary(BaseModel):
    """Collective GST validation results for vendor and company."""

    vendor: Optional[GSTValidationEntry] = None
    company: Optional[GSTValidationEntry] = None


class HSNRateViolation(BaseModel):
    """Represents a mismatch between billed and expected GST rates."""

    line_no: Optional[int] = None
    billed_rate: Optional[Decimal] = None
    expected_rate: Optional[Decimal] = None
    description: Optional[str] = None

    @field_validator("billed_rate", "expected_rate", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: Optional[str | float | int | Decimal]) -> Optional[Decimal]:
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class HSNRateCheck(BaseModel):
    """Checks whether line item GST rates align with HSN/SAC expectations."""

    status: Literal["aligned", "mismatch", "unknown"] = "unknown"
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    violations: List[HSNRateViolation] = Field(default_factory=list)

    @field_validator("confidence", mode="before")
    @classmethod
    def _coerce_confidence(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class ArithmeticDiscrepancy(BaseModel):
    """Details arithmetic mismatches between invoice totals."""

    field: Optional[str] = None
    expected: Optional[Decimal] = None
    actual: Optional[Decimal] = None
    difference: Optional[Decimal] = None
    note: Optional[str] = None

    @field_validator("expected", "actual", "difference", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: Optional[str | float | int | Decimal]) -> Optional[Decimal]:
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class ArithmeticCheck(BaseModel):
    """Arithmetic integrity assessment for totals and taxes."""

    passes: Optional[bool] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    discrepancies: List[ArithmeticDiscrepancy] = Field(default_factory=list)
    recomputed_totals: Dict[str, Decimal | None] = Field(default_factory=dict)

    @field_validator("confidence", mode="before")
    @classmethod
    def _coerce_confidence(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)

    @field_validator("recomputed_totals", mode="before")
    @classmethod
    def _coerce_totals(cls, value: Optional[Dict[str, str | float | int | Decimal]]) -> Dict[str, Decimal | None]:
        if value is None:
            return {}
        coerced: Dict[str, Decimal | None] = {}
        for key, raw in value.items():
            if raw in (None, ""):
                coerced[key] = None
            elif isinstance(raw, Decimal):
                coerced[key] = raw
            else:
                coerced[key] = Decimal(str(raw))
        return coerced


class PriceOutlier(BaseModel):
    """Represents an item whose price deviates from market baselines."""

    line_no: Optional[int] = None
    description: Optional[str] = None
    billed_price: Optional[Decimal] = None
    market_average: Optional[Decimal] = None
    delta_percent: Optional[float] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)

    @field_validator("billed_price", "market_average", mode="before")
    @classmethod
    def _coerce_decimal(cls, value: Optional[str | float | int | Decimal]) -> Optional[Decimal]:
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @field_validator("delta_percent", "confidence", mode="before")
    @classmethod
    def _coerce_float(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class PriceOutlierCheck(BaseModel):
    """Price benchmarking verdict based on AI grounding or historical data."""

    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    method: Optional[str] = None
    outliers: List[PriceOutlier] = Field(default_factory=list)

    @field_validator("confidence", mode="before")
    @classmethod
    def _coerce_confidence(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class InvoiceAnalysis(BaseModel):
    """Higher-level insights derived from the parsed invoice."""

    estimated_accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    duplicate_check: Optional[DuplicateCheck] = None
    gst_validation: Optional[GSTValidationSummary] = None
    hsn_rate_check: Optional[HSNRateCheck] = None
    arithmetic_check: Optional[ArithmeticCheck] = None
    price_outlier_check: Optional[PriceOutlierCheck] = None

    @field_validator("estimated_accuracy", mode="before")
    @classmethod
    def _coerce_accuracy(cls, value: Optional[str | float | int]) -> Optional[float]:
        if value in (None, ""):
            return None
        return float(value)


class ParseResult(BaseModel):
    """Normalized invoice extraction payload."""

    header: Header
    line_items: List[LineItemModel]
    pages_parsed: int = Field(..., ge=1)
    analysis: Optional[InvoiceAnalysis] = None

    @model_validator(mode="after")
    def _validate_confidences(self) -> "ParseResult":
        """Ensure header confidence keys align with provided header data."""
        expected = {
            "invoice_no",
            "invoice_date",
            "vendor_gst",
            "company_gst",
            "currency",
            "subtotal",
            "tax_total",
            "grand_total",
        }
        if set(self.header.per_field_confidence.model_dump().keys()) != expected:
            raise ValueError("per_field_confidence keys do not match header requirements")
        return self

    @classmethod
    def from_gemini_payload(cls, raw: dict) -> "ParseResult":
        """Validate and normalize a raw Finvela payload into a `ParseResult`."""
        try:
            return cls.model_validate(raw)
        except ValidationError as exc:  # pragma: no cover - defensive validation path
            raise ValueError(f"Invalid Finvela payload: {exc}") from exc

    def critical_confidence_mean(self, keys: Iterable[str]) -> Optional[float]:
        """Delegate to the header convenience helper for critical fields."""
        return self.header.critical_confidence_mean(keys)
