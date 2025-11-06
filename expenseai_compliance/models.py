"""Shared enums and helper dataclasses for compliance processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional


class CheckType(str, Enum):
    GST_VENDOR = "GST_VENDOR"
    GST_COMPANY = "GST_COMPANY"
    HSN_RATE = "HSN_RATE"
    ARITHMETIC = "ARITHMETIC"


class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    NEEDS_API = "NEEDS_API"
    ERROR = "ERROR"


class FindingSeverity(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class Finding:
    """Convenience representation of a compliance finding prior to persistence."""

    check_type: CheckType
    severity: FindingSeverity
    code: str
    message: str
    evidence: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckResult:
    """Aggregated outcome of a compliance module."""

    check_type: CheckType
    status: CheckStatus
    summary: str
    score: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)
    findings: List[Finding] = field(default_factory=list)

    def extend_findings(self, entries: Iterable[Finding]) -> None:
        self.findings.extend(entries)

    @property
    def created_at(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
