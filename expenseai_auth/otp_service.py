"""Helpers for issuing and verifying one-time passcodes."""
from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Any, Tuple

from flask import current_app

from expenseai_ext.db import db
from expenseai_models.audit import AuditLog
from expenseai_models.otp import OneTimePasscode
from expenseai_models.user import User


class OtpError(Exception):
    """Base class for OTP failures."""


class OtpNotFoundError(OtpError):
    """Raised when no matching OTP record exists."""


class OtpExpiredError(OtpError):
    """Raised when the OTP record has expired."""


class OtpAttemptsExceededError(OtpError):
    """Raised when the OTP is exhausted due to too many attempts."""


class OtpThrottleError(OtpError):
    """Raised when a resend is attempted too soon."""


class OtpValidationError(OtpError):
    """Raised when the supplied code does not match the hash."""


_HASH_PREFIX = "pbkdf2_sha256"
_HASH_ITERATIONS = 200_000


def _hash_code(code: str) -> str:
    """Hash an OTP value using PBKDF2-HMAC-SHA256."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", code.encode("utf-8"), salt, _HASH_ITERATIONS)
    return f"{_HASH_PREFIX}${_HASH_ITERATIONS}${salt.hex()}${digest.hex()}"


def _verify_code(code: str, stored: str) -> bool:
    """Compare an OTP against its stored hash in constant time."""
    try:
        prefix, iter_str, salt_hex, digest_hex = stored.split("$")
        if prefix != _HASH_PREFIX:
            return False
        iterations = int(iter_str)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except (ValueError, TypeError):  # pragma: no cover - defensive parsing
        return False
    computed = hashlib.pbkdf2_hmac("sha256", code.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(expected, computed)


def _now() -> datetime:
    return datetime.utcnow()


def _issue_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _expiry_timestamp() -> datetime:
    minutes = int(current_app.config.get("OTP_EXPIRY_MINUTES", 10))
    return _now() + timedelta(minutes=max(minutes, 1))


def _max_attempts() -> int:
    attempts = int(current_app.config.get("OTP_MAX_ATTEMPTS", 5))
    return max(attempts, 1)


def issue_otp(user: User, *, purpose: str, metadata: dict[str, Any] | None = None) -> Tuple[str, OneTimePasscode]:
    """Create (or replace) an OTP for the user and return the raw code."""
    metadata = metadata or {}
    OneTimePasscode.query.filter_by(user_id=user.id, purpose=purpose).delete()
    code = _issue_code()
    otp = OneTimePasscode(
        user=user,
        purpose=purpose,
        code_hash=_hash_code(code),
        expires_at=_expiry_timestamp(),
        attempts_remaining=_max_attempts(),
        metadata_json=metadata,
    )
    db.session.add(otp)
    db.session.commit()
    AuditLog.log(
        action="otp_issued",
        entity="otp",
        entity_id=otp.id,
        data={"purpose": purpose, "user_id": user.id},
    )
    return code, otp


def get_active_otp(user: User, *, purpose: str) -> OneTimePasscode | None:
    """Return the active OTP record for a user and purpose."""
    record = (
        OneTimePasscode.query.filter_by(user_id=user.id, purpose=purpose)
        .order_by(OneTimePasscode.created_at.desc())
        .first()
    )
    if record and record.expires_at < _now():
        db.session.delete(record)
        db.session.commit()
        raise OtpExpiredError("OTP has expired")
    return record


def verify_otp(user: User, *, purpose: str, candidate: str) -> dict[str, Any]:
    """Validate an OTP submission for the user and return metadata."""
    record = (
        OneTimePasscode.query.filter_by(user_id=user.id, purpose=purpose)
        .order_by(OneTimePasscode.created_at.desc())
        .first()
    )
    if not record:
        raise OtpNotFoundError("OTP is not available for this user")
    if record.expires_at < _now():
        db.session.delete(record)
        db.session.commit()
        raise OtpExpiredError("OTP has expired")

    if not _verify_code(candidate, record.code_hash):
        record.attempts_remaining = max(record.attempts_remaining - 1, 0)
        db.session.add(record)
        db.session.commit()
        AuditLog.log(
            action="otp_failed",
            entity="otp",
            entity_id=record.id,
            data={"remaining": record.attempts_remaining, "purpose": purpose, "user_id": user.id},
        )
        if record.attempts_remaining <= 0:
            db.session.delete(record)
            db.session.commit()
            raise OtpAttemptsExceededError("Maximum attempts reached")
        raise OtpValidationError("OTP does not match")

    metadata = record.metadata_json or {}
    db.session.delete(record)
    db.session.commit()
    AuditLog.log(
        action="otp_verified",
        entity="otp",
        entity_id=record.id,
        data={"purpose": purpose, "user_id": user.id},
    )
    return metadata


def ensure_resend_allowed(record: OneTimePasscode) -> None:
    """Validate resend throttle before issuing a new OTP."""
    throttle_seconds = int(current_app.config.get("RESEND_THROTTLE_SECONDS", 60))
    delta = _now() - (record.updated_at or record.created_at)
    if delta.total_seconds() < max(throttle_seconds, 1):
        raise OtpThrottleError("Please wait before requesting another code")


def resend_otp(record: OneTimePasscode) -> Tuple[str, OneTimePasscode]:
    """Reissue a new OTP using metadata from an existing record."""
    ensure_resend_allowed(record)
    metadata = record.metadata_json or {}
    user = record.user
    purpose = record.purpose
    db.session.delete(record)
    db.session.commit()
    return issue_otp(user, purpose=purpose, metadata=metadata)
