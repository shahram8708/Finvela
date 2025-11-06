"""GSTIN validation providers with caching and graceful degradation."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from flask import current_app

from expenseai_compliance.models import CheckStatus
from expenseai_ext import cache

GSTIN_PATTERN = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[A-Z0-9]{1}Z[0-9A-Z]{1}$")

CACHE_PREFIX = "gst-validation"


class GstProviderBase:
    """Abstract provider with caching helpers."""

    def __init__(self, app=None) -> None:
        self.app = app or current_app
        self.timeout = 20

    def validate_gstin(self, gstin: str) -> Dict[str, Any]:  # pragma: no cover - interface
        raise NotImplementedError

    # Shared helpers -----------------------------------------------------

    def _cache_key(self, gstin: str) -> str:
        provider_name = self.__class__.__name__.lower()
        return f"{CACHE_PREFIX}:{provider_name}:{gstin}"

    def _get_cached(self, gstin: str) -> Optional[Dict[str, Any]]:
        ttl = self.app.config.get("GST_CACHE_TTL_SECS", 86400)
        data = cache.get(self._cache_key(gstin))
        if not data:
            return None
        if time.time() - data.get("cached_at", 0) > ttl:
            cache.delete(self._cache_key(gstin))
            return None
        return data.get("payload")

    def _set_cache(self, gstin: str, payload: Dict[str, Any]) -> None:
        cache.set(self._cache_key(gstin), {"payload": payload, "cached_at": time.time()})

    def _mask(self, value: str | None) -> str:
        if not value:
            return ""  # pragma: no cover - defensive
        return value[:4] + "***"

    def _http_get(self, url: str, headers: Dict[str, str]) -> requests.Response:
        self.app.logger.debug("GST provider request", extra={"url": url, "headers": {k: self._mask(v) for k, v in headers.items()}})
        response = requests.get(url, headers=headers, timeout=self.timeout)
        return response


class NoneProvider(GstProviderBase):
    """Provider stub used when GST verification is disabled."""

    def validate_gstin(self, gstin: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": "UNKNOWN",
            "reason": "GST provider disabled",
            "raw": {},
        }


class TestFixtureProvider(GstProviderBase):
    """Offline provider that validates GSTINs against a JSON fixture file."""

    def __init__(self, app=None) -> None:
        super().__init__(app)
        default_fixture = Path(__file__).with_name("gst_test_numbers.json")
        configured_path = self.app.config.get("GST_TEST_FIXTURE_PATH")
        self.fixture_path = Path(configured_path) if configured_path else default_fixture
        self._index: Optional[Dict[str, Dict[str, Any]]] = None

    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        if self._index is not None:
            return self._index

        try:
            with self.fixture_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            self.app.logger.warning(
                "GST test fixture not found; all GSTINs will be treated as unverified",
                extra={"fixture_path": str(self.fixture_path)},
            )
            self._index = {}
            return self._index
        except json.JSONDecodeError as exc:
            self.app.logger.warning(
                "GST test fixture could not be parsed; all GSTINs will be treated as unverified",
                extra={"fixture_path": str(self.fixture_path), "error": str(exc)},
            )
            self._index = {}
            return self._index

        index: Dict[str, Dict[str, Any]] = {}
        if isinstance(payload, list):
            for entry in payload:
                if isinstance(entry, str):
                    index[entry.strip().upper()] = {}
                elif isinstance(entry, dict):
                    gstin = entry.get("gst_number") or entry.get("gstin")
                    if isinstance(gstin, str):
                        normalized = gstin.strip().upper()
                        data = {k: v for k, v in entry.items() if k not in {"gst_number", "gstin"}}
                        index[normalized] = data
        elif isinstance(payload, dict):
            for key, value in payload.items():
                if not isinstance(key, str):
                    continue
                normalized = key.strip().upper()
                if isinstance(value, dict):
                    index[normalized] = value
                elif isinstance(value, str):
                    index[normalized] = {"legal_name": value}
                else:
                    index[normalized] = {}

        self._index = index
        return self._index

    def validate_gstin(self, gstin: str) -> Dict[str, Any]:
        if not gstin:
            return {
                "ok": False,
                "status": "INVALID",
                "reason": "empty_gstin",
                "raw": {"source": "fixture", "match": False},
            }

        normalized = gstin.strip().upper()
        index = self._load_index()

        if not self.fixture_path.exists():
            return {
                "ok": False,
                "status": "UNKNOWN",
                "reason": "fixture_missing",
                "raw": {"source": "fixture", "fixture_path": str(self.fixture_path)},
            }

        record = index.get(normalized)
        base_payload = {
            "raw": {
                "source": "fixture",
                "fixture_path": str(self.fixture_path),
                "match": bool(record),
                "gstin": normalized,
            }
        }

        if record is None:
            base_payload.update({
                "ok": False,
                "status": "INVALID",
                "reason": "gstin_not_found",
            })
            return base_payload

        payload = {
            "ok": True,
            "status": "VALID",
            "legal_name": record.get("legal_name"),
            "state": record.get("state"),
            "last_verified_at": record.get("last_verified_at"),
            "raw": base_payload["raw"],
        }
        return payload


class ClearTaxProvider(GstProviderBase):
    """Integrates with the ClearTax GST verification API."""

    def validate_gstin(self, gstin: str) -> Dict[str, Any]:
        cached = self._get_cached(gstin)
        if cached:
            return cached

        base_url = self.app.config.get("GST_API_BASE_URL") or "https://api.cleartax.in"
        api_key = self.app.config.get("GST_API_KEY")
        if not api_key:
            return {
                "ok": False,
                "status": "NEEDS_API",
                "reason": "Missing ClearTax API key",
                "raw": {},
            }

        url = f"{base_url.rstrip('/')}/gst/v2/gstin/{gstin}"  # sample endpoint
        headers = {
            "x-cleartax-auth-token": api_key,
            "accept": "application/json",
        }

        try:
            response = self._http_get(url, headers)
        except requests.Timeout as exc:  # pragma: no cover - runtime path
            self.app.logger.warning("GST validation timeout", extra={"gstin": gstin, "provider": "cleartax"})
            return {
                "ok": False,
                "status": "WARN",
                "reason": "timeout",
                "raw": {"error": str(exc)},
            }
        except requests.RequestException as exc:  # pragma: no cover - runtime path
            self.app.logger.error("GST validation error", extra={"gstin": gstin, "provider": "cleartax", "error": str(exc)})
            return {
                "ok": False,
                "status": "WARN",
                "reason": "network_error",
                "raw": {"error": str(exc)},
            }

        if response.status_code == 401:
            return {
                "ok": False,
                "status": "NEEDS_API",
                "reason": "unauthorized",
                "raw": {"status_code": response.status_code},
            }
        if response.status_code == 429:
            return {
                "ok": False,
                "status": "WARN",
                "reason": "rate_limited",
                "raw": {"status_code": response.status_code},
            }
        if response.status_code >= 500:
            return {
                "ok": False,
                "status": "WARN",
                "reason": "server_error",
                "raw": {"status_code": response.status_code},
            }

        data = response.json()
        payload = {
            "ok": data.get("status") == "ACTIVE",
            "status": "VALID" if data.get("status") == "ACTIVE" else "INVALID",
            "legal_name": data.get("legal_name"),
            "state": data.get("state"),
            "last_verified_at": data.get("last_verified_on"),
            "raw": {"status": data.get("status"), "state": data.get("state")},
        }
        self._set_cache(gstin, payload)
        return payload


class MastersIndiaProvider(GstProviderBase):
    """Integrates with the Masters India GST API."""

    def validate_gstin(self, gstin: str) -> Dict[str, Any]:
        cached = self._get_cached(gstin)
        if cached:
            return cached

        base_url = self.app.config.get("GST_API_BASE_URL") or "https://gstapi.mastersindia.co"
        api_key = self.app.config.get("GST_API_KEY")
        api_secret = self.app.config.get("GST_API_SECRET")
        if not api_key or not api_secret:
            return {
                "ok": False,
                "status": "NEEDS_API",
                "reason": "missing_credentials",
                "raw": {},
            }

        url = f"{base_url.rstrip('/')}/v1/gstin/{gstin}"
        headers = {
            "client_id": api_key,
            "client_secret": api_secret,
            "accept": "application/json",
        }
        try:
            response = self._http_get(url, headers)
        except requests.Timeout as exc:  # pragma: no cover
            self.app.logger.warning("GST validation timeout", extra={"gstin": gstin, "provider": "mastersindia"})
            return {
                "ok": False,
                "status": "WARN",
                "reason": "timeout",
                "raw": {"error": str(exc)},
            }
        except requests.RequestException as exc:  # pragma: no cover
            self.app.logger.error("GST validation error", extra={"gstin": gstin, "provider": "mastersindia", "error": str(exc)})
            return {
                "ok": False,
                "status": "WARN",
                "reason": "network_error",
                "raw": {"error": str(exc)},
            }

        if response.status_code == 401:
            return {
                "ok": False,
                "status": "NEEDS_API",
                "reason": "unauthorized",
                "raw": {"status_code": response.status_code},
            }
        if response.status_code == 429:
            return {
                "ok": False,
                "status": "WARN",
                "reason": "rate_limited",
                "raw": {"status_code": response.status_code},
            }
        if response.status_code >= 500:
            return {
                "ok": False,
                "status": "WARN",
                "reason": "server_error",
                "raw": {"status_code": response.status_code},
            }

        data = response.json().get("data", {})
        status_code = data.get("stjCd")
        payload = {
            "ok": data.get("sts") == "Active",
            "status": "VALID" if data.get("sts") == "Active" else "INVALID",
            "legal_name": data.get("lgnm"),
            "state": status_code,
            "last_verified_at": data.get("lstupdt"),
            "raw": {"sts": data.get("sts"), "stjCd": status_code},
        }
        self._set_cache(gstin, payload)
        return payload


def normalize_gstin(value: str | None) -> str:
    """Return an uppercase GSTIN stripped of whitespace and punctuation."""
    if not value:
        return ""
    cleaned = re.sub(r"[^0-9A-Za-z]", "", value)
    return cleaned.upper()


def get_provider(app=None) -> GstProviderBase:
    """Factory returning the configured GST provider implementation."""
    app = app or current_app
    provider = (app.config.get("GST_PROVIDER") or "none").lower()
    test_mode_enabled = app.config.get("GST_TEST_MODE_ENABLED", True)

    def _test_provider() -> GstProviderBase:
        app.logger.info(
            "GST validation running in test mode using fixture data",
            extra={"fixture_path": app.config.get("GST_TEST_FIXTURE_PATH")},
        )
        return TestFixtureProvider(app)

    if provider in {"test", "dummy", "fixture"}:
        return _test_provider()

    if provider == "cleartax":
        if not app.config.get("GST_API_KEY") and test_mode_enabled:
            return _test_provider()
        return ClearTaxProvider(app)

    if provider == "mastersindia":
        if (not app.config.get("GST_API_KEY") or not app.config.get("GST_API_SECRET")) and test_mode_enabled:
            return _test_provider()
        return MastersIndiaProvider(app)

    if provider == "none" and test_mode_enabled:
        return _test_provider()

    return NoneProvider(app)


def validate_format(gstin: str | None) -> bool:
    """Return True if GSTIN matches the structural regex."""
    normalized = normalize_gstin(gstin)
    if not normalized:
        return False
    return bool(GSTIN_PATTERN.fullmatch(normalized))


def classify_provider_status(result: Dict[str, Any]) -> CheckStatus:
    """Map provider responses to CheckStatus for ComplianceCheck summary."""
    status = result.get("status")
    if status == "VALID":
        return CheckStatus.PASS
    if status == "INVALID":
        return CheckStatus.FAIL
    if status == "UNKNOWN":
        return CheckStatus.WARN
    if status == "NEEDS_API":
        return CheckStatus.NEEDS_API
    return CheckStatus.WARN
