"""Application configuration classes and helpers."""
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterable

from dotenv import load_dotenv

# Ensure environment variables from a local .env file are available during
# development. Production deployments should rely on platform-specific secrets
# management instead of an on-disk .env file.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)


def _bool(value: str | None, default: bool = False) -> bool:
    """Parse environment flags such as "true", "1", "yes"."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y"}


def _split(value: str | None, default: Iterable[str]) -> list[str]:
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _split_paths(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(":") if item.strip()]


class BaseConfig:
    """Shared configuration defaults used by every environment."""

    APP_NAME = "Finvela"
    VERSION = "0.1.0"

    SECRET_KEY = os.getenv("SECRET_KEY", "please-change-me")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///expenseai.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyDa20LzWUY04hoqASDyxuE50yV7v_1vJWw")
    GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
    GEMINI_REQUEST_TIMEOUT = int(os.getenv("GEMINI_REQUEST_TIMEOUT", "90"))
    GEMINI_MAX_RETRIES = int(os.getenv("GEMINI_MAX_RETRIES", "3"))
    GEMINI_RETRY_BACKOFF_SECS = float(os.getenv("GEMINI_RETRY_BACKOFF_SECS", "2"))
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "gemini-2.5-flash-lite")
    EMBEDDING_DISABLE_REMOTE = _bool(os.getenv("EMBEDDING_DISABLE_REMOTE"), default=False)

    # Parser behaviour knobs allow operators to tune performance vs. cost.
    PARSER_MAX_PAGES = int(os.getenv("PARSER_MAX_PAGES", "6"))
    AUTO_PARSE_ON_UPLOAD = _bool(os.getenv("AUTO_PARSE_ON_UPLOAD"), default=True)
    APP_DISABLE_BG_PARSER = _bool(os.getenv("APP_DISABLE_BG_PARSER"), default=False)

    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", REDIS_URL)
    CELERY_RESULT_EXPIRES = int(os.getenv("CELERY_RESULT_EXPIRES", "3600"))
    CELERY_VISIBILITY_TIMEOUT = int(os.getenv("CELERY_VISIBILITY_TIMEOUT", "3600"))
    CELERY_TASK_DEFAULT_QUEUE = os.getenv("CELERY_TASK_DEFAULT_QUEUE", "default")
    CELERY_TASK_ALWAYS_EAGER = _bool(os.getenv("CELERY_EAGER"), default=False)

    COUNTERFACT_MAX_LINES = int(os.getenv("COUNTERFACT_MAX_LINES", "200"))
    COUNTERFACT_MAX_DELTA_PCT = float(os.getenv("COUNTERFACT_MAX_DELTA_PCT", "0.5"))

    SESSION_COOKIE_NAME = "expenseai_session"
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = _bool(os.getenv("SECURE_COOKIES"), default=False)
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_DURATION = timedelta(days=30)
    REMEMBER_COOKIE_SECURE = SESSION_COOKIE_SECURE
    REMEMBER_COOKIE_SAMESITE = SESSION_COOKIE_SAMESITE

    WTF_CSRF_TIME_LIMIT = 3600

    CACHE_TYPE = "SimpleCache"
    CACHE_DEFAULT_TIMEOUT = 300

    RATELIMIT_HEADERS_ENABLED = True
    RATELIMIT_DEFAULT = "100 per minute"

    SECURITY_HEADERS = True
    CONTENT_SECURITY_POLICY: Dict[str, str] = {
        "default-src": "'self'",
        "style-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com",
        "script-src": "'self' https://cdn.jsdelivr.net https://checkout.razorpay.com",
        "font-src": "'self' https://cdn.jsdelivr.net https://fonts.gstatic.com data:",
        "img-src": "'self' data:",
        "connect-src": "'self' https://cdn.jsdelivr.net https://api.razorpay.com https://lumberjack.razorpay.com",
        "frame-src": "'self' https://checkout.razorpay.com https://api.razorpay.com",
        "object-src": "'none'",
        "frame-ancestors": "'self'",
    }

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FORMAT = os.getenv("LOG_FORMAT", "json").lower()
    REQUEST_BODY_LOG_MAX = int(os.getenv("REQUEST_BODY_LOG_MAX", "2048"))
    REDACT_KEYS = _split(
        os.getenv("REDACT_KEYS"),
        {"GEMINI_API_KEY", "WHATSAPP_ACCESS_TOKEN", "TWILIO_AUTH_TOKEN", "Authorization"},
    )
    JSON_SORT_KEYS = False
    PREFERRED_URL_SCHEME = "https" if SESSION_COOKIE_SECURE else "http"

    ALLOW_SELF_REGISTRATION = _bool(os.getenv("ALLOW_SELF_REGISTRATION"), default=True)
    ALLOW_INVITE_REGISTRATION = _bool(os.getenv("ALLOW_INVITE_REGISTRATION"), default=True)
    INVITE_CODE_LENGTH = int(os.getenv("INVITE_CODE_LENGTH", "12"))
    INVITE_CODE_EXPIRY_HOURS = int(os.getenv("INVITE_CODE_EXPIRY_HOURS", "168"))
    _invite_code_max_uses = os.getenv("INVITE_CODE_MAX_USES")
    INVITE_CODE_MAX_USES = int(_invite_code_max_uses) if _invite_code_max_uses else None

    BABEL_DEFAULT_LOCALE = os.getenv("BABEL_DEFAULT_LOCALE", "en")
    BABEL_SUPPORTED_LOCALES = _split(os.getenv("BABEL_SUPPORTED_LOCALES"), {"en", "hi"})
    BABEL_TRANSLATION_DIRECTORIES = os.getenv(
        "BABEL_TRANSLATION_DIRECTORIES",
        str(Path(__file__).resolve().parent / "translations"),
    )

    # Flask-Limiter default limits can be overridden per route.
    RATES = {
        "LOGIN": os.getenv("RATE_LIMIT_LOGIN", "10 per minute"),
        "REGISTER": os.getenv("RATE_LIMIT_REGISTER", "5 per minute"),
        "OTP_SEND": os.getenv("RATE_LIMIT_OTP_SEND", "5 per minute"),
        "OTP_VERIFY": os.getenv("RATE_LIMIT_OTP_VERIFY", "12 per minute"),
        "PASSWORD_RESET": os.getenv("RATE_LIMIT_PASSWORD_RESET", "5 per minute"),
    }

    GLOBAL_RATE_LIMIT = os.getenv("GLOBAL_RATE_LIMIT", "500/minute")

    TRACING_ENABLED = _bool(os.getenv("TRACING_ENABLED"), default=False)
    TRACING_SAMPLE_RATE = float(os.getenv("TRACING_SAMPLE_RATE", "0.1"))

    MAX_RETRIES_TRANSIENT = int(os.getenv("MAX_RETRIES_TRANSIENT", "3"))
    BACKOFF_BASE_SECS = float(os.getenv("BACKOFF_BASE_SECS", "1.5"))
    CIRCUIT_FAIL_THRESHOLD = int(os.getenv("CIRCUIT_FAIL_THRESHOLD", "5"))
    CIRCUIT_RESET_SECS = int(os.getenv("CIRCUIT_RESET_SECS", "60"))
    WORKER_HEARTBEAT_SECS = int(os.getenv("WORKER_HEARTBEAT_SECS", "10"))
    WORKER_STALL_TIMEOUT_SECS = int(os.getenv("WORKER_STALL_TIMEOUT_SECS", "120"))

    IDEMPOTENCY_TTL_SECS = int(os.getenv("IDEMPOTENCY_TTL_SECS", "600"))

    SESSION_MAX_AGE_HOURS = int(os.getenv("SESSION_MAX_AGE_HOURS", "24"))
    LOGIN_LOCKOUT_THRESHOLD = int(os.getenv("LOGIN_LOCKOUT_THRESHOLD", "7"))
    LOGIN_LOCKOUT_WINDOW_MIN = int(os.getenv("LOGIN_LOCKOUT_WINDOW_MIN", "15"))

    AUDIT_RETENTION_DAYS = int(os.getenv("AUDIT_RETENTION_DAYS", "365"))
    EVENT_RETENTION_DAYS = int(os.getenv("EVENT_RETENTION_DAYS", "180"))
    PII_EXPORT_ENABLED = _bool(os.getenv("PII_EXPORT_ENABLED"), default=True)
    PII_DELETE_ENABLED = _bool(os.getenv("PII_DELETE_ENABLED"), default=True)

    FF_VENDOR_DRIFT_ALERTS = _bool(os.getenv("FF_VENDOR_DRIFT_ALERTS"), default=True)
    FF_WHATSAPP = _bool(os.getenv("FF_WHATSAPP"), default=True)

    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "ACe22c004acbbbef75cec10cd919dbea35")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "a244fad6a3ee4912734384f6e08988d8")
    TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "+14155238886")
    TWILIO_VALIDATE_SIGNATURE = _bool(os.getenv("TWILIO_VALIDATE_SIGNATURE"), default=True)
    WHATSAPP_SESSION_STALE_HOURS = int(os.getenv("WHATSAPP_SESSION_STALE_HOURS", "48"))
    WHATSAPP_AUTOCREATE_USERS = _bool(os.getenv("WHATSAPP_AUTOCREATE_USERS"), default=True)
    WHATSAPP_AUTOCREATE_EMAIL_DOMAIN = os.getenv("WHATSAPP_AUTOCREATE_EMAIL_DOMAIN", "auto.expenseai")

    MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "10"))
    MAX_CONTENT_LENGTH = MAX_UPLOAD_MB * 1024 * 1024
    UPLOAD_ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg"}
    UPLOAD_ALLOWED_MIME_TYPES = {
        "application/pdf",
        "image/png",
        "image/jpeg",
    }
    UPLOAD_STORAGE_DIR = os.getenv("UPLOAD_STORAGE_DIR", "uploads")
    THUMBNAIL_DIR = os.getenv("THUMBNAIL_DIR", "thumbnails")

    CHAT_UPLOAD_DIR = os.getenv("CHAT_UPLOAD_DIR", "chat_uploads")
    CHAT_ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg"}
    CHAT_ALLOWED_MIME_TYPES = {
        "application/pdf",
        "image/png",
        "image/jpeg",
    }
    CHAT_HISTORY_LIMIT = int(os.getenv("CHAT_HISTORY_LIMIT", "5"))

    INGEST_WATCH_PATHS = _split_paths(os.getenv("INGEST_WATCH_PATHS"))
    INGEST_POLL_SECS = int(os.getenv("INGEST_POLL_SECS", "10"))
    INGEST_MAX_FILE_MB = int(os.getenv("INGEST_MAX_FILE_MB", "20"))
    INGEST_EMAIL_HOST = os.getenv("INGEST_EMAIL_HOST", "")
    INGEST_EMAIL_USER = os.getenv("INGEST_EMAIL_USER", "")
    INGEST_EMAIL_PASS = os.getenv("INGEST_EMAIL_PASS", "")
    INGEST_EMAIL_SSL = _bool(os.getenv("INGEST_EMAIL_SSL"), default=True)
    INGEST_EMAIL_FOLDER = os.getenv("INGEST_EMAIL_FOLDER", "INBOX")

    # Placeholder Finvela configuration for future AI features.
    GEMINI_CLIENT_SETTINGS: Dict[str, Any] = {
        "client_options": {},
    }

    # Compliance engine configuration
    GST_PROVIDER = os.getenv("GST_PROVIDER", "none").lower()
    GST_API_BASE_URL = os.getenv("GST_API_BASE_URL", "")
    GST_API_KEY = os.getenv("GST_API_KEY", "")
    GST_API_SECRET = os.getenv("GST_API_SECRET", "")
    GST_CACHE_TTL_SECS = int(os.getenv("GST_CACHE_TTL_SECS", "86400"))
    GST_TEST_MODE_ENABLED = _bool(os.getenv("GST_TEST_MODE_ENABLED"), default=True)
    GST_TEST_FIXTURE_PATH = os.getenv(
        "GST_TEST_FIXTURE_PATH",
        str(Path(__file__).resolve().parent / "expenseai_compliance" / "gst_test_numbers.json"),
    )

    HSN_RATES_SOURCE = os.getenv("HSN_RATES_SOURCE", str(Path("instance") / "hsn_rates.csv"))

    ARITH_ROUNDING_MODE = os.getenv("ARITH_ROUNDING_MODE", "ROUND_HALF_UP")
    ARITH_DECIMAL_PLACES = int(os.getenv("ARITH_DECIMAL_PLACES", "2"))
    ARITH_EPSILON = float(os.getenv("ARITH_EPSILON", "0.01"))

    AUTO_COMPLIANCE_ON_PARSE = _bool(os.getenv("AUTO_COMPLIANCE_ON_PARSE"), default=True)

    BENCH_LOOKBACK_DAYS = int(os.getenv("BENCH_LOOKBACK_DAYS", "365"))
    OUTLIER_EPSILON = float(os.getenv("OUTLIER_EPSILON", "0.01"))
    MARKET_PRICE_MAX_ITEMS = int(os.getenv("MARKET_PRICE_MAX_ITEMS", "5"))
    MARKET_PRICE_DEBUG = _bool(os.getenv("MARKET_PRICE_DEBUG"), default=False)
    RISK_WATERFALL_MAX_CONTRIBS = int(os.getenv("RISK_WATERFALL_MAX_CONTRIBS", "8"))
    FINGERPRINT_LOOKBACK_DAYS = int(os.getenv("FINGERPRINT_LOOKBACK_DAYS", "365"))
    FINGERPRINT_MIN_LINES = int(os.getenv("FINGERPRINT_MIN_LINES", "30"))
    FINGERPRINT_DRIFT_THRESH = float(os.getenv("FINGERPRINT_DRIFT_THRESH", "0.25"))

    _risk_weights_default = {
        "market_outlier": 0.40,
        "arithmetic": 0.20,
        "hsn_rate": 0.20,
        "gst_vendor": 0.10,
        "gst_company": 0.05,
        "duplicate": 0.05,
    }
    _risk_weights_env = os.getenv("RISK_WEIGHTS")
    if _risk_weights_env:
        try:
            RISK_WEIGHTS = json.loads(_risk_weights_env)
        except json.JSONDecodeError:
            RISK_WEIGHTS = _risk_weights_default
    else:
        RISK_WEIGHTS = _risk_weights_default

    BANDIT_ENABLED = _bool(os.getenv("BANDIT_ENABLED"), default=True)
    BANDIT_ALPHA = float(os.getenv("BANDIT_ALPHA", "1.0"))
    BANDIT_MIN_BATCH = int(os.getenv("BANDIT_MIN_BATCH", "20"))
    BANDIT_UPDATE_INTERVAL_MINS = int(os.getenv("BANDIT_UPDATE_INTERVAL_MINS", "30"))
    BANDIT_CONTEXT_VERSION = os.getenv("BANDIT_CONTEXT_VERSION", "v1")

    ORG_FREE_USER_LIMIT = int(os.getenv("ORG_FREE_USER_LIMIT", "5"))
    ORG_PRICE_PER_ADDITIONAL_USER = os.getenv("ORG_PRICE_PER_ADDITIONAL_USER", "199")
    ORG_PRICE_CURRENCY = os.getenv("ORG_PRICE_CURRENCY", "INR")

    RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "rzp_test_kXRfpPBzUjfJgV")
    RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "7R38rICxIYrIUzYCQ2iGHsuw")
    RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")
    MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT = int(os.getenv("MAIL_PORT", 587))
    MAIL_USE_TLS = os.getenv("MAIL_USE_TLS", "true").lower() == "true"
    MAIL_USERNAME = "multimosaic.help@gmail.com"
    MAIL_PASSWORD = "eurr xxsx brxz anrz"
    MAIL_USE_SSL = os.getenv("MAIL_USE_SSL", "false").lower() == "true"
    MAIL_SUPPRESS_SEND = os.getenv("MAIL_SUPPRESS_SEND", "false").lower() == "true"

    SMTP_HOST = os.getenv("SMTP_HOST", MAIL_SERVER)
    SMTP_PORT = int(os.getenv("SMTP_PORT", str(MAIL_PORT)))
    SMTP_USER = os.getenv("SMTP_USER", MAIL_USERNAME)
    SMTP_PASS = os.getenv("SMTP_PASS", MAIL_PASSWORD)
    SMTP_USE_TLS = _bool(os.getenv("SMTP_USE_TLS"), default=MAIL_USE_TLS)
    SMTP_USE_SSL = _bool(os.getenv("SMTP_USE_SSL"), default=MAIL_USE_SSL)
    EMAIL_FROM = os.getenv("EMAIL_FROM", f"multimosaic.help@gmail.com")
    ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", EMAIL_FROM)

    OTP_EXPIRY_MINUTES = int(os.getenv("OTP_EXPIRY_MINUTES", "10"))
    OTP_MAX_ATTEMPTS = int(os.getenv("OTP_MAX_ATTEMPTS", "5"))
    RESEND_THROTTLE_SECONDS = int(os.getenv("RESEND_THROTTLE_SECONDS", "60"))

class DevConfig(BaseConfig):
    """Development defaults with verbose logging and auto reload."""

    DEBUG = True
    ENV = "development"
    TEMPLATES_AUTO_RELOAD = True
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")


class ProdConfig(BaseConfig):
    """Production defaults focused on security and performance."""

    DEBUG = False
    ENV = "production"
    SESSION_COOKIE_SECURE = True
    REMEMBER_COOKIE_SECURE = True
    PREFERRED_URL_SCHEME = "https"
    RATELIMIT_DEFAULT = os.getenv("RATE_LIMIT_DEFAULT", "60 per minute")
    CACHE_TYPE = os.getenv("CACHE_TYPE", "SimpleCache")
