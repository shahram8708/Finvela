"""Idempotency utilities for POST handlers."""
from __future__ import annotations

import functools
import hashlib
from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar

from flask import Response, current_app, g, make_response, request
from flask_login import current_user
from sqlalchemy.exc import IntegrityError

from expenseai_ext.db import db
from expenseai_ext.errors import ConflictError
from expenseai_models import IdempotencyKey

F = TypeVar("F", bound=Callable[..., Any])


def idempotent(scope: str) -> Callable[[F], F]:
	"""Ensure a POST action can be retried safely within a time window."""

	def decorator(func: F) -> F:
		@functools.wraps(func)
		def wrapped(*args: Any, **kwargs: Any):
			key = _resolve_key()
			if not key:
				return func(*args, **kwargs)

			now = datetime.utcnow()
			request_hash = _hash_request()
			record = IdempotencyKey.query.filter_by(key=key).first()
			if record and record.request_hash and record.request_hash != request_hash:
				raise ConflictError(
					user_msg="Idempotency key already used with different payload.",
					safe_context={"key": key},
				)
			if record and _is_valid(record, now):
				record.used_at = now
				db.session.add(record)
				db.session.commit()
				g.idempotent_replay = True
				return _build_response(record)
			if record and record.expires_at < now:
				db.session.delete(record)
				db.session.commit()

			response = make_response(func(*args, **kwargs))
			payload = _extract_body(response)
			ttl = current_app.config.get("IDEMPOTENCY_TTL_SECS", 600)
			expires_at = now + timedelta(seconds=ttl)
			user_id = _current_user_id()

			record = IdempotencyKey(
				user_id=user_id,
				scope=scope,
				key=key,
				request_hash=request_hash,
				response_json=payload,
				status_code=response.status_code,
				created_at=now,
				expires_at=expires_at,
				used_at=now,
			)
			db.session.add(record)
			try:
				db.session.commit()
			except IntegrityError:
				db.session.rollback()
				existing = IdempotencyKey.query.filter_by(key=key).first()
				replay_now = datetime.utcnow()
				if existing and existing.request_hash and existing.request_hash != request_hash:
					raise ConflictError(
						user_msg="Idempotency key already used with different payload.",
						safe_context={"key": key},
					)
				if existing and _is_valid(existing, replay_now):
					existing.used_at = replay_now
					db.session.commit()
					g.idempotent_replay = True
					return _build_response(existing)
				raise

			response.headers.setdefault("Idempotent-Key", key)
			return response

		return wrapped  # type: ignore[return-value]

	return decorator


def _resolve_key() -> str | None:
	return request.headers.get("Idempotency-Key") or request.form.get("idempotency_key")


def _is_valid(record: IdempotencyKey, now: datetime) -> bool:
	return record.expires_at >= now and record.response_json is not None and record.status_code is not None


def _hash_request() -> str:
	body = request.get_data(cache=True) if request.method in {"POST", "PUT", "PATCH", "DELETE"} else b""
	digest = hashlib.sha256()
	digest.update(request.path.encode("utf-8"))
	digest.update(request.method.encode("utf-8"))
	digest.update(body or b"")
	return digest.hexdigest()


def _extract_body(response: Response) -> Any:
	if response.is_json:
		return response.get_json(silent=True)
	try:
		text = response.get_data(as_text=True)
		limit = current_app.config.get("REQUEST_BODY_LOG_MAX", 2048)
		if len(text) > limit:
			text = text[:limit] + "…"
		return {"body": text}
	except Exception:  # pragma: no cover - defensive
		return None


def _build_response(record: IdempotencyKey) -> Response:
	response = make_response(record.response_json or {}, record.status_code or 200)
	response.headers["Idempotent-Replay"] = "true"
	return response


def _current_user_id() -> int | None:
	if not getattr(current_user, "is_authenticated", False):
		return None
	try:
		raw = current_user.get_id()
		return int(raw) if raw is not None else None
	except (TypeError, ValueError):  # pragma: no cover - defensive
		return None

