"""Routes and helpers for the AI chat workspace."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import filetype
import requests
from flask import Response, abort, current_app, jsonify, render_template, request
from flask_login import current_user, login_required
from twilio.base.exceptions import TwilioRestException
from twilio.request_validator import RequestValidator
from twilio.rest import Client
from werkzeug.utils import secure_filename

from expenseai_ai import chat_service
from expenseai_ext.db import db
from expenseai_ext.security import csrf
from expenseai_models.chat import AiChatMessage, AiChatSession, ContextualChatMessage, ContextualChatSession
from expenseai_models.user import User
from expenseai_models.whatsapp_contact import WhatsAppContact
from expenseai_models.whatsapp_message_log import WhatsAppMessageLog
from expenseai_models.whatsapp_subscription import WhatsAppSubscription
from expenseai_web import web_bp


def _chat_storage_root() -> Path:
    return Path(current_app.instance_path) / current_app.config["CHAT_UPLOAD_DIR"]


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _session_to_dict(session: AiChatSession) -> dict[str, object]:
    return {
        "id": session.id,
        "title": session.title,
        "created_at": session.created_at.isoformat() + "Z",
        "updated_at": session.updated_at.isoformat() + "Z",
        "has_file": bool(session.file_path),
        "file_name": session.file_name,
    }


def _messages_to_dict(messages: Iterable[AiChatMessage]) -> list[dict[str, object]]:
    return [message.as_dict() for message in messages]


def _get_session_or_404(session_id: int) -> AiChatSession:
    session = (
        AiChatSession.query.filter(
            AiChatSession.id == session_id,
            AiChatSession.user_id == current_user.id,
        )
        .limit(1)
        .first()
    )
    if session is None:
        abort(404)
    return session


def _get_context_session_or_404(session_id: int) -> ContextualChatSession:
    session = (
        ContextualChatSession.query.filter(
            ContextualChatSession.id == session_id,
            ContextualChatSession.user_id == current_user.id,
        )
        .limit(1)
        .first()
    )
    if session is None:
        abort(404)
    return session


def _context_session_to_dict(session: ContextualChatSession) -> dict[str, object]:
    payload = session.as_dict()
    preview = (session.seed_context or "").strip()
    if preview:
        payload["seed_context_preview"] = preview[:400]
    else:
        payload["seed_context_preview"] = ""
    payload["source_count"] = len(payload.get("source_session_ids", []))
    return payload


def _context_messages_to_dict(messages: Iterable[ContextualChatMessage]) -> list[dict[str, object]]:
    return [message.as_dict() for message in messages]


def _context_history_limit() -> int:
    limit = current_app.config.get("CONTEXT_CHAT_HISTORY_LIMIT", 5)
    try:
        limit_int = int(limit)
    except (TypeError, ValueError):
        limit_int = 5
    return max(limit_int, 1)


def _collect_invoice_context_sources(user_id: int) -> list[dict[str, object]]:
    sessions = (
        AiChatSession.query.filter(AiChatSession.user_id == user_id, AiChatSession.file_path.isnot(None))
        .order_by(AiChatSession.updated_at.desc())
        .all()
    )
    items: list[dict[str, object]] = []
    for session in sessions:
        first_response = (
            AiChatMessage.query.filter(
                AiChatMessage.session_id == session.id,
                AiChatMessage.role == "assistant",
            )
            .order_by(AiChatMessage.created_at.asc())
            .limit(1)
            .first()
        )
        if first_response is None:
            continue
        content = (first_response.content or "").strip()
        items.append(
            {
                "id": session.id,
                "title": session.title,
                "created_at": session.created_at.isoformat() + "Z",
                "updated_at": session.updated_at.isoformat() + "Z",
                "file_name": session.file_name,
                "model_name": session.model_name,
                "first_ai_message": content,
                "preview": content[:280],
            }
        )
    return items


def _build_combined_context(session_ids: Sequence[int]) -> tuple[str, list[AiChatSession]]:
    if not session_ids:
        return "", []
    unique_ids: list[int] = []
    for raw in session_ids:
        try:
            value = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if value <= 0 or value in unique_ids:
            continue
        unique_ids.append(value)
    if not unique_ids:
        return "", []

    sessions = (
        AiChatSession.query.filter(
            AiChatSession.user_id == current_user.id,
            AiChatSession.id.in_(unique_ids),
        )
        .all()
    )
    if len(sessions) != len(unique_ids):
        missing = set(unique_ids) - {session.id for session in sessions}
        raise ValueError(f"Unknown invoice chat session(s): {sorted(missing)}")

    session_map = {session.id: session for session in sessions}
    blocks: list[str] = []
    resolved_sessions: list[AiChatSession] = []
    for identifier in unique_ids:
        session = session_map.get(identifier)
        if session is None:
            continue
        first_response = (
            AiChatMessage.query.filter(
                AiChatMessage.session_id == session.id,
                AiChatMessage.role == "assistant",
            )
            .order_by(AiChatMessage.created_at.asc())
            .limit(1)
            .first()
        )
        if first_response is None or not (first_response.content or "").strip():
            raise ValueError(f"Invoice chat {session.id} has no AI responses to use as context.")
        title = session.title or f"Invoice chat #{session.id}"
        content = (first_response.content or "").strip()
        blocks.append(f"AI response from {title}:\n{content}")
        resolved_sessions.append(session)

    combined = "\n\n".join(blocks).strip()
    if not combined:
        raise ValueError("No context available from the selected chats.")
    return combined, resolved_sessions


_WHATSAPP_MEDIA_EXTENSIONS = {
    "application/pdf": ".pdf",
    "image/png": ".png",
    "image/jpeg": ".jpg",
}


def _twilio_client() -> Client | None:
    sid = current_app.config.get("TWILIO_ACCOUNT_SID", "")
    token = current_app.config.get("TWILIO_AUTH_TOKEN", "")
    if not sid or not token:
        return None
    return Client(sid, token)


def _twilio_from_address() -> str | None:
    number = (current_app.config.get("TWILIO_WHATSAPP_NUMBER") or "").strip()
    if not number:
        return None
    if not number.startswith("whatsapp:"):
        number = f"whatsapp:{number}"
    return number


def _normalize_whatsapp_number(address: str | None) -> str:
    value = (address or "").strip()
    if not value:
        return ""
    if value.startswith("whatsapp:"):
        value = value[len("whatsapp:") :]
    return value


def _ensure_whatsapp_contact(phone: str, display_name: str | None) -> WhatsAppContact:
    contact = (
        WhatsAppContact.query.filter(WhatsAppContact.phone_e164 == phone)
        .limit(1)
        .first()
    )
    if contact:
        if display_name and display_name != contact.display_name:
            contact.display_name = display_name[:128]
        return contact

    contact = WhatsAppContact(
        phone_e164=phone,
        display_name=display_name[:128] if display_name else None,
    )
    db.session.add(contact)
    db.session.flush()
    return contact


def _auto_link_contact(contact: WhatsAppContact) -> bool:
    if contact.user_id is not None:
        return True

    subscription = (
        WhatsAppSubscription.query.filter(WhatsAppSubscription.phone_e164 == contact.phone_e164)
        .order_by(
            (WhatsAppSubscription.confirmed_at.isnot(None)).desc(),
            WhatsAppSubscription.confirmed_at.desc(),
            WhatsAppSubscription.created_at.desc(),
        )
        .limit(1)
        .first()
    )
    if not subscription:
        if not current_app.config.get("WHATSAPP_AUTOCREATE_USERS", True):
            return False

        digits = "".join(ch for ch in contact.phone_e164 if ch.isdigit())
        if not digits:
            digits = secrets.token_hex(3)
        domain = (current_app.config.get("WHATSAPP_AUTOCREATE_EMAIL_DOMAIN") or "auto.expenseai").strip()
        if not domain:
            domain = "auto.expenseai"
        local_part = f"whatsapp+{digits}"
        email = f"{local_part}@{domain}"

        existing = User.query.filter(User.email == email).limit(1).first()
        while existing is not None:
            suffix = secrets.token_hex(1)
            email = f"{local_part}{suffix}@{domain}"
            existing = User.query.filter(User.email == email).limit(1).first()

        display = digits[-4:] if len(digits) >= 4 else digits
        full_name = f"WhatsApp {display}" if display else "WhatsApp User"
        user = User(email=email, full_name=full_name)
        user.set_password(secrets.token_urlsafe(12))
        user.email_verified_at = datetime.utcnow()
        db.session.add(user)
        db.session.flush()

        contact.user_id = user.id
        contact.updated_at = datetime.utcnow()
        db.session.add(contact)
        db.session.flush()
        current_app.logger.info(
            "Auto-created WhatsApp user for contact",
            extra={"contact_id": contact.id, "user_id": user.id},
        )
        return True

    if subscription.confirmed_at is None and not subscription.is_active():
        return False

    if subscription.confirmed_at is None:
        subscription.mark_confirmed()
        db.session.add(subscription)

    contact.user_id = subscription.user_id
    contact.updated_at = datetime.utcnow()
    db.session.add(contact)
    db.session.flush()
    return True


def _validate_twilio_signature() -> None:
    if not current_app.config.get("TWILIO_VALIDATE_SIGNATURE", True):
        return
    token = current_app.config.get("TWILIO_AUTH_TOKEN", "")
    if not token:
        current_app.logger.warning("Twilio signature validation skipped: missing auth token")
        return
    signature = request.headers.get("X-Twilio-Signature")
    if not signature:
        current_app.logger.warning(
            "Twilio signature validation failed: header missing",
            extra={
                "headers": {k: v for k, v in request.headers.items()},
                "form": request.form.to_dict(flat=True),
            },
        )
        abort(403)
    validator = RequestValidator(token)
    url = request.url
    forwarded_host = request.headers.get("X-Forwarded-Host")
    forwarded_proto = request.headers.get("X-Forwarded-Proto")
    forwarded_port = request.headers.get("X-Forwarded-Port")
    if forwarded_host:
        host = forwarded_host.split(",")[0].strip()
        proto = (forwarded_proto or request.scheme or "https").split(",")[0].strip()
        path = request.full_path or request.path
        if path.endswith("?"):
            path = path[:-1]
        port = (forwarded_port or "").split(",")[0].strip()
        if port and port not in {"80", "443"} and ":" not in host:
            host = f"{host}:{port}"
        url = f"{proto}://{host}{path}"
    elif forwarded_proto:
        if url.startswith("http://") and forwarded_proto.startswith("https"):
            url = "https://" + url[len("http://") :]
        elif url.startswith("https://") and forwarded_proto.startswith("http"):
            url = "http://" + url[len("https://") :]
    original = request.headers.get("X-Twilio-Webhook-Url")
    if original:
        url = original.strip()
    form_dict = request.form.to_dict(flat=True)
    result = validator.validate(url, request.form, signature)
    if not result:
        expected = validator.compute_signature(url, form_dict)
        header_debug = {
            "X-Forwarded-Proto": request.headers.get("X-Forwarded-Proto"),
            "X-Forwarded-Host": request.headers.get("X-Forwarded-Host"),
            "X-Forwarded-Port": request.headers.get("X-Forwarded-Port"),
            "X-Twilio-Webhook-Url": request.headers.get("X-Twilio-Webhook-Url"),
        }
        current_app.logger.warning(
            "Twilio signature validation failed url=%s provided=%s expected=%s headers=%s form_keys=%s",
            url,
            signature,
            expected,
            header_debug,
            sorted(form_dict.keys()),
        )
        abort(403)
    current_app.logger.debug(
        "Twilio signature validation succeeded url=%s form_keys=%s",
        url,
        sorted(form_dict.keys()),
    )


def _whatsapp_session_ttl() -> timedelta:
    hours = current_app.config.get("WHATSAPP_SESSION_STALE_HOURS", 48)
    try:
        hours_int = int(hours)
    except (TypeError, ValueError):
        hours_int = 48
    return timedelta(hours=max(hours_int, 1))


def _resolve_recent_session(user_id: int, *, require_file: bool = True) -> AiChatSession | None:
    query = AiChatSession.query.filter(AiChatSession.user_id == user_id)
    if require_file:
        query = query.filter(AiChatSession.file_path.isnot(None))
    session = query.order_by(AiChatSession.updated_at.desc()).limit(1).first()
    if not session:
        return None
    stale_before = datetime.utcnow() - _whatsapp_session_ttl()
    if session.updated_at and session.updated_at < stale_before:
        return None
    return session


def _allowed_extension(extension: str) -> bool:
    allowed = {item.lower() for item in current_app.config.get("CHAT_ALLOWED_EXTENSIONS", set())}
    return extension.lower().lstrip(".") in allowed if allowed else True


def _allowed_mime(mime: str) -> bool:
    allowed = current_app.config.get("CHAT_ALLOWED_MIME_TYPES", set())
    if not allowed:
        return True
    return mime in allowed


def _extension_for_mime(mime: str) -> str | None:
    if mime in _WHATSAPP_MEDIA_EXTENSIONS:
        return _WHATSAPP_MEDIA_EXTENSIONS[mime]
    return None


def _download_twilio_media(url: str) -> tuple[bytes, str]:
    sid = current_app.config.get("TWILIO_ACCOUNT_SID", "")
    token = current_app.config.get("TWILIO_AUTH_TOKEN", "")
    if not sid or not token:
        raise RuntimeError("Twilio credentials are missing; cannot download media")
    response = requests.get(url, auth=(sid, token), timeout=30)
    response.raise_for_status()
    return response.content, response.headers.get("Content-Type", "")


def _max_upload_bytes() -> int:
    limit_mb = current_app.config.get("MAX_UPLOAD_MB", 10)
    try:
        limit_int = int(limit_mb)
    except (TypeError, ValueError):
        limit_int = 10
    return limit_int * 1024 * 1024


def _store_whatsapp_media(session: AiChatSession, filename: str, data: bytes, mime: str) -> Path:
    root = _chat_storage_root() / str(session.user_id)
    session_dir = root / str(session.id)
    _ensure_dir(session_dir)
    safe_name = secure_filename(filename) or f"invoice{Path(filename).suffix or '.dat'}"
    stored_name = (
        f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}{Path(safe_name).suffix.lower()}"
    )
    file_path = session_dir / stored_name
    file_path.write_bytes(data)
    session.file_name = safe_name[:255]
    session.file_path = str(file_path)
    session.file_mime_type = mime
    session.file_size_bytes = len(data)
    session.updated_at = datetime.utcnow()
    stem = Path(safe_name).stem.strip()
    if stem:
        session.title = stem[:250]
    session.ensure_title()
    return file_path


def _log_whatsapp_inbound(contact: WhatsAppContact, payload: dict[str, str]) -> None:
    db.session.add(
        WhatsAppMessageLog(
            contact=contact,
            direction="in",
            msg_type="text",
            status="delivered",
            payload_json=payload,
        )
    )


def _handle_whatsapp_status_callback(payload: dict[str, str]) -> bool:
    message_sid = payload.get("MessageSid") or payload.get("SmsSid")
    status = payload.get("MessageStatus") or payload.get("SmsStatus")
    if not message_sid or not status or status == "received":
        return False
    log = (
        WhatsAppMessageLog.query.filter(WhatsAppMessageLog.whatsapp_msg_id == message_sid)
        .limit(1)
        .first()
    )
    if not log:
        return False
    details = dict(log.payload_json or {})
    details["status_payload"] = payload
    log.payload_json = details
    log.update_status(status)
    return True


def _send_whatsapp_text(contact: WhatsAppContact, body: str, *, session_id: int | None = None) -> None:
    client = _twilio_client()
    sender = _twilio_from_address()
    if client is None or sender is None:
        raise RuntimeError("Twilio WhatsApp configuration is incomplete")

    # Twilio limits concatenated messages; keep individual parts under 1500 chars
    max_len = 1500
    parts = [body[i : i + max_len] for i in range(0, len(body), max_len)] if body else [""]

    for idx, part in enumerate(parts, start=1):
        payload = {"body": part}
        if session_id is not None:
            payload["session_id"] = session_id
        # annotate part index when we split the message
        if len(parts) > 1:
            payload["part"] = idx
            payload["parts_total"] = len(parts)

        log = WhatsAppMessageLog(contact=contact, direction="out", msg_type="text", payload_json=payload)
        db.session.add(log)
        db.session.flush()

        try:
            message = client.messages.create(
                body=part,
                from_=sender,
                to=f"whatsapp:{contact.phone_e164}",
            )
        except TwilioRestException as exc:
            try:
                log.update_status("failed", error_code=str(exc.code), error_message=str(exc.msg))
            except Exception:
                current_app.logger.exception("Failed to mark WhatsApp log as failed")
            raise
        else:
            log.whatsapp_msg_id = message.sid
            log.update_status("sent")


def _handle_whatsapp_file(contact: WhatsAppContact, payload: dict[str, str]) -> None:
    if contact.user_id is None:
        _send_whatsapp_text(contact, "Please link your WhatsApp number with Finvela before sending invoices.")
        return

    media_url = payload.get("MediaUrl0")
    if not media_url:
        _send_whatsapp_text(contact, "We did not receive the attachment. Please resend the invoice.")
        return

    media_content_type = payload.get("MediaContentType0") or ""
    try:
        data, remote_mime = _download_twilio_media(media_url)
    except Exception:
        current_app.logger.exception("Failed to download WhatsApp media", extra={"media_url": media_url})
        _send_whatsapp_text(contact, "We could not download your attachment. Please try again.")
        return

    kind = filetype.guess(data)
    mime = kind.mime if kind else media_content_type or remote_mime or "application/octet-stream"
    if not _allowed_mime(mime):
        _send_whatsapp_text(contact, "Unsupported file type. Send a PDF or clear image of the invoice.")
        return

    filename = payload.get("MediaFilename0") or "invoice"
    if "." not in filename:
        ext = _extension_for_mime(mime)
        if not ext or not _allowed_extension(ext):
            _send_whatsapp_text(contact, "Unsupported file extension. Send a PDF or image of the invoice.")
            return
        filename = f"{filename}{ext}"
    else:
        ext = Path(filename).suffix
        if not _allowed_extension(ext):
            _send_whatsapp_text(contact, "Unsupported file extension. Send a PDF or image of the invoice.")
            return

    if len(data) == 0:
        _send_whatsapp_text(contact, "The received file was empty. Please resend the invoice.")
        return
    if len(data) > _max_upload_bytes():
        _send_whatsapp_text(contact, "The file is too large. Please send a smaller invoice document.")
        return

    session = AiChatSession(
        user_id=contact.user_id,
        title="WhatsApp chat",
        model_name=current_app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
    )
    db.session.add(session)
    db.session.flush()

    try:
        file_path = _store_whatsapp_media(session, filename, data, mime)
    except Exception:
        current_app.logger.exception("Failed to persist WhatsApp upload", extra={"session_id": session.id})
        db.session.delete(session)
        db.session.flush()
        _send_whatsapp_text(contact, "We could not store the file. Please try again.")
        return

    user_message = AiChatMessage(
        session=session,
        role="user",
        content=f"{chat_service.ANALYSIS_REQUEST_MESSAGE}\n\nSource file: {session.file_name}",
    )
    db.session.add(user_message)

    try:
        assistant_text = chat_service.run_file_analysis(
            file_path=str(file_path),
            mime_type=session.file_mime_type or mime,
            model_name=session.model_name,
            channel="whatsapp",
        )
    except Exception:
        current_app.logger.exception("Finvela analysis failed for WhatsApp upload", extra={"session_id": session.id})
        try:
            file_path.unlink(missing_ok=True)
        except Exception:
            current_app.logger.warning(
                "Failed to delete WhatsApp upload after analysis error",
                extra={"path": str(file_path)},
            )
        db.session.delete(session)
        db.session.flush()
        _send_whatsapp_text(contact, "We could not analyze the invoice right now. Please try again later.")
        return

    assistant_message = AiChatMessage(session=session, role="assistant", content=assistant_text)
    db.session.add(assistant_message)
    session.updated_at = datetime.utcnow()
    _send_whatsapp_text(contact, assistant_text, session_id=session.id)


def _handle_whatsapp_text(contact: WhatsAppContact, payload: dict[str, str]) -> None:
    message_body = (payload.get("Body") or "").strip()
    if not message_body:
        _send_whatsapp_text(contact, "Please send a message or attach an invoice to continue.")
        return

    if contact.user_id is None:
        _send_whatsapp_text(contact, "Please link your WhatsApp number with Finvela before chatting.")
        return

    session = _resolve_recent_session(contact.user_id, require_file=True)
    if session is None or not session.file_path:
        _send_whatsapp_text(contact, "Send an invoice document first so we can analyze it.")
        return

    message = AiChatMessage(session=session, role="user", content=message_body)
    db.session.add(message)
    db.session.flush()

    limit = current_app.config.get("CHAT_HISTORY_LIMIT", 5)
    recent = (
        AiChatMessage.query.filter(AiChatMessage.session_id == session.id)
        .order_by(AiChatMessage.created_at.desc())
        .limit(limit)
        .all()
    )
    history = list(reversed(recent))
    history_payload = [{"role": item.role, "content": item.content} for item in history]

    try:
        assistant_text = chat_service.continue_chat(
            user_message=message_body,
            history=history_payload[:-1],
            model_name=session.model_name,
            channel="whatsapp",
        )
    except Exception:
        current_app.logger.exception("Finvela chat continuation failed for WhatsApp", extra={"session_id": session.id})
        db.session.delete(message)
        db.session.flush()
        _send_whatsapp_text(contact, "We ran into a problem responding. Please try again in a moment.")
        return

    assistant_message = AiChatMessage(session=session, role="assistant", content=assistant_text)
    db.session.add(assistant_message)
    session.updated_at = datetime.utcnow()
    _send_whatsapp_text(contact, assistant_text, session_id=session.id)


@web_bp.route("/ai-chat", methods=["GET"])
@login_required
def ai_chat_home() -> Response:
    sessions = (
        AiChatSession.query.filter(AiChatSession.user_id == current_user.id)
        .order_by(AiChatSession.updated_at.desc())
        .all()
    )
    if not sessions:
        session = AiChatSession(
            user_id=current_user.id,
            title="Untitled chat",
            model_name=current_app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
        )
        db.session.add(session)
        db.session.commit()
        sessions = [session]

    requested_id = request.args.get("session_id", type=int)
    active_session = None
    if requested_id is not None:
        active_session = next((s for s in sessions if s.id == requested_id), None)
    if active_session is None:
        active_session = sessions[0]

    messages = _messages_to_dict(active_session.messages) if active_session else []
    template_sessions = [_session_to_dict(item) for item in sessions]

    return render_template(
        "ai_chat/index.html",
        sessions=template_sessions,
        active_session=_session_to_dict(active_session) if active_session else None,
        active_messages=messages,
        history_limit=current_app.config.get("CHAT_HISTORY_LIMIT", 5),
    )


@web_bp.route("/ai-chat/sessions", methods=["GET"])
@login_required
def list_ai_chat_sessions() -> Response:
    sessions = (
        AiChatSession.query.filter(AiChatSession.user_id == current_user.id)
        .order_by(AiChatSession.updated_at.desc())
        .all()
    )
    return jsonify({"sessions": [_session_to_dict(item) for item in sessions]})


@web_bp.route("/ai-chat/sessions", methods=["POST"])
@login_required
def create_ai_chat_session() -> Response:
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "Untitled chat").strip()
    if not title:
        title = "Untitled chat"
    session = AiChatSession(
        user_id=current_user.id,
        title=title,
        model_name=current_app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
    )
    db.session.add(session)
    db.session.commit()
    return jsonify({"session": _session_to_dict(session)}), 201


@web_bp.route("/ai-chat/sessions/<int:session_id>/messages", methods=["GET"])
@login_required
def fetch_ai_chat_messages(session_id: int) -> Response:
    session = _get_session_or_404(session_id)
    messages = (
        AiChatMessage.query.filter(AiChatMessage.session_id == session.id)
        .order_by(AiChatMessage.created_at.asc())
        .all()
    )
    return jsonify({"messages": _messages_to_dict(messages)})


@web_bp.route("/ai-chat/sessions/<int:session_id>/upload", methods=["POST"])
@login_required
def upload_ai_chat_file(session_id: int) -> Response:
    session = _get_session_or_404(session_id)
    if session.file_path:
        return jsonify({"error": "A file has already been uploaded for this chat."}), 400

    uploaded = request.files.get("file")
    if uploaded is None:
        return jsonify({"error": "No file provided."}), 400

    filename = secure_filename(uploaded.filename or "invoice")
    if not filename:
        return jsonify({"error": "Invalid filename."}), 400

    allowed_ext = current_app.config.get("CHAT_ALLOWED_EXTENSIONS", {"pdf", "png", "jpg", "jpeg"})
    suffix = Path(filename).suffix.lower().lstrip(".")
    if suffix not in allowed_ext:
        return jsonify({"error": "Unsupported file extension."}), 400

    uploaded.stream.seek(0)
    data = uploaded.read()
    if not data:
        return jsonify({"error": "File is empty."}), 400

    kind = filetype.guess(data)
    mime = kind.mime if kind else uploaded.mimetype or "application/octet-stream"
    allowed_mimes = current_app.config.get("CHAT_ALLOWED_MIME_TYPES", set())
    if allowed_mimes and mime not in allowed_mimes:
        return jsonify({"error": "Unsupported file type."}), 400

    root = _chat_storage_root() / str(current_user.id)
    session_dir = root / str(session.id)
    _ensure_dir(session_dir)
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}{Path(filename).suffix.lower()}"
    file_path = session_dir / stored_name
    file_path.write_bytes(data)

    session.file_name = filename
    session.file_path = str(file_path)
    session.file_size_bytes = len(data)
    session.file_mime_type = mime
    session.updated_at = datetime.utcnow()
    stem = Path(filename).stem.strip()
    if stem:
        session.title = stem[:250]
    session.ensure_title()

    user_message = AiChatMessage(
        session=session,
        role="user",
        content=f"{chat_service.ANALYSIS_REQUEST_MESSAGE}\n\nSource file: {filename}",
    )
    db.session.add(user_message)
    db.session.flush()

    try:
        assistant_text = chat_service.run_file_analysis(
            file_path=str(file_path),
            mime_type=mime,
            model_name=session.model_name,
        )
    except Exception as exc:  # pragma: no cover - runtime interaction
        current_app.logger.exception("Finvela analysis failed", extra={"session_id": session.id})
        try:
            file_path.unlink(missing_ok=True)
        except Exception:  # pragma: no cover - best effort cleanup
            current_app.logger.warning("Failed to remove uploaded chat file after Finvela error", extra={"path": str(file_path)})
        db.session.rollback()
        return jsonify({"error": str(exc)}), 502

    assistant_message = AiChatMessage(session=session, role="assistant", content=assistant_text)
    db.session.add(assistant_message)
    session.updated_at = datetime.utcnow()

    db.session.commit()
    return jsonify(
        {
            "session": _session_to_dict(session),
            "messages": _messages_to_dict([user_message, assistant_message]),
        }
    ), 201


@web_bp.route("/ai-chat/sessions/<int:session_id>/messages", methods=["POST"])
@login_required
def send_ai_chat_message(session_id: int) -> Response:
    session = _get_session_or_404(session_id)
    if not session.file_path:
        return jsonify({"error": "Please upload an invoice before chatting."}), 400
    payload = request.get_json(force=True)
    text = (payload.get("message") or "").strip()
    if not text:
        return jsonify({"error": "Message cannot be empty."}), 400

    message = AiChatMessage(session=session, role="user", content=text)
    db.session.add(message)
    db.session.flush()

    limit = current_app.config.get("CHAT_HISTORY_LIMIT", 5)
    recent = (
        AiChatMessage.query.filter(AiChatMessage.session_id == session.id)
        .order_by(AiChatMessage.created_at.desc())
        .limit(limit)
        .all()
    )
    history = list(reversed(recent))
    history_payload = [{"role": item.role, "content": item.content} for item in history]

    try:
        assistant_text = chat_service.continue_chat(
            user_message=text,
            history=history_payload[:-1],
            model_name=session.model_name,
        )
    except Exception as exc:  # pragma: no cover - runtime interaction
        current_app.logger.exception("Finvela chat failed", extra={"session_id": session.id})
        db.session.rollback()
        return jsonify({"error": str(exc)}), 502

    assistant_message = AiChatMessage(session=session, role="assistant", content=assistant_text)
    db.session.add(assistant_message)
    session.updated_at = datetime.utcnow()

    db.session.commit()
    return jsonify(
        {
            "messages": _messages_to_dict([message, assistant_message]),
            "session": _session_to_dict(session),
        }
    )


@web_bp.route("/ai-chat/contextual", methods=["GET"])
@login_required
def contextual_chat_home() -> Response:
    invoice_sources = _collect_invoice_context_sources(current_user.id)
    sessions = (
        ContextualChatSession.query.filter(ContextualChatSession.user_id == current_user.id)
        .order_by(ContextualChatSession.updated_at.desc())
        .all()
    )

    requested_id = request.args.get("session_id", type=int)
    active_session = None
    if requested_id is not None:
        active_session = next((item for item in sessions if item.id == requested_id), None)
    if active_session is None and sessions:
        active_session = sessions[0]

    active_messages = _context_messages_to_dict(active_session.messages) if active_session else []

    return render_template(
        "contextual_chat/index.html",
        invoice_sources=invoice_sources,
        sessions=[_context_session_to_dict(item) for item in sessions],
        active_session=_context_session_to_dict(active_session) if active_session else None,
        active_messages=active_messages,
        history_limit=_context_history_limit(),
    )


@web_bp.route("/ai-chat/contextual/invoice-sources", methods=["GET"])
@login_required
def list_contextual_invoice_sources() -> Response:
    sources = _collect_invoice_context_sources(current_user.id)
    return jsonify({"sources": sources})


@web_bp.route("/ai-chat/contextual/sessions", methods=["GET"])
@login_required
def list_contextual_chat_sessions() -> Response:
    sessions = (
        ContextualChatSession.query.filter(ContextualChatSession.user_id == current_user.id)
        .order_by(ContextualChatSession.updated_at.desc())
        .all()
    )
    return jsonify({"sessions": [_context_session_to_dict(item) for item in sessions]})


@web_bp.route("/ai-chat/contextual/sessions", methods=["POST"])
@login_required
def create_contextual_chat_session() -> Response:
    payload = request.get_json(silent=True) or {}
    source_ids_raw = payload.get("source_session_ids")
    if not isinstance(source_ids_raw, (list, tuple, set)):
        return jsonify({"error": "Select at least one invoice chat to seed the session."}), 400

    try:
        combined_context, resolved_sessions = _build_combined_context(source_ids_raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    if not resolved_sessions:
        return jsonify({"error": "Select at least one invoice chat to seed the session."}), 400

    provided_title = (payload.get("title") or "").strip()
    default_title = resolved_sessions[0].title if resolved_sessions else "Context chat"
    title = provided_title or f"{default_title} · Context"
    title = title[:255]

    session = ContextualChatSession(
        user_id=current_user.id,
        title=title,
        model_name=current_app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
        seed_context=combined_context,
        source_session_ids=[item.id for item in resolved_sessions],
        is_initialized=False,
    )
    db.session.add(session)
    db.session.commit()

    return jsonify({"session": _context_session_to_dict(session)}), 201


@web_bp.route("/ai-chat/contextual/sessions/<int:session_id>/messages", methods=["GET"])
@login_required
def fetch_contextual_chat_messages(session_id: int) -> Response:
    session = _get_context_session_or_404(session_id)
    messages = (
        ContextualChatMessage.query.filter(ContextualChatMessage.session_id == session.id)
        .order_by(ContextualChatMessage.created_at.asc())
        .all()
    )
    return jsonify({"messages": _context_messages_to_dict(messages)})


@web_bp.route("/ai-chat/contextual/sessions/<int:session_id>/messages", methods=["POST"])
@login_required
def send_contextual_chat_message(session_id: int) -> Response:
    session = _get_context_session_or_404(session_id)
    payload = request.get_json(force=True)
    text = (payload.get("message") or "").strip()
    if not text:
        return jsonify({"error": "Message cannot be empty."}), 400

    history_limit = _context_history_limit()
    previous_messages = (
        ContextualChatMessage.query.filter(ContextualChatMessage.session_id == session.id)
        .order_by(ContextualChatMessage.created_at.desc())
        .limit(history_limit)
        .all()
    )
    history_messages = list(reversed(previous_messages))

    user_message = ContextualChatMessage(session=session, role="user", content=text)
    db.session.add(user_message)
    db.session.flush()

    history_payload = [{"role": item.role, "content": item.content} for item in history_messages]

    try:
        if not session.is_initialized:
            context_prefix = (session.seed_context or "").strip()
            if context_prefix:
                prompt = f"{context_prefix}\n\nUser: {text}"
            else:
                prompt = text
            assistant_text = chat_service.continue_chat(
                user_message=prompt,
                history=[],
                model_name=session.model_name,
            )
            session.is_initialized = True
        else:
            assistant_text = chat_service.continue_chat(
                user_message=text,
                history=history_payload,
                model_name=session.model_name,
            )
    except Exception as exc:
        current_app.logger.exception("Contextual chat generation failed", extra={"session_id": session.id})
        db.session.rollback()
        return jsonify({"error": str(exc)}), 502

    assistant_message = ContextualChatMessage(session=session, role="assistant", content=assistant_text)
    db.session.add(assistant_message)
    db.session.flush()

    session.updated_at = datetime.utcnow()

    messages_payload = _context_messages_to_dict([user_message, assistant_message])
    session_payload = _context_session_to_dict(session)

    db.session.commit()
    return jsonify({"messages": messages_payload, "session": session_payload})


@web_bp.route("/ai-chat/whatsapp/webhook", methods=["POST"])
@csrf.exempt
def whatsapp_chat_webhook() -> Response:
    if not current_app.config.get("FF_WHATSAPP", False):
        abort(404)

    _validate_twilio_signature()

    if _twilio_client() is None or _twilio_from_address() is None:
        current_app.logger.error("Twilio WhatsApp credentials are not configured")
        abort(503)

    payload = request.form.to_dict(flat=True)

    if _handle_whatsapp_status_callback(payload):
        db.session.commit()
        return Response(status=200)

    sender = _normalize_whatsapp_number(payload.get("From"))
    if not sender:
        wa_id = payload.get("WaId") or ""
        wa_id = wa_id.strip()
        if wa_id:
            sender = f"+{wa_id.lstrip('+')}"
    if not sender:
        current_app.logger.warning("WhatsApp webhook missing sender", extra={"payload": payload})
        return Response(status=400)

    contact = _ensure_whatsapp_contact(sender, payload.get("ProfileName"))
    _log_whatsapp_inbound(contact, payload)

    # Try to link via subscription or auto-create. If still unlinked, create a
    # minimal user account so chats can proceed even without prior linking.
    try:
        _auto_link_contact(contact)
    except Exception:
        current_app.logger.exception("Auto-link contact failed; continuing to ensure user")

    if contact.user_id is None:
        # Create a lightweight user and attach the contact so we can persist sessions
        try:
            digits = "".join(ch for ch in contact.phone_e164 if ch.isdigit())
            if not digits:
                digits = secrets.token_hex(3)
            domain = (current_app.config.get("WHATSAPP_AUTOCREATE_EMAIL_DOMAIN") or "auto.expenseai").strip()
            local_part = f"whatsapp+{digits}"
            email = f"{local_part}@{domain}"

            existing = User.query.filter(User.email == email).limit(1).first()
            while existing is not None:
                suffix = secrets.token_hex(1)
                email = f"{local_part}{suffix}@{domain}"
                existing = User.query.filter(User.email == email).limit(1).first()

            display = digits[-4:] if len(digits) >= 4 else digits
            full_name = f"WhatsApp {display}" if display else "WhatsApp User"
            user = User(email=email, full_name=full_name)
            user.set_password(secrets.token_urlsafe(12))
            user.email_verified_at = datetime.utcnow()
            db.session.add(user)
            db.session.flush()

            contact.user_id = user.id
            contact.updated_at = datetime.utcnow()
            db.session.add(contact)
            db.session.flush()
            current_app.logger.info("Auto-created WhatsApp user for contact (inline)", extra={"contact_id": contact.id, "user_id": user.id})
        except Exception:
            current_app.logger.exception("Failed to auto-create user for contact")

    # Proceed even if user creation/linking failed; downstream handlers will
    # gracefully abort if necessary.

    try:
        num_media = int(payload.get("NumMedia", "0")) if payload.get("NumMedia") else 0
    except ValueError:
        num_media = 0

    try:
        if num_media > 0 and payload.get("MediaUrl0"):
            _handle_whatsapp_file(contact, payload)
        else:
            _handle_whatsapp_text(contact, payload)
        db.session.commit()
    except TwilioRestException as exc:
        db.session.rollback()
        current_app.logger.exception(
            "Failed to send WhatsApp message", extra={"contact_id": contact.id, "error_code": exc.code}
        )
        return jsonify({"error": "Unable to send WhatsApp reply."}), 502
    except Exception:
        db.session.rollback()
        raise

    return Response(status=200)
