"""Simple SMTP email helper used for transactional notifications."""
from __future__ import annotations

import smtplib
from contextlib import contextmanager
from email.message import EmailMessage
from typing import Iterable

from flask import current_app, render_template


@contextmanager
def _smtp_connection():
    """Yield a configured SMTP connection based on application settings."""
    host = current_app.config.get("SMTP_HOST")
    port = int(current_app.config.get("SMTP_PORT", 587))
    username = current_app.config.get("SMTP_USER")
    password = current_app.config.get("SMTP_PASS")
    use_ssl = bool(current_app.config.get("SMTP_USE_SSL"))
    use_tls = bool(current_app.config.get("SMTP_USE_TLS", True)) and not use_ssl

    if use_ssl:
        server: smtplib.SMTP | smtplib.SMTP_SSL = smtplib.SMTP_SSL(host, port, timeout=15)
    else:
        server = smtplib.SMTP(host, port, timeout=15)
    try:
        if use_tls and not use_ssl:
            server.starttls()
        if username and password:
            server.login(username, password)
        yield server
    finally:
        try:
            server.quit()
        except Exception:
            current_app.logger.debug("SMTP quit failed", exc_info=True)


def send_email(
    *,
    subject: str,
    recipients: Iterable[str],
    html_template: str,
    text_template: str,
    context: dict[str, object] | None = None,
) -> None:
    """Render templates and deliver an email via SMTP."""
    context = context or {}
    sender = current_app.config.get("EMAIL_FROM")
    if not sender:
        raise RuntimeError("EMAIL_FROM is not configured")

    suppress_send = bool(current_app.config.get("MAIL_SUPPRESS_SEND"))
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(render_template(text_template, **context))
    msg.add_alternative(render_template(html_template, **context), subtype="html")

    if suppress_send:
        current_app.logger.info(
            "Email suppressed (MAIL_SUPPRESS_SEND=true)",
            extra={"subject": subject, "to": list(recipients)},
        )
        return

    with _smtp_connection() as smtp:
        smtp.send_message(msg)
        current_app.logger.info("Email dispatched", extra={"subject": subject, "to": list(recipients)})
