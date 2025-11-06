"""IMAP/POP ingestion worker that mirrors attachments into the queue."""
from __future__ import annotations

import atexit
import imaplib
import threading
from datetime import datetime
from email import message_from_bytes, policy
from email.message import Message
from typing import Iterable

from flask import Flask

from expenseai_ingest.config import IngestSettings
from expenseai_ingest.tasks import create_invoice_from_bytes
from expenseai_ingest import utils


class EmailPoller(threading.Thread):
    def __init__(self, app: Flask, settings: IngestSettings):
        super().__init__(daemon=True, name="expenseai-email-poller")
        self.app = app
        self.settings = settings
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self.last_poll: datetime | None = None
        self.errors: list[str] = []

    @property
    def enabled(self) -> bool:
        return self.settings.email.enabled

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    def trigger_now(self) -> None:
        self._wake_event.set()

    def run(self) -> None:  # pragma: no cover - requires IMAP
        if not self.enabled:
            return
        poll_wait = max(1, self.settings.email.poll_secs)
        while not self._stop_event.is_set():
            try:
                processed = self._poll_once()
                self.last_poll = datetime.utcnow()
                if processed:
                    with self.app.app_context():
                        self.app.logger.info("Processed email attachments", extra={"count": processed})
            except Exception as exc:
                with self.app.app_context():
                    self.app.logger.exception("Email ingestion failed", extra={"error": str(exc)})
                self.errors.append(str(exc))
            self._wake_event.wait(timeout=poll_wait)
            self._wake_event.clear()

    def _poll_once(self) -> int:
        email_cfg = self.settings.email
        client_cls = imaplib.IMAP4_SSL if email_cfg.use_ssl else imaplib.IMAP4
        processed = 0
        with client_cls(email_cfg.host) as client:
            client.login(email_cfg.username, email_cfg.password)
            client.select(email_cfg.folder, readonly=False)
            status, data = client.search(None, "UNSEEN")
            if status != "OK":
                return 0
            message_ids: Iterable[bytes] = data[0].split()
            for uid in message_ids:
                status, payload = client.fetch(uid, "(RFC822)")
                if status != "OK" or not payload:
                    continue
                msg_bytes = payload[0][1]
                message = message_from_bytes(msg_bytes, policy=policy.default)
                processed += self._process_message(message, uid.decode("ascii", "ignore"))
                client.store(uid, "+FLAGS", "(\\Seen)")
        return processed

    def _process_message(self, message: Message, uid: str) -> int:
        attachments = list(message.iter_attachments())
        processed = 0
        for attachment in attachments:
            filename = attachment.get_filename() or f"ingest-{uid}"
            try:
                utils.validate_extension(filename, self.settings.allowed_extensions)
            except ValueError:
                continue
            payload = attachment.get_payload(decode=True)
            if not payload:
                continue
            mime = attachment.get_content_type()
            try:
                utils.enforce_mime(mime, self.settings.allowed_mime_types)
            except ValueError:
                continue
            if len(payload) > self.settings.max_bytes:
                continue
            metadata = {
                "source": "email",
                "message_id": message.get("Message-ID"),
                "subject": message.get("Subject"),
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "uid": uid,
            }
            create_invoice_from_bytes.delay(
                data_b64=utils.encode_bytes(payload),
                filename=filename,
                metadata=metadata,
                mime_type=mime,
            )
            processed += 1
        return processed

    def status(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "last_poll": self.last_poll.isoformat() + "Z" if self.last_poll else None,
            "errors": self.errors[-5:],
        }


def start_email_poller(app: Flask, settings: IngestSettings) -> EmailPoller | None:
    poller = EmailPoller(app, settings)
    if not poller.enabled:
        return None
    poller.start()
    atexit.register(poller.stop)
    return poller


__all__ = ["EmailPoller", "start_email_poller"]
