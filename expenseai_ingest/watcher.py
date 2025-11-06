"""Filesystem watcher that ingests invoices dropped into shared folders."""
from __future__ import annotations

import atexit
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from flask import Flask
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from expenseai_ingest.config import IngestSettings
from expenseai_ingest.tasks import create_invoice_from_path
from expenseai_ingest import utils


class _InvoiceEventHandler(FileSystemEventHandler):
    def __init__(self, manager: "WatcherManager", root: Path):
        super().__init__()
        self.manager = manager
        self.root = root

    def on_created(self, event: FileSystemEvent) -> None:  # pragma: no cover - requires watchdog runtime
        if not event.is_directory:
            self.manager.submit(Path(event.src_path), root=self.root)

    def on_moved(self, event: FileSystemEvent) -> None:  # pragma: no cover - requires watchdog runtime
        if not event.is_directory:
            self.manager.submit(Path(event.dest_path), root=self.root)


class WatcherManager:
    def __init__(self, app: Flask, settings: IngestSettings):
        self.app = app
        self.settings = settings
        self._observer = Observer()
        self._lock = threading.Lock()
        self._processed: set[str] = set()
        self._running = False
        self._last_event: datetime | None = None

    @property
    def watch_paths(self) -> list[str]:
        return [str(Path(path)) for path in self.settings.watch_paths]

    @property
    def is_running(self) -> bool:
        return self._running and self._observer.is_alive()

    @property
    def last_event(self) -> datetime | None:
        return self._last_event

    def start(self) -> bool:
        if self._running or not self.settings.watch_paths:
            return False
        started = False
        for path_str in self.settings.watch_paths:
            path = Path(path_str)
            if not path.exists():
                self.app.logger.warning("Ingest watch path missing", extra={"path": path_str})
                continue
            handler = _InvoiceEventHandler(self, path)
            self._observer.schedule(handler, str(path), recursive=False)
            started = True
        if not started:
            return False
        self._observer.start()
        self._running = True
        self.app.logger.info("Started ingestion folder watcher", extra={"paths": self.watch_paths})
        self.scan_now()
        return True

    def stop(self) -> None:
        if not self._running:
            return
        self._observer.stop()
        self._observer.join(timeout=5)
        self._running = False

    def submit(self, path: Path, *, root: Optional[Path] = None) -> None:
        if not path.exists() or path.is_dir():
            return
        canonical = str(path.resolve())
        with self._lock:
            if canonical in self._processed:
                return
            self._processed.add(canonical)
        try:
            self._ingest_path(path, root=root)
        except Exception as exc:  # pragma: no cover - defensive logging
            with self.app.app_context():
                self.app.logger.exception("Failed to enqueue watched file", extra={"path": str(path), "error": str(exc)})
            with self._lock:
                self._processed.discard(canonical)

    def _wait_for_stable_size(self, path: Path, attempts: int = 5, delay: float = 0.5) -> bool:
        previous = -1
        for _ in range(attempts):
            size = path.stat().st_size
            if size == previous and size > 0:
                return True
            previous = size
            time.sleep(delay)
        return False

    def _ingest_path(self, path: Path, *, root: Optional[Path]) -> None:
        if not self._wait_for_stable_size(path):
            raise RuntimeError("File size never stabilized before timeout")
        with path.open("rb") as fh:
            head = fh.read(4096)
        utils.validate_extension(path.name, self.settings.allowed_extensions)
        mime_guess = utils.guess_mime_from_name(path.name)
        mime = utils.detect_mime(head, mime_guess)
        utils.enforce_mime(mime, self.settings.allowed_mime_types)
        size = path.stat().st_size
        if size > self.settings.max_bytes:
            raise ValueError("File exceeds ingestion size limit")
        metadata = {
            "source": "watcher",
            "watch_root": str(root) if root else None,
            "ingested_at": datetime.utcnow().isoformat() + "Z",
        }
        with self.app.app_context():
            self.app.logger.info(
                "Queueing ingested file",
                extra={"path": str(path), "size": size, "mime": mime},
            )
        create_invoice_from_path.delay(str(path), metadata=metadata)
        self._last_event = datetime.utcnow()

    def scan_now(self) -> int:
        discovered = 0
        for path_str in self.settings.watch_paths:
            root = Path(path_str)
            if not root.exists():
                continue
            for candidate in root.iterdir():
                if candidate.is_file():
                    self.submit(candidate, root=root)
                    discovered += 1
        return discovered

    def status(self) -> dict[str, object]:
        return {
            "running": self.is_running,
            "paths": self.watch_paths,
            "processed": len(self._processed),
            "last_event": self._last_event.isoformat() + "Z" if self._last_event else None,
        }


def start_watchers(app: Flask, settings: IngestSettings) -> WatcherManager | None:
    if not settings.watch_paths:
        return None
    manager = WatcherManager(app, settings)
    if manager.start():
        atexit.register(manager.stop)
        return manager
    return None


__all__ = ["WatcherManager", "start_watchers"]
