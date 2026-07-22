from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .artifact import atomic_write_json, verify_existing
from .errors import PaperFetchError


SCHEMA = "academic-paper-download.idempotency.v2"


def request_fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self, output_dir: Path) -> None:
        self.directory = output_dir.expanduser().resolve() / ".academic-paper-download-idem"

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.directory / f"{digest}.json"

    def load(self, key: str, fingerprint: str) -> dict[str, Any] | None:
        path = self._path(key)
        if not path.is_file():
            return None
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if record.get("schema_version") != SCHEMA:
            return None
        if record.get("request_fingerprint") != fingerprint:
            raise PaperFetchError(
                "idempotency_conflict",
                "This idempotency key was already used for a different request",
                retryable=False,
            )
        envelope = record.get("envelope")
        if not isinstance(envelope, dict) or envelope.get("ok") is not True:
            return None
        results = ((envelope.get("data") or {}).get("results") or [])
        for result in results:
            if result.get("dry_run"):
                return None
            file_value = result.get("file")
            doi = result.get("doi")
            if not file_value or not doi or verify_existing(Path(file_value), doi) is None:
                return None
        return envelope

    def store(self, key: str, fingerprint: str, envelope: dict[str, Any]) -> None:
        if envelope.get("ok") is not True:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self._path(key),
            {
                "schema_version": SCHEMA,
                "request_fingerprint": fingerprint,
                "envelope": envelope,
            },
        )
