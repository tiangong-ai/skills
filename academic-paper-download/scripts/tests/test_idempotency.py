from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp
from paper_fetch.artifact import ArtifactStore
from paper_fetch.errors import PaperFetchError
from paper_fetch.idempotency import IdempotencyStore, request_fingerprint
from paper_fetch.models import Candidate, PaperMetadata


class IdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.url = "https://oa.example/paper.pdf"
        artifact = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: PDF_BYTES}),
        ).save(
            "10.1234/one",
            Candidate("unpaywall", self.url),
            PaperMetadata(title="Paper", author="Author", year=2024),
            timeout=1,
        )
        self.result = {"doi": "10.1234/one", "success": True, "source": "unpaywall", **artifact}
        self.envelope = {"ok": True, "data": {"results": [self.result]}}
        self.store = IdempotencyStore(self.directory)

    def test_same_request_replays_only_with_valid_artifact(self):
        fingerprint = request_fingerprint({"doi": "10.1234/one"})
        self.store.store("key", fingerprint, self.envelope)
        self.assertEqual(self.store.load("key", fingerprint), self.envelope)

    def test_same_key_different_request_is_conflict(self):
        first = request_fingerprint({"doi": "10.1234/one"})
        second = request_fingerprint({"doi": "10.1234/two"})
        self.store.store("key", first, self.envelope)
        with self.assertRaises(PaperFetchError) as raised:
            self.store.load("key", second)
        self.assertEqual(raised.exception.code, "idempotency_conflict")

    def test_missing_or_corrupt_artifact_is_not_replayed(self):
        fingerprint = request_fingerprint({"doi": "10.1234/one"})
        self.store.store("key", fingerprint, self.envelope)
        Path(self.result["file"]).write_bytes(b"corrupt")
        self.assertIsNone(self.store.load("key", fingerprint))

    def test_failure_envelope_is_not_cached(self):
        fingerprint = request_fingerprint({"doi": "10.1234/one"})
        self.store.store("key", fingerprint, {"ok": False, "data": {"results": []}})
        self.assertIsNone(self.store.load("key", fingerprint))


if __name__ == "__main__":
    unittest.main()
