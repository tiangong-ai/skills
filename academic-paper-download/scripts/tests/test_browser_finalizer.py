from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, make_pdf_bytes
import finalize_browser_download as browser
from paper_fetch.errors import PaperFetchError


class BrowserFinalizerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.downloads = Path(self.temporary.name)
        self.output = self.downloads / "final"
        self.snapshot_path = self.downloads / "snapshot.json"

    def _snapshot(self, expected: str = "Expected.pdf") -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            code = browser.snapshot(self.downloads, expected, self.snapshot_path)
        self.assertEqual(code, 0)

    def _args(self, **overrides):
        values = {
            "snapshot": str(self.snapshot_path),
            "downloads_dir": str(self.downloads),
            "expected_filename": "Expected.pdf",
            "output_dir": str(self.output),
            "doi": "10.1234/example",
            "title": "Expected paper",
            "author": "Alice Example",
            "year": "2024",
            "journal": None,
            "source_url": "https://publisher.example/article",
            "download_id": "download-123",
            "browser_backend": None,
            "download_url": None,
            "suggested_filename": None,
            "cloakbrowser_version": None,
            "browser_version": None,
            "access_mode": "current-browser-session",
            "filename": "Expected.pdf",
            "timeout": 0.2,
            "poll_interval": 0.001,
            "stable_seconds": 0,
            "max_bytes": 1024,
            "access_basis": "user_authorized_browser",
            "license_status": "unknown",
            "license": None,
            "license_url": None,
            "host_type": None,
            "article_version": None,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_concurrent_newer_pdf_is_never_selected(self):
        self._snapshot()
        expected = self.downloads / "Expected.pdf"
        unrelated = self.downloads / "Unrelated.pdf"
        expected.write_bytes(PDF_BYTES + b"expected")
        unrelated.write_bytes(PDF_BYTES + b"unrelated")
        now = time.time_ns()
        os.utime(expected, ns=(now, now))
        os.utime(unrelated, ns=(now + 1_000_000, now + 1_000_000))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(Path(payload["data"]["file"]).read_bytes(), PDF_BYTES + b"expected")
        self.assertEqual(unrelated.read_bytes(), PDF_BYTES + b"unrelated")
        self.assertEqual(payload["data"]["source_detail"]["download_id"], "download-123")
        self.assertEqual(payload["data"]["access_basis"], "user_authorized_browser")
        self.assertEqual(payload["data"]["license_status"], "unknown")
        manifest = json.loads(Path(payload["manifest"]).read_text(encoding="utf-8"))
        final_path = Path(payload["data"]["file"])
        self.assertEqual(payload["data"]["size"], final_path.stat().st_size)
        self.assertEqual(payload["data"]["size"], manifest["size"])
        self.assertEqual(payload["data"]["sha256"], manifest["sha256"])

    def test_snapshot_rejects_reserved_expected_path(self):
        (self.downloads / "Expected.pdf").write_bytes(PDF_BYTES)
        with contextlib.redirect_stdout(io.StringIO()):
            code = browser.snapshot(self.downloads, "Expected.pdf", self.snapshot_path)
        self.assertEqual(code, 3)

    def test_finalize_rejects_filename_different_from_snapshot(self):
        self._snapshot()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args(expected_filename="Other.pdf"))
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "expected_filename_mismatch")

    def test_finalize_rejects_directory_different_from_snapshot(self):
        self._snapshot()
        other = self.downloads / "other"
        other.mkdir()
        (other / "Expected.pdf").write_bytes(PDF_BYTES)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args(downloads_dir=str(other)))
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "snapshot_directory_mismatch")

    def test_finalize_rejects_file_that_predates_snapshot(self):
        self._snapshot()
        expected = self.downloads / "Expected.pdf"
        expected.write_bytes(PDF_BYTES)
        state = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        old = state["created_at_ns"] - 1_000_000_000
        os.utime(expected, ns=(old, old))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "download_predates_snapshot")

    def test_finalize_rejects_symbolic_link(self):
        self._snapshot()
        actual = self.downloads / "Actual.pdf"
        actual.write_bytes(PDF_BYTES)
        (self.downloads / "Expected.pdf").symlink_to(actual)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "unsafe_download_path")

    def test_identity_mismatch_is_not_finalized(self):
        self._snapshot()
        source = self.downloads / "Expected.pdf"
        source.write_bytes(
            make_pdf_bytes(
                doi="10.9999/wrong",
                title="Wrong paper",
                author="Mallory",
                year=2020,
            )
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(payload["error"]["code"], "pdf_identity_mismatch")
        self.assertTrue(source.is_file())
        self.assertEqual(list(self.output.glob("*.pdf")), [])
        self.assertEqual(list(self.output.glob("*.json")), [])

    def test_identity_unresolved_is_not_finalized(self):
        self._snapshot()
        source = self.downloads / "Expected.pdf"
        source.write_bytes(
            make_pdf_bytes(
                doi=None,
                title=None,
                author=None,
                year=None,
                include_document_metadata=False,
            )
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(payload["error"]["code"], "pdf_identity_unresolved")
        self.assertTrue(source.is_file())
        self.assertEqual(list(self.output.glob("*.pdf")), [])
        self.assertEqual(list(self.output.glob("*.json")), [])

    def test_identity_validator_unavailable_is_runtime_failure(self):
        self._snapshot()
        (self.downloads / "Expected.pdf").write_bytes(PDF_BYTES)
        output = io.StringIO()
        with mock.patch(
            "finalize_browser_download.validate_pdf_identity",
            side_effect=PaperFetchError(
                "pdf_validator_unavailable",
                "identity validator unavailable",
            ),
        ), contextlib.redirect_stdout(output):
            code = browser.finalize(self._args())
        self.assertEqual(code, 4)
        self.assertEqual(
            json.loads(output.getvalue())["error"]["code"],
            "pdf_validator_unavailable",
        )

    def test_legacy_destination_is_not_replayed_as_verified_duplicate(self):
        self._snapshot()
        source = self.downloads / "Expected.pdf"
        source.write_bytes(PDF_BYTES)
        first_output = io.StringIO()
        with contextlib.redirect_stdout(first_output):
            self.assertEqual(browser.finalize(self._args()), 0)
        first = json.loads(first_output.getvalue())
        sidecar = Path(first["manifest"])
        legacy = json.loads(sidecar.read_text(encoding="utf-8"))
        legacy["schema_version"] = "academic-paper-download.artifact.v2"
        legacy.pop("identity_status", None)
        legacy.pop("identity", None)
        sidecar.write_text(json.dumps(legacy), encoding="utf-8")

        source.unlink()
        self._snapshot()
        source.write_bytes(PDF_BYTES)
        second_output = io.StringIO()
        with contextlib.redirect_stdout(second_output):
            self.assertEqual(browser.finalize(self._args()), 0)
        second = json.loads(second_output.getvalue())
        self.assertFalse(second["data"]["duplicate"])
        self.assertEqual(second["data"]["identity_status"], "matched")
        self.assertNotEqual(second["data"]["file"], first["data"]["file"])

    def test_invalid_doi_is_validation_exit(self):
        self._snapshot()
        (self.downloads / "Expected.pdf").write_bytes(PDF_BYTES)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args(doi="not-a-doi"))
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "validation_error")

    def test_output_directory_error_is_json_failure(self):
        self._snapshot()
        (self.downloads / "Expected.pdf").write_bytes(PDF_BYTES)
        blocked = self.downloads / "not-a-directory"
        blocked.write_text("x", encoding="utf-8")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args(output_dir=str(blocked)))
        self.assertEqual(code, 4)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "output_dir_error")

    def test_finalize_requires_explicit_output_directory(self):
        self._snapshot()
        (self.downloads / "Expected.pdf").write_bytes(PDF_BYTES)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = browser.finalize(self._args(output_dir=None))
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "validation_error")


if __name__ == "__main__":
    unittest.main()
