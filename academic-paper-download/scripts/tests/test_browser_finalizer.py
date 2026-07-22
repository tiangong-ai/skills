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

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES
import finalize_browser_download as browser


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
