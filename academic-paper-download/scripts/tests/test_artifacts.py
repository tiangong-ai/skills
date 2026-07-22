from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp
from paper_fetch.artifact import ArtifactStore, manifest_path
from paper_fetch.errors import PaperFetchError
from paper_fetch.models import Candidate, PaperMetadata


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.url = "https://oa.example/paper.pdf"
        self.metadata = PaperMetadata(title="同名论文", author="张三", year=2024)

    def test_atomic_commit_writes_pdf_hash_and_manifest(self):
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        result = ArtifactStore(self.directory, http).save(
            "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
        )
        path = Path(result["file"])
        manifest = json.loads(manifest_path(path).read_text(encoding="utf-8"))
        self.assertEqual(manifest["doi"], "10.1234/one")
        self.assertEqual(manifest["sha256"], result["sha256"])
        self.assertEqual(manifest["size"], result["size"])
        self.assertEqual(manifest["size"], path.stat().st_size)
        self.assertEqual(manifest["sha256"], result["sha256"])
        self.assertEqual(manifest["license"], "unknown")
        self.assertEqual(manifest["license_status"], "unknown")
        self.assertIn("张三", path.name)
        self.assertEqual(list(self.directory.glob("*.part")), [])

    def test_matching_manifest_and_hash_are_required_to_skip(self):
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        second = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertFalse(first["skipped"])
        self.assertTrue(second["skipped"])
        self.assertTrue(second["verified_existing"])
        self.assertEqual(sum(method == "download" for method, _ in http.calls), 1)
        self.assertEqual(second["committed_manifest"]["source"], "unpaywall")

    def test_corrupt_existing_file_is_not_skipped_and_gets_collision_name(self):
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        Path(first["file"]).write_bytes(b"<html>corrupt</html>")
        second = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertFalse(second["skipped"])
        self.assertNotEqual(first["file"], second["file"])
        self.assertTrue(second["file"].endswith("-2.pdf"))

    def test_same_filename_different_doi_does_not_skip(self):
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        second = store.save("10.1234/two", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertNotEqual(first["file"], second["file"])
        self.assertFalse(second["skipped"])

    def test_html_response_never_commits(self):
        http = RoutingHttp(download_payloads={self.url: b"<html>login</html>"})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "invalid_pdf")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_truncated_pdf_with_header_is_rejected(self):
        http = RoutingHttp(download_payloads={self.url: b"%PDF-1.7\ntruncated"})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "invalid_pdf")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_missing_pdf_parser_fails_closed(self):
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        with mock.patch("paper_fetch.artifact.importlib.import_module", side_effect=ImportError):
            with self.assertRaises(PaperFetchError) as raised:
                ArtifactStore(self.directory, http).save(
                    "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
                )
        self.assertEqual(raised.exception.code, "pdf_validator_unavailable")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_symbolic_link_output_directory_is_rejected(self):
        target = self.directory / "target"
        target.mkdir()
        link = self.directory / "linked-output"
        link.symlink_to(target, target_is_directory=True)
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(link, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "output_dir_error")

    def test_file_used_as_output_directory_is_rejected(self):
        blocked = self.directory / "not-a-directory"
        blocked.write_text("blocked", encoding="utf-8")
        http = RoutingHttp(download_payloads={self.url: PDF_BYTES})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(blocked, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "output_dir_error")


if __name__ == "__main__":
    unittest.main()
