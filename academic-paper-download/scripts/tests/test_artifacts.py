from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import RoutingHttp, make_pdf_bytes
from paper_fetch.artifact import ArtifactStore, manifest_path
from paper_fetch.errors import PaperFetchError
from paper_fetch.identity import MAX_FIRST_PAGE_CONTENT_BYTES, MAX_FIRST_PAGE_ENCODED_BYTES
from paper_fetch.models import Candidate, PaperMetadata


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.url = "https://oa.example/paper.pdf"
        self.metadata = PaperMetadata(title="同名论文", author="张三", year=2024)
        self.pdf_bytes = make_pdf_bytes(
            doi="10.1234/one",
            title="同名论文",
            author="张三",
            year=2024,
        )

    def test_atomic_commit_writes_pdf_hash_and_manifest(self):
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
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
        self.assertEqual(manifest["identity_status"], "matched")
        self.assertEqual(manifest["identity"]["method"], "doi_document_metadata")
        self.assertIn("张三", path.name)
        self.assertEqual(list(self.directory.glob("*.part")), [])

    def test_matching_manifest_and_hash_are_required_to_skip(self):
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        second = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertFalse(first["skipped"])
        self.assertTrue(second["skipped"])
        self.assertTrue(second["verified_existing"])
        self.assertEqual(sum(method == "download" for method, _ in http.calls), 1)
        self.assertEqual(second["committed_manifest"]["source"], "unpaywall")

    def test_corrupt_existing_file_is_not_skipped_and_gets_collision_name(self):
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        Path(first["file"]).write_bytes(b"<html>corrupt</html>")
        second = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertFalse(second["skipped"])
        self.assertNotEqual(first["file"], second["file"])
        self.assertTrue(second["file"].endswith("-2.pdf"))

    def test_same_filename_different_doi_does_not_skip(self):
        second_url = "https://oa.example/paper-two.pdf"
        http = RoutingHttp(
            download_payloads={
                self.url: self.pdf_bytes,
                second_url: make_pdf_bytes(
                    doi="10.1234/two",
                    title="同名论文",
                    author="张三",
                    year=2024,
                ),
            }
        )
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        second = store.save("10.1234/two", Candidate("unpaywall", second_url), self.metadata, timeout=1)
        self.assertNotEqual(first["file"], second["file"])
        self.assertFalse(second["skipped"])

    def test_wrong_primary_doi_is_rejected_before_commit(self):
        wrong = make_pdf_bytes(
            doi="10.9999/wrong",
            title="Completely different paper",
            author="Mallory",
            year=2020,
        )
        http = RoutingHttp(download_payloads={self.url: wrong})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "pdf_identity_mismatch")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])
        self.assertEqual(list(self.directory.glob("*.json")), [])
        self.assertEqual(list(self.directory.glob("*.part")), [])

    def test_document_doi_url_ignores_tracking_query_and_fragment(self):
        payload = make_pdf_bytes(
            doi="https://doi.org/10.1234/one?utm_source=repository#landing",
            title="Requested Paper",
            author="Alice",
            year=2024,
        )
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save(
            "10.1234/one",
            Candidate("unpaywall", self.url),
            PaperMetadata(title="Requested Paper", author="Alice", year=2024),
            timeout=1,
        )
        self.assertEqual(result["identity_status"], "matched")
        self.assertEqual(result["identity"]["method"], "doi_document_metadata")

    def test_first_page_requested_doi_wins_over_reference_doi_noise(self):
        payload = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
            first_page_lines=(
                "Requested Paper",
                "Journal header https://doi.org/10.1234/one",
                "Background cites doi:10.9999/reference",
            ),
        )
        metadata = PaperMetadata(title="Requested Paper", author="Alice", year=2024)
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["identity_status"], "matched")
        self.assertEqual(manifest["identity"]["method"], "doi_first_page")
        self.assertCountEqual(
            manifest["identity"]["observed"]["first_page_dois"],
            ["10.1234/one", "10.9999/reference"],
        )

    def test_doi_less_document_can_match_title_author_and_year(self):
        payload = make_pdf_bytes(
            doi=None,
            title="Requested Paper",
            author="Alice Example",
            year=2024,
        )
        metadata = PaperMetadata(title="Requested Paper", author="Alice Example", year=2024)
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["identity_status"], "matched")
        self.assertEqual(manifest["identity"]["method"], "title_document_metadata")
        self.assertEqual(manifest["identity"]["checks"]["author"]["status"], "matched")
        self.assertEqual(manifest["identity"]["checks"]["year"]["status"], "matched")

    def test_doi_less_accepted_manuscript_title_and_author_variants_match(self):
        payload = make_pdf_bytes(
            doi=None,
            title="A Study of Carbon Footprints: Accepted Manuscript",
            author="A. Example",
            year=2024,
        )
        metadata = PaperMetadata(
            title="A Study of Carbon Footprints",
            author="Alice Example",
            year=2024,
        )
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        self.assertEqual(result["identity_status"], "matched")
        self.assertEqual(result["identity"]["method"], "title_document_metadata")
        self.assertEqual(result["identity"]["checks"]["author"]["status"], "matched")

    def test_context_metadata_doi_is_not_promoted_to_primary_identity(self):
        payload = make_pdf_bytes(
            doi=None,
            title="Requested Paper",
            author="Alice Example",
            year=2024,
            subject="doi:10.9999/cited-reference",
        )
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save(
            "10.1234/one",
            Candidate("unpaywall", self.url),
            PaperMetadata(title="Requested Paper", author="Alice Example", year=2024),
            timeout=1,
        )
        self.assertEqual(result["identity_status"], "matched")
        self.assertEqual(result["identity"]["method"], "title_document_metadata")
        self.assertEqual(result["identity"]["observed"]["document_dois"], [])
        self.assertEqual(
            result["identity"]["observed"]["metadata_context_dois"],
            ["10.9999/cited-reference"],
        )

    def test_context_metadata_requested_doi_does_not_identify_wrong_pdf(self):
        payload = make_pdf_bytes(
            doi=None,
            title="Completely different paper",
            author="Mallory",
            year=2020,
            subject="doi:10.1234/one",
        )
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(
                self.directory,
                RoutingHttp(download_payloads={self.url: payload}),
            ).save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertEqual(raised.exception.code, "pdf_identity_mismatch")

    def test_first_page_requested_doi_can_resolve_preprint_metadata_conflict(self):
        payload = make_pdf_bytes(
            doi="10.48550/arxiv.2401.00001",
            title="Requested Paper",
            author="Alice Example",
            year=2024,
            first_page_lines=(
                "Requested Paper",
                "Journal header https://doi.org/10.1234/one",
            ),
        )
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save(
            "10.1234/one",
            Candidate("unpaywall", self.url),
            PaperMetadata(title="Requested Paper", author="Alice Example", year=2024),
            timeout=1,
        )
        self.assertEqual(result["identity_status"], "matched")
        self.assertEqual(result["identity"]["method"], "doi_first_page")
        self.assertIn(
            "document_doi_conflicts_with_first_page",
            result["identity"]["limitations"],
        )

    def test_reference_doi_noise_does_not_override_first_page_title_match(self):
        payload = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
            first_page_lines=(
                "Requested Paper",
                "Alice Example",
                "2024",
                "Background cites doi:10.9999/reference",
            ),
        )
        metadata = PaperMetadata(title="Requested Paper", author="Alice Example", year=2024)
        result = ArtifactStore(
            self.directory,
            RoutingHttp(download_payloads={self.url: payload}),
        ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["identity_status"], "matched")
        self.assertEqual(manifest["identity"]["method"], "title_first_page")
        self.assertEqual(
            manifest["identity"]["observed"]["first_page_dois"],
            ["10.9999/reference"],
        )
        self.assertIn("non_primary_dois_ignored", manifest["identity"]["limitations"])

    def test_doi_less_title_mismatch_is_rejected(self):
        payload = make_pdf_bytes(
            doi=None,
            title="Completely different paper",
            author="Mallory",
            year=2020,
        )
        metadata = PaperMetadata(title="Requested Paper", author="Alice Example", year=2024)
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(
                self.directory,
                RoutingHttp(download_payloads={self.url: payload}),
            ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        self.assertEqual(raised.exception.code, "pdf_identity_mismatch")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_requested_doi_in_later_references_does_not_identify_wrong_paper(self):
        payload = make_pdf_bytes(
            doi=None,
            title="Completely different paper",
            author="Mallory",
            year=2020,
            first_page_lines=("Completely different paper", "Mallory", "2020"),
            additional_pages=(("References", "doi:10.1234/one"),),
        )
        metadata = PaperMetadata(title="Requested Paper", author="Alice Example", year=2024)
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(
                self.directory,
                RoutingHttp(download_payloads={self.url: payload}),
            ).save("10.1234/one", Candidate("unpaywall", self.url), metadata, timeout=1)
        self.assertEqual(raised.exception.code, "pdf_identity_mismatch")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_pdf_without_identity_evidence_is_unresolved_and_not_committed(self):
        blank = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
        )
        http = RoutingHttp(download_payloads={self.url: blank})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "pdf_identity_unresolved")
        self.assertEqual(list(self.directory.glob("*.pdf")), [])
        self.assertEqual(list(self.directory.glob("*.json")), [])
        self.assertEqual(list(self.directory.glob("*.part")), [])

    def test_oversized_first_page_content_fails_closed_before_text_extraction(self):
        class OversizedContents:
            @staticmethod
            def get_data():
                return b"x" * (MAX_FIRST_PAGE_CONTENT_BYTES + 1)

        blank = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
        )
        http = RoutingHttp(download_payloads={self.url: blank})
        with mock.patch(
            "pypdf._page.PageObject.get_contents",
            return_value=OversizedContents(),
        ), self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "pdf_identity_unresolved")
        self.assertIn(
            "first_page_content_limit_exceeded",
            raised.exception.details["identity"]["limitations"],
        )
        self.assertEqual(list(self.directory.glob("*.pdf")), [])

    def test_oversized_encoded_first_page_is_rejected_before_decompression(self):
        payload = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
            first_page_lines=("x" * (MAX_FIRST_PAGE_ENCODED_BYTES + 1),),
        )
        http = RoutingHttp(download_payloads={self.url: payload})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(self.directory, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "pdf_identity_unresolved")
        self.assertIn(
            "first_page_encoded_content_limit_exceeded",
            raised.exception.details["identity"]["limitations"],
        )

    def test_first_page_title_without_author_or_year_support_is_unresolved(self):
        payload = make_pdf_bytes(
            doi=None,
            title=None,
            author=None,
            year=None,
            include_document_metadata=False,
            first_page_lines=("Requested Paper",),
        )
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(
                self.directory,
                RoutingHttp(download_payloads={self.url: payload}),
            ).save(
                "10.1234/one",
                Candidate("unpaywall", self.url),
                PaperMetadata(title="Requested Paper"),
                timeout=1,
            )
        self.assertEqual(raised.exception.code, "pdf_identity_unresolved")

    def test_legacy_manifest_is_not_replayed_as_identity_verified(self):
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
        store = ArtifactStore(self.directory, http)
        first = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        sidecar = manifest_path(Path(first["file"]))
        legacy = json.loads(sidecar.read_text(encoding="utf-8"))
        legacy["schema_version"] = "academic-paper-download.artifact.v2"
        legacy.pop("identity_status", None)
        legacy.pop("identity", None)
        sidecar.write_text(json.dumps(legacy), encoding="utf-8")
        second = store.save("10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1)
        self.assertEqual(second["identity_status"], "matched")
        self.assertEqual(second["committed_manifest"]["identity_status"], "matched")
        self.assertNotEqual(
            second["committed_manifest"]["schema_version"],
            "academic-paper-download.artifact.v2",
        )

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
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
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
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(link, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "output_dir_error")

    def test_file_used_as_output_directory_is_rejected(self):
        blocked = self.directory / "not-a-directory"
        blocked.write_text("blocked", encoding="utf-8")
        http = RoutingHttp(download_payloads={self.url: self.pdf_bytes})
        with self.assertRaises(PaperFetchError) as raised:
            ArtifactStore(blocked, http).save(
                "10.1234/one", Candidate("unpaywall", self.url), self.metadata, timeout=1
            )
        self.assertEqual(raised.exception.code, "output_dir_error")


if __name__ == "__main__":
    unittest.main()
