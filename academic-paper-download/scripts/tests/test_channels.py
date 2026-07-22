from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp, arxiv_atom
from paper_fetch import FetchRequest, fetch_paper
from paper_fetch.artifact import ArtifactStore, manifest_path
from paper_fetch.errors import PaperFetchError
from paper_fetch.pipeline import PaperFetcher, SOURCE_ORDER
from paper_fetch.resolvers import OpenAccessResolvers


DOI = "10.1234/example"


def s2_payload(
    *,
    pdf: str | None = None,
    arxiv: str | None = None,
    license_value: str | None = None,
) -> dict:
    oa_pdf = None
    if pdf:
        oa_pdf = {"url": pdf, "status": "GREEN"}
        if license_value:
            oa_pdf["license"] = license_value
    return {
        "title": "Example paper",
        "year": 2024,
        "authors": [{"name": "Alice Example"}],
        "venue": "Example Journal",
        "isOpenAccess": bool(pdf),
        "openAccessPdf": oa_pdf,
        "externalIds": {"ArXiv": arxiv} if arxiv else {},
    }


class DownloadChannelTests(unittest.TestCase):
    def run_fetch(self, transport: RoutingHttp, *, email: str = ""):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        resolvers = OpenAccessResolvers(transport, unpaywall_email=email)
        fetcher = PaperFetcher(
            resolvers,
            ArtifactStore(Path(temporary.name), transport),
        )
        result = fetcher.fetch(DOI, timeout=1)
        if result.get("success"):
            self.assertTrue(Path(result["file"]).is_file())
            self.assertTrue(manifest_path(Path(result["file"])).is_file())
            self.assertEqual(len(result["sha256"]), 64)
        return result, transport.calls

    def test_automatic_source_order_is_fixed(self):
        self.assertEqual(
            SOURCE_ORDER,
            ("unpaywall", "semantic_scholar", "arxiv", "browser_handoff"),
        )

    def test_unpaywall_fields_reach_manifest(self):
        url = "https://oa.example/unpaywall.pdf"
        transport = RoutingHttp(
            json_routes={
                "api.unpaywall.org": {
                    "title": "Example paper",
                    "year": 2024,
                    "z_authors": [{"family": "Example"}],
                    "journal_name": "Example Journal",
                    "is_oa": True,
                    "oa_status": "gold",
                    "best_oa_location": {
                        "url_for_pdf": url,
                        "url": "https://publisher.example/article",
                        "license": "cc-by",
                        "license_url": "https://license.example/cc-by",
                        "version": "publishedVersion",
                        "host_type": "publisher",
                    },
                }
            },
            download_payloads={url: PDF_BYTES},
        )
        result, calls = self.run_fetch(transport, email="researcher@example.edu")
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(result["source"], "unpaywall")
        self.assertEqual(result["sources_tried"], ["unpaywall"])
        self.assertEqual(manifest["access_basis"], "open_access")
        self.assertEqual(manifest["license_status"], "declared")
        self.assertEqual(manifest["license"], "cc-by")
        self.assertEqual(manifest["license_url"], "https://license.example/cc-by")
        self.assertEqual(manifest["host_type"], "publisher")
        self.assertEqual(manifest["article_version"], "publishedVersion")
        self.assertFalse(any("semanticscholar" in called_url for _, called_url in calls))

    def test_missing_license_is_unknown(self):
        url = "https://oa.example/s2.pdf"
        transport = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(pdf=url)},
            download_payloads={url: PDF_BYTES},
        )
        result, _ = self.run_fetch(transport)
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(result["source"], "semantic_scholar")
        self.assertEqual(manifest["license"], "unknown")
        self.assertEqual(manifest["license_url"], "unknown")
        self.assertEqual(manifest["license_status"], "unknown")

    def test_arxiv_license_link_is_preserved(self):
        url = "https://arxiv.org/pdf/2401.01234.pdf"
        atom = arxiv_atom().replace(
            "</entry>",
            '<link rel="license" href="https://creativecommons.org/licenses/by/4.0/" /></entry>',
        )
        transport = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(arxiv="2401.01234")},
            text_routes={"export.arxiv.org/api/query": atom},
            download_payloads={url: PDF_BYTES},
        )
        result, _ = self.run_fetch(transport)
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(result["source"], "arxiv")
        self.assertEqual(manifest["license_status"], "declared")
        self.assertEqual(
            manifest["license_url"],
            "https://creativecommons.org/licenses/by/4.0/",
        )

    def test_failed_candidates_fall_through_to_browser_handoff(self):
        unpaywall_url = "https://oa.example/unpaywall.pdf"
        s2_url = "https://oa.example/s2.pdf"
        arxiv_url = "https://arxiv.org/pdf/2401.01234.pdf"
        transport = RoutingHttp(
            json_routes={
                "api.unpaywall.org": {
                    "title": "Example paper",
                    "best_oa_location": {"url_for_pdf": unpaywall_url},
                },
                "api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(
                    pdf=s2_url, arxiv="2401.01234"
                ),
            },
            text_routes={"export.arxiv.org/api/query": arxiv_atom()},
            download_payloads={
                unpaywall_url: b"<html>blocked</html>",
                s2_url: b"<html>blocked</html>",
                arxiv_url: b"<html>blocked</html>",
            },
        )
        result, calls = self.run_fetch(transport, email="researcher@example.edu")
        self.assertFalse(result["success"])
        self.assertEqual(
            result["sources_tried"],
            ["unpaywall", "semantic_scholar", "arxiv"],
        )
        self.assertIn("browser_handoff", result)
        downloaded = [url for method, url in calls if method == "download"]
        self.assertEqual(downloaded, [unpaywall_url, s2_url, arxiv_url])

    def test_retryable_transport_failure_stays_structured(self):
        failure = PaperFetchError(
            "network_error",
            "temporary outage",
            retryable=True,
        )
        transport = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": failure},
        )
        result, _ = self.run_fetch(transport)
        self.assertFalse(result["success"])
        self.assertTrue(result["error"]["retryable"])
        attempt = result["error"]["attempts"][0]
        self.assertEqual(attempt["stage"], "resolve")
        self.assertEqual(attempt["error"]["code"], "network_error")

    def test_doi_resolution_and_download_use_injected_transport(self):
        url = "https://oa.example/injected.pdf"
        transport = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(pdf=url)},
            download_payloads={url: PDF_BYTES},
        )
        with tempfile.TemporaryDirectory() as output:
            result = fetch_paper(
                FetchRequest(doi=DOI, output_dir=output),
                transport=transport,
            )
        self.assertTrue(result["success"])
        self.assertEqual([method for method, _ in transport.calls], ["json", "download"])


if __name__ == "__main__":
    unittest.main()
