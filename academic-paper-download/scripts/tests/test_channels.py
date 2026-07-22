from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp, arxiv_atom
from paper_fetch.artifact import ArtifactStore, manifest_path
from paper_fetch.pipeline import PaperFetcher
from paper_fetch.resolvers import OpenAccessResolvers
from paper_fetch.scihub import SciHubResolver


DOI = "10.1234/example"


def s2_payload(*, pdf: str | None = None, arxiv: str | None = None) -> dict:
    return {
        "title": "Example paper",
        "year": 2024,
        "authors": [{"name": "Alice Example"}],
        "venue": "Example Journal",
        "openAccessPdf": {"url": pdf} if pdf else None,
        "externalIds": {"ArXiv": arxiv} if arxiv else {},
    }


class DownloadChannelTests(unittest.TestCase):
    def run_fetch(self, http: RoutingHttp, *, email: str = "", scihub: bool = True):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        resolvers = OpenAccessResolvers(http, unpaywall_email=email)
        fetcher = PaperFetcher(
            resolvers,
            SciHubResolver(http, mirrors=["sci-hub.test"]),
            ArtifactStore(Path(temporary.name), http),
            scihub_enabled=scihub,
        )
        result = fetcher.fetch(DOI, timeout=1)
        if result.get("success"):
            self.assertTrue(Path(result["file"]).is_file())
            self.assertTrue(manifest_path(Path(result["file"])).is_file())
            self.assertEqual(len(result["sha256"]), 64)
        return result, http.calls

    def test_unpaywall_channel_downloads_and_short_circuits(self):
        url = "https://oa.example/unpaywall.pdf"
        http = RoutingHttp(
            json_routes={
                "api.unpaywall.org": {
                    "title": "Example paper",
                    "year": 2024,
                    "z_authors": [{"family": "Example"}],
                    "journal_name": "Example Journal",
                    "best_oa_location": {"url_for_pdf": url},
                }
            },
            download_payloads={url: PDF_BYTES},
        )
        result, calls = self.run_fetch(http, email="researcher@example.edu")
        self.assertEqual(result["source"], "unpaywall")
        self.assertEqual(result["sources_tried"], ["unpaywall"])
        self.assertFalse(any("semanticscholar" in url for _, url in calls))

    def test_semantic_scholar_channel_downloads_after_unpaywall_skip(self):
        url = "https://oa.example/s2.pdf"
        http = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(pdf=url)},
            download_payloads={url: PDF_BYTES},
        )
        result, _ = self.run_fetch(http)
        self.assertEqual(result["source"], "semantic_scholar")
        self.assertEqual(result["sources_tried"], ["semantic_scholar"])

    def test_arxiv_channel_downloads_after_s2_has_no_pdf(self):
        url = "https://arxiv.org/pdf/2401.01234.pdf"
        http = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(arxiv="2401.01234")},
            text_routes={"export.arxiv.org/api/query": arxiv_atom()},
            download_payloads={url: PDF_BYTES},
        )
        result, _ = self.run_fetch(http)
        self.assertEqual(result["source"], "arxiv")
        self.assertEqual(result["sources_tried"], ["semantic_scholar", "arxiv"])

    def test_scihub_channel_downloads_after_all_oa_channels(self):
        pdf_url = "https://cdn.sci-hub.test/example.pdf"
        http = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload()},
            text_routes={"sci-hub.test": f'<html><iframe id="pdf" src="{pdf_url}"></iframe></html>'},
            download_payloads={pdf_url: PDF_BYTES},
        )
        result, _ = self.run_fetch(http)
        self.assertEqual(result["source"], "scihub")
        self.assertEqual(result["sources_tried"], ["semantic_scholar", "arxiv", "scihub"])
        self.assertEqual(result["source_detail"]["mirror"], "sci-hub.test")

    def test_failed_candidates_fall_through_in_fixed_full_order(self):
        unpaywall_url = "https://oa.example/unpaywall.pdf"
        s2_url = "https://oa.example/s2.pdf"
        arxiv_url = "https://arxiv.org/pdf/2401.01234.pdf"
        scihub_url = "https://cdn.sci-hub.test/final.pdf"
        http = RoutingHttp(
            json_routes={
                "api.unpaywall.org": {
                    "title": "Example paper",
                    "year": 2024,
                    "z_authors": [{"family": "Example"}],
                    "best_oa_location": {"url_for_pdf": unpaywall_url},
                },
                "api.semanticscholar.org/graph/v1/paper/DOI": s2_payload(
                    pdf=s2_url, arxiv="2401.01234"
                ),
            },
            text_routes={
                "export.arxiv.org/api/query": arxiv_atom(),
                "sci-hub.test": f'<iframe id="pdf" src="{scihub_url}"></iframe>',
            },
            download_payloads={
                unpaywall_url: b"<html>blocked</html>",
                s2_url: b"<html>blocked</html>",
                arxiv_url: b"<html>blocked</html>",
                scihub_url: PDF_BYTES,
            },
        )
        result, calls = self.run_fetch(http, email="researcher@example.edu")
        self.assertEqual(
            result["sources_tried"],
            ["unpaywall", "semantic_scholar", "arxiv", "scihub"],
        )
        downloaded = [url for method, url in calls if method == "download"]
        self.assertEqual(downloaded, [unpaywall_url, s2_url, arxiv_url, scihub_url])
        self.assertEqual(result["source"], "scihub")

    def test_scihub_disable_preserves_oa_order(self):
        http = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": s2_payload()},
        )
        result, calls = self.run_fetch(http, scihub=False)
        self.assertFalse(result["success"])
        self.assertEqual(result["sources_tried"], ["semantic_scholar", "arxiv"])
        self.assertFalse(any("sci-hub" in url for _, url in calls))
        self.assertIn("browser_handoff", result)


if __name__ == "__main__":
    unittest.main()
