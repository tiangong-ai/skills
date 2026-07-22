from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp
from paper_fetch import FetchRequest, fetch_paper
from paper_fetch.errors import PaperFetchError
from paper_fetch.resolvers import OpenAccessResolvers


class TitleResolverTests(unittest.TestCase):
    def test_title_resolution_and_download_use_injected_transport(self):
        pdf_url = "https://oa.example/title.pdf"
        transport = RoutingHttp(
            json_routes={
                "api.crossref.org/works": {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1234/example",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Vaswani"}],
                                "issued": {"date-parts": [[2017]]},
                            }
                        ]
                    }
                },
                "paper/search/match": {"data": []},
                "graph/v1/paper/DOI": {
                    "title": "Attention Is All You Need",
                    "authors": [{"name": "Vaswani"}],
                    "year": 2017,
                    "venue": "NeurIPS",
                    "isOpenAccess": True,
                    "openAccessPdf": {"url": pdf_url, "status": "GREEN"},
                    "externalIds": {},
                },
            },
            download_payloads={pdf_url: PDF_BYTES},
        )
        with tempfile.TemporaryDirectory() as output:
            result = fetch_paper(
                FetchRequest(title="Attention Is All You Need", output_dir=output),
                transport=transport,
            )
        self.assertTrue(result["success"])
        self.assertEqual(
            [method for method, _ in transport.calls],
            ["json", "json", "json", "download"],
        )

    def test_crossref_exact_title_is_accepted(self):
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1234/example",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Vaswani"}],
                                "issued": {"date-parts": [[2017]]},
                                "container-title": ["NeurIPS"],
                                "score": 100,
                            }
                        ]
                    }
                },
                "paper/search/match": {"data": []},
            }
        )
        result = OpenAccessResolvers(http).resolve_title("Attention Is All You Need", timeout=1)
        self.assertEqual(result.doi, "10.1234/example")
        self.assertFalse(result.details["low_confidence"])

    def test_unrelated_title_is_rejected_instead_of_auto_downloaded(self):
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1234/wrong",
                                "title": ["Completely different research"],
                                "score": 99,
                            }
                        ]
                    }
                },
                "paper/search/match": {
                    "data": [
                        {
                            "title": "Another unrelated article",
                            "externalIds": {"DOI": "10.1234/also-wrong"},
                            "authors": [],
                        }
                    ]
                },
            }
        )
        with self.assertRaises(PaperFetchError) as raised:
            OpenAccessResolvers(http).resolve_title("Attention Is All You Need", timeout=1)
        self.assertEqual(raised.exception.code, "title_low_confidence")

    def test_duplicate_exact_titles_require_disambiguation(self):
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1234/old",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Vaswani"}],
                                "issued": {"date-parts": [[2017]]},
                            },
                            {
                                "DOI": "10.1234/new",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Other"}],
                                "issued": {"date-parts": [[2025]]},
                            },
                        ]
                    }
                },
                "paper/search/match": {"data": []},
            }
        )
        with self.assertRaises(PaperFetchError) as raised:
            OpenAccessResolvers(http).resolve_title("Attention Is All You Need", timeout=1)
        self.assertEqual(raised.exception.code, "title_ambiguous")
        self.assertEqual(len(raised.exception.details["candidates"]), 2)

    def test_author_and_year_disambiguate_duplicate_title(self):
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": {
                    "message": {
                        "items": [
                            {
                                "DOI": "10.1234/old",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Vaswani"}],
                                "issued": {"date-parts": [[2017]]},
                            },
                            {
                                "DOI": "10.1234/new",
                                "title": ["Attention Is All You Need"],
                                "author": [{"family": "Other"}],
                                "issued": {"date-parts": [[2025]]},
                            },
                        ]
                    }
                },
                "paper/search/match": {"data": []},
            }
        )
        result = OpenAccessResolvers(http).resolve_title(
            "Attention Is All You Need", timeout=1, author="Vaswani", year=2017
        )
        self.assertEqual(result.doi, "10.1234/old")

    def test_transport_failure_remains_retryable(self):
        transport = PaperFetchError("network_error", "offline", retryable=True)
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": transport,
                "paper/search/match": transport,
            }
        )
        with self.assertRaises(PaperFetchError) as raised:
            OpenAccessResolvers(http).resolve_title("Attention Is All You Need", timeout=1)
        self.assertEqual(raised.exception.code, "title_resolution_transport")
        self.assertTrue(raised.exception.retryable)


if __name__ == "__main__":
    unittest.main()
