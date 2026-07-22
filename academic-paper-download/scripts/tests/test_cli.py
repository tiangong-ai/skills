from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp
from paper_fetch import cli
from paper_fetch.errors import PaperFetchError


class CliTests(unittest.TestCase):
    def test_success_and_idempotent_replay_use_verified_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = "https://oa.example/s2.pdf"
            http = RoutingHttp(
                json_routes={
                    "api.semanticscholar.org/graph/v1/paper/DOI": {
                        "title": "Example paper",
                        "year": 2024,
                        "authors": [{"name": "Alice Example"}],
                        "venue": "Journal",
                        "openAccessPdf": {"url": url},
                        "externalIds": {},
                    }
                },
                download_payloads={url: PDF_BYTES},
            )
            arguments = [
                "10.1234/example",
                "--out",
                temporary,
                "--idempotency-key",
                "retry-key",
            ]
            with mock.patch("paper_fetch.cli.HttpClient", return_value=http):
                first_stdout = io.StringIO()
                with contextlib.redirect_stdout(first_stdout), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(cli.main(arguments), 0)
                second_stdout = io.StringIO()
                with contextlib.redirect_stdout(second_stdout), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(cli.main(arguments), 0)
            first = json.loads(first_stdout.getvalue())
            second = json.loads(second_stdout.getvalue())
            self.assertTrue(Path(first["data"]["results"][0]["file"]).is_file())
            self.assertTrue(second["meta"]["replayed"])
            self.assertEqual(sum(method == "download" for method, _ in http.calls), 1)

    def test_stream_pretty_combination_is_rejected(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(["10.1234/example", "--out", "/tmp/papers", "--stream", "--pretty"])
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "validation_error")

    def test_title_transport_failure_uses_retryable_exit(self):
        transport = PaperFetchError("network_error", "offline", retryable=True)
        http = RoutingHttp(
            json_routes={
                "api.crossref.org/works": transport,
                "paper/search/match": transport,
            }
        )
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary, mock.patch(
            "paper_fetch.cli.HttpClient", return_value=http
        ), contextlib.redirect_stdout(output):
            code = cli.main(["--title", "Attention Is All You Need", "--out", temporary])
        self.assertEqual(code, 4)
        result = json.loads(output.getvalue())["data"]["results"][0]
        self.assertEqual(result["error"]["code"], "title_resolution_transport")
        self.assertTrue(result["error"]["retryable"])

    def test_title_qualifiers_require_title(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(["10.1234/example", "--year", "2024", "--out", "/tmp/papers"])
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "validation_error")

    def test_missing_out_is_clear_validation_error(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(["10.1234/example"])
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 3)
        self.assertEqual(payload["error"]["code"], "validation_error")
        self.assertIn("--out", payload["error"]["message"])


if __name__ == "__main__":
    unittest.main()
