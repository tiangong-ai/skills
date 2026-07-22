from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
SKILL = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES, RoutingHttp
from paper_fetch import FetchRequest, fetch_paper
from paper_fetch import cli
from paper_fetch.errors import PaperFetchError


class SanitizationTests(unittest.TestCase):
    def test_sensitive_url_values_do_not_enter_result_manifest_or_events(self):
        email = "private-contact@example.edu"
        token = "download-token-secret"
        pdf_url = f"https://oa.example/paper.pdf?email={email}&token={token}"
        transport = RoutingHttp(
            json_routes={
                "api.unpaywall.org": {
                    "title": "Example paper",
                    "best_oa_location": {
                        "url_for_pdf": pdf_url,
                        "license": "cc-by",
                    },
                }
            },
            download_payloads={pdf_url: PDF_BYTES},
        )
        events: list[dict] = []
        with tempfile.TemporaryDirectory() as output:
            result = fetch_paper(
                FetchRequest(
                    doi="10.1234/example",
                    output_dir=output,
                    unpaywall_email=email,
                ),
                transport=transport,
                progress=lambda event, fields: events.append({"event": event, **fields}),
            )
            manifest_text = Path(result["manifest"]).read_text(encoding="utf-8")
        emitted = json.dumps({"result": result, "events": events}) + manifest_text
        self.assertNotIn(email, emitted)
        self.assertNotIn(token, emitted)
        self.assertIn("REDACTED", emitted)
        self.assertTrue(any(email in url for _, url in transport.calls))

    def test_sensitive_failure_values_do_not_enter_stdout_or_stderr(self):
        api_key = "very-secret-api-key"
        bearer = "very-secret-bearer"
        failure = PaperFetchError(
            "network_error",
            f"authorization=Bearer-{bearer} api_key={api_key}",
            retryable=True,
            authorization=bearer,
            x_api_key=api_key,
            url=f"https://api.example/data?key={api_key}&token={bearer}",
        )
        transport = RoutingHttp(
            json_routes={"api.semanticscholar.org/graph/v1/paper/DOI": failure},
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as output, mock.patch.dict(
            os.environ,
            {"SEMANTIC_SCHOLAR_API_KEY": api_key},
            clear=False,
        ), mock.patch("paper_fetch.cli.HttpClient", return_value=transport), contextlib.redirect_stdout(
            stdout
        ), contextlib.redirect_stderr(stderr):
            code = cli.main(["10.1234/example", "--out", output])
        combined = stdout.getvalue() + stderr.getvalue()
        self.assertEqual(code, 4)
        self.assertNotIn(api_key, combined)
        self.assertNotIn(bearer, combined)
        self.assertIn("REDACTED", combined)

    def test_removed_source_name_is_absent_from_skill_files(self):
        forbidden = "sci" + "hub"
        matches: list[str] = []
        for path in SKILL.rglob("*"):
            if path.is_file():
                try:
                    text = path.read_text(encoding="utf-8").casefold().replace("-", " ")
                except UnicodeDecodeError:
                    continue
                if forbidden in text.replace(" ", ""):
                    matches.append(str(path.relative_to(SKILL)))
        self.assertEqual(matches, [])


if __name__ == "__main__":
    unittest.main()
