from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from paper_fetch.normalize import normalize_doi


class NormalizeTests(unittest.TestCase):
    def test_doi_url_drops_query_and_fragment(self):
        self.assertEqual(
            normalize_doi("https://doi.org/10.1234/example?utm_source=test#section"),
            "10.1234/example",
        )

    def test_raw_doi_preserves_legal_question_mark_in_suffix(self):
        self.assertEqual(normalize_doi("10.1234/example?part"), "10.1234/example?part")


if __name__ == "__main__":
    unittest.main()
