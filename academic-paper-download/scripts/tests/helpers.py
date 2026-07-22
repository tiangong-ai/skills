from __future__ import annotations

import copy
import io
from pathlib import Path
from typing import Any

from pypdf import PdfWriter

from paper_fetch.errors import PaperFetchError


def make_pdf_bytes() -> bytes:
    buffer = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    writer.write(buffer)
    return buffer.getvalue()


PDF_BYTES = make_pdf_bytes()


class RoutingHttp:
    def __init__(
        self,
        *,
        json_routes: dict[str, dict[str, Any] | PaperFetchError] | None = None,
        text_routes: dict[str, str | PaperFetchError] | None = None,
        download_payloads: dict[str, bytes | PaperFetchError] | None = None,
    ) -> None:
        self.json_routes = json_routes or {}
        self.text_routes = text_routes or {}
        self.download_payloads = download_payloads or {}
        self.calls: list[tuple[str, str]] = []

    @staticmethod
    def _match(routes: dict[str, Any], url: str) -> Any:
        for needle, value in routes.items():
            if needle in url:
                if isinstance(value, Exception):
                    raise value
                return copy.deepcopy(value)
        raise AssertionError(f"No fake route for {url}")

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("json", url))
        return self._match(self.json_routes, url)

    def get_text(self, url: str, **kwargs: Any) -> str:
        self.calls.append(("text", url))
        return self._match(self.text_routes, url)

    def download_to(self, url: str, destination: Path, **kwargs: Any) -> int:
        self.calls.append(("download", url))
        payload = self._match(self.download_payloads, url)
        destination.write_bytes(payload)
        return len(payload)


def arxiv_atom(title: str = "Example paper") -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>{title}</title>
    <published>2024-01-02T00:00:00Z</published>
    <author><name>Alice Example</name></author>
  </entry>
</feed>"""
