from __future__ import annotations

import copy
import io
from pathlib import Path
from typing import Any

from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

from paper_fetch.errors import PaperFetchError


def _pdf_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _write_page_text(writer: PdfWriter, page: Any, lines: tuple[str, ...]) -> None:
    if not lines:
        return
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject(
                {NameObject("/F1"): writer._add_object(font)}
            )
        }
    )
    commands = ["BT /F1 12 Tf 14 TL 72 720 Td"]
    for index, line in enumerate(lines):
        commands.append(f"({_pdf_text(line)}) Tj")
        if index < len(lines) - 1:
            commands.append("T*")
    commands.append("ET")
    contents = DecodedStreamObject()
    contents.set_data(" ".join(commands).encode("latin-1"))
    page[NameObject("/Contents")] = writer._add_object(contents)


def make_pdf_bytes(
    *,
    doi: str | None = "10.1234/example",
    title: str | None = "Example paper",
    author: str | None = "Alice Example",
    year: int | str | None = 2024,
    subject: str | None = None,
    first_page_lines: tuple[str, ...] = (),
    additional_pages: tuple[tuple[str, ...], ...] = (),
    include_document_metadata: bool = True,
) -> bytes:
    buffer = io.BytesIO()
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    if include_document_metadata:
        metadata = {
            key: str(value)
            for key, value in {
                "/DOI": doi,
                "/Title": title,
                "/Author": author,
                "/Year": year,
                "/Subject": subject,
            }.items()
            if value not in (None, "")
        }
        if metadata:
            writer.add_metadata(metadata)
    _write_page_text(writer, page, first_page_lines)
    for lines in additional_pages:
        _write_page_text(writer, writer.add_blank_page(width=612, height=792), lines)
    writer.write(buffer)
    return buffer.getvalue()


PDF_BYTES = make_pdf_bytes(
    doi="10.1234/example",
    title="Example paper",
    author="Alice Example",
    year=2024,
)


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
