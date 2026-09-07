from __future__ import annotations

import importlib
import re
import urllib.parse
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .errors import PaperFetchError
from .models import PaperMetadata
from .normalize import normalize_doi, normalize_title, title_similarity
from .sanitize import sanitize_data


IDENTITY_SCHEMA = "academic-paper-download.identity.v1"
MAX_METADATA_VALUE_CHARS = 2048
MAX_FIRST_PAGE_ENCODED_BYTES = 2 * 1024 * 1024
MAX_FIRST_PAGE_CONTENT_BYTES = 2 * 1024 * 1024
MAX_FIRST_PAGE_TEXT_CHARS = 64 * 1024
MAX_FIRST_PAGE_LINES = 80
MAX_IDENTITY_HEADER_LINES = 24
MAX_FIRST_PAGE_CONTENT_STREAMS = 256
MAX_OBSERVED_DOIS = 16
TITLE_MATCH_MIN = 0.86
TITLE_MISMATCH_MAX = 0.45
AUTHOR_MATCH_MIN = 0.72

DOI_CANDIDATE_RE = re.compile(
    r"(?:(?:https?://)?(?:dx\.)?doi\.org/|doi:\s*)?10\.\d{4,9}/[^\s\"<>]+",
    re.IGNORECASE,
)
PRIMARY_DOI_LINE_RE = re.compile(
    r"(?:^|\b)(?:(?:https?://)?(?:dx\.)?doi\.org/|(?:doi|digital\s+object\s+identifier)\s*[:\s])",
    re.IGNORECASE,
)
NON_PRIMARY_DOI_LINE_RE = re.compile(
    r"\b(?:background|bibliograph\w*|cit(?:e|ed|es|ing|ation|ations)|references?)\b",
    re.IGNORECASE,
)
EXPLICIT_IDENTIFIER_KEYS = {
    "doi",
    "dc:identifier",
    "prism:doi",
}
TITLE_KEYS = {"title", "dc:title"}
AUTHOR_KEYS = {"author", "creator", "dc:creator"}
YEAR_KEYS = {"year", "publicationyear", "publication_year", "prism:publicationdate"}
CONTEXT_KEYS = {"identifier", "subject", "keywords", "description", "dc:description"}


def _bounded_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip()
    except Exception:
        return None
    return text[:MAX_METADATA_VALUE_CHARS] or None


def _flatten(value: Any) -> Iterable[Any]:
    if value is None:
        return
    if isinstance(value, dict):
        for nested in value.values():
            yield from _flatten(nested)
        return
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            yield from _flatten(nested)
        return
    yield value


def _append_values(target: list[tuple[str, str]], source: str, value: Any) -> None:
    for nested in _flatten(value):
        text = _bounded_text(nested)
        if text and (source, text) not in target:
            target.append((source, text))


def _trim_doi_candidate(value: str) -> str:
    candidate = value.rstrip(".,;:'\"，。；：、")
    pairs = ((")", "("), ("]", "["), ("}", "{"))
    changed = True
    while changed and candidate:
        changed = False
        for closer, opener in pairs:
            if candidate.endswith(closer) and candidate.count(closer) > candidate.count(opener):
                candidate = candidate[:-1]
                changed = True
    return candidate


def _extract_dois(values: Iterable[str]) -> list[str]:
    observed: list[str] = []
    for value in values:
        decoded = urllib.parse.unquote(value)
        for match in DOI_CANDIDATE_RE.finditer(decoded):
            candidate = _trim_doi_candidate(match.group(0))
            try:
                normalized = normalize_doi(candidate)
            except PaperFetchError:
                continue
            if normalized not in observed:
                observed.append(normalized)
            if len(observed) >= MAX_OBSERVED_DOIS:
                return observed
    return observed


def _metadata_evidence(reader: Any, limitations: list[str]) -> dict[str, Any]:
    titles: list[tuple[str, str]] = []
    authors: list[tuple[str, str]] = []
    years: list[tuple[str, str]] = []
    identifier_values: list[str] = []
    context_values: list[str] = []

    try:
        document = reader.metadata
    except Exception:
        document = None
        limitations.append("document_metadata_unreadable")
    if document:
        try:
            items = list(document.items())
        except Exception:
            items = []
            limitations.append("document_metadata_unreadable")
        for raw_key, raw_value in items:
            key = str(raw_key).lstrip("/").casefold()
            text = _bounded_text(raw_value)
            if not text:
                continue
            if key in EXPLICIT_IDENTIFIER_KEYS or key.endswith(":doi"):
                identifier_values.append(text)
            elif key in TITLE_KEYS:
                _append_values(titles, "document_metadata", text)
            elif key in AUTHOR_KEYS:
                _append_values(authors, "document_metadata", text)
            elif key in YEAR_KEYS:
                _append_values(years, "document_metadata", text)
            elif key in CONTEXT_KEYS:
                context_values.append(text)

    try:
        xmp = reader.xmp_metadata
    except Exception:
        xmp = None
        limitations.append("xmp_metadata_unreadable")
    if xmp:
        try:
            identifier = getattr(xmp, "dc_identifier", None)
            if identifier:
                identifier_values.extend(
                    text for value in _flatten(identifier) if (text := _bounded_text(value))
                )
            _append_values(titles, "xmp_metadata", getattr(xmp, "dc_title", None))
            _append_values(authors, "xmp_metadata", getattr(xmp, "dc_creator", None))
            _append_values(years, "xmp_metadata", getattr(xmp, "dc_date", None))
            for attribute in ("dc_description", "dc_subject"):
                for value in _flatten(getattr(xmp, attribute, None)):
                    text = _bounded_text(value)
                    if text:
                        context_values.append(text)
        except Exception:
            limitations.append("xmp_metadata_partially_unreadable")

    return {
        "document_dois": _extract_dois(identifier_values),
        "metadata_context_dois": _extract_dois(context_values),
        "titles": titles,
        "authors": authors,
        "years": years,
    }


def _encoded_content_bytes(page: Any) -> int | None:
    try:
        raw = page.raw_get("/Contents")
    except Exception:
        return None
    pending = [raw]
    total = 0
    streams = 0
    while pending:
        item = pending.pop()
        try:
            stream = item.get_object() if hasattr(item, "get_object") else item
        except Exception:
            return None
        if isinstance(stream, (list, tuple)):
            pending.extend(stream)
            if len(pending) + streams > MAX_FIRST_PAGE_CONTENT_STREAMS:
                return MAX_FIRST_PAGE_ENCODED_BYTES + 1
            continue
        streams += 1
        if streams > MAX_FIRST_PAGE_CONTENT_STREAMS:
            return MAX_FIRST_PAGE_ENCODED_BYTES + 1
        # pypdf exposes only decoded bytes publicly; inspect its encoded payload
        # here so an oversized stream can be rejected before decompression.
        data = getattr(stream, "_data", None)
        if not isinstance(data, (bytes, bytearray)):
            return None
        total += len(data)
        if total > MAX_FIRST_PAGE_ENCODED_BYTES:
            return total
    return total


def _first_page_evidence(reader: Any, limitations: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "examined": True,
        "text": "",
        "header_text": "",
        "encoded_bytes": None,
        "content_bytes": 0,
        "dois": [],
        "primary_dois": [],
    }
    try:
        page = reader.pages[0]
        encoded_bytes = _encoded_content_bytes(page)
        result["encoded_bytes"] = encoded_bytes
        if encoded_bytes is not None and encoded_bytes > MAX_FIRST_PAGE_ENCODED_BYTES:
            limitations.append("first_page_encoded_content_limit_exceeded")
            return result
        contents = page.get_contents()
        if contents is None:
            limitations.append("first_page_has_no_content_stream")
            return result
        content_bytes = len(contents.get_data())
        result["content_bytes"] = content_bytes
        if content_bytes > MAX_FIRST_PAGE_CONTENT_BYTES:
            limitations.append("first_page_content_limit_exceeded")
            return result
        text = page.extract_text() or ""
    except Exception:
        limitations.append("first_page_text_unreadable")
        return result
    if len(text) > MAX_FIRST_PAGE_TEXT_CHARS:
        text = text[:MAX_FIRST_PAGE_TEXT_CHARS]
        limitations.append("first_page_text_truncated")
    result["text"] = text
    lines = text.splitlines()
    result["header_text"] = "\n".join(lines[:MAX_IDENTITY_HEADER_LINES])
    all_dois: list[str] = []
    primary_dois: list[str] = []
    for index, line in enumerate(lines[:MAX_FIRST_PAGE_LINES]):
        line_dois = _extract_dois([line])
        for doi in line_dois:
            if doi not in all_dois:
                all_dois.append(doi)
        if (
            index < MAX_IDENTITY_HEADER_LINES
            and PRIMARY_DOI_LINE_RE.search(line)
            and not NON_PRIMARY_DOI_LINE_RE.search(line)
        ):
            for doi in line_dois:
                if doi not in primary_dois:
                    primary_dois.append(doi)
    result["dois"] = all_dois[:MAX_OBSERVED_DOIS]
    result["primary_dois"] = primary_dois[:MAX_OBSERVED_DOIS]
    if len(all_dois) > 1:
        limitations.append("multiple_first_page_dois")
    return result


def _best_title_check(
    requested: str | None,
    candidates: list[tuple[str, str]],
    first_page_text: str,
) -> dict[str, Any]:
    requested_normalized = normalize_title(requested or "")
    check: dict[str, Any] = {
        "status": "unavailable",
        "requested": requested_normalized or None,
        "observed": None,
        "similarity": None,
        "source": None,
    }
    if not requested_normalized:
        return check
    best: tuple[float, str, str, str] | None = None
    for source, value in candidates:
        normalized = normalize_title(value)
        if not normalized:
            continue
        score = title_similarity(requested, value)
        shorter = min(
            (requested_normalized, normalized),
            key=len,
        )
        if (
            requested_normalized != normalized
            and shorter in max((requested_normalized, normalized), key=len)
            and (len(shorter) >= 24 or len(shorter.split()) >= 4)
        ):
            score = max(score, 0.95)
        if best is None or score > best[0]:
            best = (score, source, value, normalized)
    page_normalized = normalize_title(first_page_text)
    padded_page = f" {page_normalized} "
    padded_requested = f" {requested_normalized} "
    if (
        len(requested_normalized) >= 8
        and padded_requested in padded_page
        and (best is None or best[0] < 1.0)
    ):
        best = (1.0, "first_page", requested, requested_normalized)
    if best is None:
        return check
    score, source, value, normalized = best
    check.update(
        {
            "status": (
                "matched"
                if score >= TITLE_MATCH_MIN
                else "mismatched"
                if score <= TITLE_MISMATCH_MAX
                else "ambiguous"
            ),
            "observed": {
                "value": _bounded_text(value),
                "normalized": normalized,
            },
            "similarity": round(score, 4),
            "source": source,
        }
    )
    return check


def _author_check(
    requested: str | None,
    candidates: list[tuple[str, str]],
    first_page_text: str,
) -> dict[str, Any]:
    requested_normalized = normalize_title(requested or "")
    check: dict[str, Any] = {
        "status": "not_requested" if not requested_normalized else "unavailable",
        "requested": requested_normalized or None,
        "observed": None,
        "source": None,
    }
    if not requested_normalized:
        return check
    requested_parts = requested_normalized.split()
    requested_tokens = set(requested_parts)
    for source, value in candidates:
        normalized = normalize_title(value)
        observed_parts = normalized.split()
        observed_tokens = set(observed_parts)
        surname_match = bool(
            requested_parts
            and observed_parts
            and (
                (len(requested_parts[-1]) > 1 and requested_parts[-1] in observed_tokens)
                or (len(observed_parts[-1]) > 1 and observed_parts[-1] in requested_tokens)
            )
        )
        if (
            requested_normalized in normalized
            or requested_tokens.issubset(observed_tokens)
            or surname_match
            or title_similarity(requested, value) >= AUTHOR_MATCH_MIN
        ):
            check.update(
                {
                    "status": "matched",
                    "observed": {"value": _bounded_text(value), "normalized": normalized},
                    "source": source,
                }
            )
            return check
    page_normalized = normalize_title(first_page_text)
    if requested_normalized and requested_normalized in page_normalized:
        check.update(
            {
                "status": "matched",
                "observed": {"value": requested, "normalized": requested_normalized},
                "source": "first_page",
            }
        )
    elif candidates:
        source, value = candidates[0]
        check.update(
            {
                "status": "mismatched",
                "observed": {
                    "value": _bounded_text(value),
                    "normalized": normalize_title(value),
                },
                "source": source,
            }
        )
    return check


def _year_value(value: str) -> str | None:
    match = re.search(r"(?<!\d)(1[5-9]\d{2}|20\d{2}|21\d{2})(?!\d)", value)
    return match.group(1) if match else None


def _year_check(
    requested: int | str | None,
    candidates: list[tuple[str, str]],
    first_page_text: str,
) -> dict[str, Any]:
    requested_year = _year_value(str(requested)) if requested not in (None, "") else None
    check: dict[str, Any] = {
        "status": "not_requested" if not requested_year else "unavailable",
        "requested": requested_year,
        "observed": None,
        "source": None,
    }
    if not requested_year:
        return check
    observed: list[tuple[str, str]] = []
    for source, value in candidates:
        if year := _year_value(value):
            observed.append((source, year))
            if year == requested_year:
                check.update({"status": "matched", "observed": year, "source": source})
                return check
    if re.search(rf"(?<!\d){re.escape(requested_year)}(?!\d)", first_page_text):
        check.update(
            {"status": "matched", "observed": requested_year, "source": "first_page"}
        )
    elif observed:
        requested_number = int(requested_year)
        nearest = min(observed, key=lambda item: abs(int(item[1]) - requested_number))
        check.update(
            {
                "status": (
                    "ambiguous"
                    if abs(int(nearest[1]) - requested_number) == 1
                    else "mismatched"
                ),
                "observed": nearest[1],
                "source": nearest[0],
            }
        )
    return check


def _identity_payload(
    *,
    requested_doi: str,
    requested_metadata: PaperMetadata,
    metadata: dict[str, Any],
    page: dict[str, Any],
    limitations: list[str],
) -> dict[str, Any]:
    title = _best_title_check(
        requested_metadata.title,
        metadata["titles"],
        page["header_text"],
    )
    author = _author_check(
        requested_metadata.author,
        metadata["authors"],
        page["header_text"],
    )
    year = _year_check(
        requested_metadata.year,
        metadata["years"],
        page["header_text"],
    )
    return {
        "schema_version": IDENTITY_SCHEMA,
        "status": "unresolved",
        "method": None,
        "confidence": 0.0,
        "requested": {
            "doi": requested_doi,
            "title": normalize_title(requested_metadata.title or "") or None,
            "author": normalize_title(requested_metadata.author or "") or None,
            "year": _year_value(str(requested_metadata.year or "")),
        },
        "observed": {
            "document_dois": metadata["document_dois"],
            "metadata_context_dois": metadata["metadata_context_dois"],
            "first_page_dois": page["dois"],
            "first_page_primary_dois": page["primary_dois"],
            "first_page_examined": page["examined"],
            "first_page_encoded_bytes": page["encoded_bytes"],
            "first_page_content_bytes": page["content_bytes"],
        },
        "checks": {
            "doi": {
                "status": "unavailable",
                "requested": requested_doi,
                "document": metadata["document_dois"],
                "metadata_context": metadata["metadata_context_dois"],
                "first_page": page["dois"],
                "first_page_primary": page["primary_dois"],
            },
            "title": title,
            "author": author,
            "year": year,
        },
        "limits": {
            "metadata_value_chars": MAX_METADATA_VALUE_CHARS,
            "first_page_encoded_bytes": MAX_FIRST_PAGE_ENCODED_BYTES,
            "first_page_content_bytes": MAX_FIRST_PAGE_CONTENT_BYTES,
            "first_page_text_chars": MAX_FIRST_PAGE_TEXT_CHARS,
            "first_page_lines": MAX_FIRST_PAGE_LINES,
            "identity_header_lines": MAX_IDENTITY_HEADER_LINES,
            "first_page_content_streams": MAX_FIRST_PAGE_CONTENT_STREAMS,
            "observed_dois": MAX_OBSERVED_DOIS,
        },
        "limitations": sorted(set(limitations)),
    }


def _matched(identity: dict[str, Any], method: str, confidence: float) -> dict[str, Any]:
    identity["status"] = "matched"
    identity["method"] = method
    identity["confidence"] = confidence
    if method.startswith("doi_"):
        identity["checks"]["doi"]["status"] = "matched"
    elif any(
        identity["checks"]["doi"][name]
        for name in ("metadata_context", "first_page")
    ):
        identity["checks"]["doi"]["status"] = "non_primary_only"
    identity["limitations"] = sorted(set(identity["limitations"]))
    return sanitize_data(identity)


def _reject(identity: dict[str, Any], code: str, message: str) -> None:
    identity["status"] = "mismatched" if code == "pdf_identity_mismatch" else "unresolved"
    identity["limitations"] = sorted(set(identity["limitations"]))
    raise PaperFetchError(code, message, retryable=False, identity=identity)


def validate_pdf_identity(
    path: Path,
    doi: str,
    metadata: PaperMetadata,
) -> dict[str, Any]:
    requested_doi = normalize_doi(doi)
    limitations = ["no_ocr", "first_page_only"]
    try:
        pdf_module = importlib.import_module("pypdf")
    except ImportError as exc:
        raise PaperFetchError(
            "pdf_validator_unavailable",
            "PDF identity validation requires pypdf; run runtime.py bootstrap --locked or satisfy pyproject.toml when embedding",
            retryable=False,
            path=str(path),
        ) from exc
    try:
        reader = pdf_module.PdfReader(str(path), strict=False)
        if reader.is_encrypted and not reader.decrypt(""):
            raise ValueError("encrypted PDF cannot be opened without a password")
    except Exception as exc:
        raise PaperFetchError(
            "pdf_identity_unresolved",
            "Downloaded PDF identity could not be read after structural validation",
            retryable=False,
            path=str(path),
        ) from exc

    try:
        extracted_metadata = _metadata_evidence(reader, limitations)
        document_dois = extracted_metadata["document_dois"]
        if document_dois:
            page = {
                "examined": False,
                "text": "",
                "header_text": "",
                "encoded_bytes": None,
                "content_bytes": 0,
                "dois": [],
                "primary_dois": [],
            }
            identity = _identity_payload(
                requested_doi=requested_doi,
                requested_metadata=metadata,
                metadata=extracted_metadata,
                page=page,
                limitations=limitations,
            )
            if requested_doi in document_dois:
                if len(document_dois) > 1:
                    identity["limitations"].append("multiple_document_dois")
                return _matched(identity, "doi_document_metadata", 1.0)

        page = _first_page_evidence(reader, limitations)
        identity = _identity_payload(
            requested_doi=requested_doi,
            requested_metadata=metadata,
            metadata=extracted_metadata,
            page=page,
            limitations=limitations,
        )
        primary_dois = page["primary_dois"]
        if requested_doi in primary_dois:
            if document_dois and requested_doi not in document_dois:
                identity["limitations"].append("document_doi_conflicts_with_first_page")
                return _matched(identity, "doi_first_page", 0.9)
            return _matched(identity, "doi_first_page", 1.0)
        if document_dois:
            identity["checks"]["doi"]["status"] = "mismatched"
            _reject(
                identity,
                "pdf_identity_mismatch",
                "Downloaded PDF document DOI does not match the requested DOI",
            )
        if primary_dois:
            identity["checks"]["doi"]["status"] = "mismatched"
            _reject(
                identity,
                "pdf_identity_mismatch",
                "Downloaded PDF primary first-page DOI does not match the requested DOI",
            )

        title_check = identity["checks"]["title"]
        support_mismatch = any(
            identity["checks"][name]["status"] == "mismatched"
            for name in ("author", "year")
        )
        positive_support = any(
            identity["checks"][name]["status"] == "matched"
            for name in ("author", "year")
        )
        first_page_title_without_support = (
            title_check["source"] == "first_page" and not positive_support
        )
        if (
            title_check["status"] == "matched"
            and not support_mismatch
            and not first_page_title_without_support
        ):
            if page["dois"] or extracted_metadata["metadata_context_dois"]:
                identity["limitations"].append("non_primary_dois_ignored")
            identity["limitations"].append("doi_not_found_title_fallback")
            method = (
                "title_first_page"
                if title_check["source"] == "first_page"
                else "title_document_metadata"
            )
            return _matched(identity, method, float(title_check["similarity"] or 0.0))
        if title_check["status"] == "mismatched" or support_mismatch:
            _reject(
                identity,
                "pdf_identity_mismatch",
                "Downloaded PDF title, author, or year does not match the requested paper",
            )
        _reject(
            identity,
            "pdf_identity_unresolved",
            "Downloaded PDF identity could not be verified from document metadata or bounded first-page text",
        )
    finally:
        close = getattr(reader, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
