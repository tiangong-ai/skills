from __future__ import annotations

import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

from .errors import PaperFetchError
from .http import PaperTransport
from .models import Candidate, ChannelResolution, PaperMetadata
from .normalize import normalize_title, title_similarity


TITLE_SIMILARITY_MIN = 0.86


def _first(value: Any) -> Any:
    return value[0] if isinstance(value, list) and value else None


def _crossref_metadata(item: dict[str, Any]) -> PaperMetadata:
    authors = item.get("author") or []
    first_author = authors[0] if authors else {}
    issued = ((item.get("issued") or {}).get("date-parts") or [[None]])[0]
    return PaperMetadata(
        title=_first(item.get("title")),
        year=issued[0] if issued else None,
        author=first_author.get("family") or first_author.get("name"),
        journal=_first(item.get("container-title")),
    )


def _author_matches(requested: str, candidate: str | None) -> bool:
    if not candidate:
        return False
    requested_norm = normalize_title(requested)
    candidate_norm = normalize_title(candidate)
    return (
        requested_norm in candidate_norm
        or candidate_norm in requested_norm
        or title_similarity(requested, candidate) >= 0.72
    )


def _year_matches(requested: int | str, candidate: Any) -> bool:
    return str(requested).strip() == str(candidate).strip() if candidate not in (None, "") else False


def _deduplicate_title_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        doi = str(candidate.get("doi") or "").strip()
        if not doi:
            continue
        key = doi.casefold()
        resolver = str(candidate.get("resolver") or "unknown")
        if key not in deduplicated:
            deduplicated[key] = {**candidate, "resolvers": [resolver]}
            continue
        existing = deduplicated[key]
        if resolver not in existing["resolvers"]:
            existing["resolvers"].append(resolver)
        for field in ("title", "year", "author", "journal", "score"):
            if existing.get(field) in (None, "") and candidate.get(field) not in (None, ""):
                existing[field] = candidate[field]
        existing["title_similarity"] = max(
            float(existing.get("title_similarity") or 0),
            float(candidate.get("title_similarity") or 0),
        )
    return list(deduplicated.values())


@dataclass
class TitleResolution:
    doi: str | None
    metadata: PaperMetadata
    details: dict[str, Any]


class OpenAccessResolvers:
    def __init__(
        self,
        transport: PaperTransport,
        *,
        unpaywall_email: str = "",
        semantic_scholar_api_key: str = "",
    ) -> None:
        self.transport = transport
        self.unpaywall_email = unpaywall_email.strip()
        self.semantic_scholar_api_key = semantic_scholar_api_key.strip()

    def _s2_headers(self) -> dict[str, str] | None:
        return {"x-api-key": self.semantic_scholar_api_key} if self.semantic_scholar_api_key else None

    def unpaywall(self, doi: str, *, timeout: float) -> ChannelResolution:
        if not self.unpaywall_email:
            return ChannelResolution()
        query = urllib.parse.urlencode({"email": self.unpaywall_email})
        url = f"https://api.unpaywall.org/v2/{urllib.parse.quote(doi, safe='')}?{query}"
        data = self.transport.get_json(url, timeout=timeout)
        authors = data.get("z_authors") or []
        location = data.get("best_oa_location") or {}
        pdf_url = location.get("url_for_pdf")
        license_value = location.get("license") or "unknown"
        license_url = location.get("license_url") or "unknown"
        return ChannelResolution(
            candidate=(
                Candidate(
                    "unpaywall",
                    pdf_url,
                    access_basis="open_access",
                    license_status=(
                        "declared" if license_value != "unknown" or license_url != "unknown" else "unknown"
                    ),
                    license=str(license_value),
                    license_url=str(license_url),
                    host_type=str(location.get("host_type") or "unknown"),
                    article_version=str(location.get("version") or "unknown"),
                    detail={
                        key: value
                        for key, value in {
                            "is_oa": data.get("is_oa"),
                            "oa_status": data.get("oa_status"),
                            "location_url": location.get("url"),
                        }.items()
                        if value is not None
                    },
                )
                if pdf_url
                else None
            ),
            metadata=PaperMetadata(
                title=data.get("title"),
                year=data.get("year"),
                author=(authors[0] or {}).get("family") if authors else None,
                journal=data.get("journal_name"),
            ),
        )

    def semantic_scholar(self, doi: str, *, timeout: float) -> ChannelResolution:
        fields = "title,year,authors,openAccessPdf,isOpenAccess,externalIds,venue"
        url = (
            "https://api.semanticscholar.org/graph/v1/paper/DOI:"
            f"{urllib.parse.quote(doi, safe='')}?fields={fields}"
        )
        data = self.transport.get_json(url, timeout=timeout, headers=self._s2_headers())
        authors = data.get("authors") or []
        oa_pdf = data.get("openAccessPdf") or {}
        pdf_url = oa_pdf.get("url")
        license_value = oa_pdf.get("license") or "unknown"
        return ChannelResolution(
            candidate=(
                Candidate(
                    "semantic_scholar",
                    pdf_url,
                    access_basis="open_access",
                    license_status="declared" if license_value != "unknown" else "unknown",
                    license=str(license_value),
                    detail={
                        key: value
                        for key, value in {
                            "is_open_access": data.get("isOpenAccess"),
                            "oa_status": oa_pdf.get("status"),
                        }.items()
                        if value is not None
                    },
                )
                if pdf_url
                else None
            ),
            metadata=PaperMetadata(
                title=data.get("title"),
                year=data.get("year"),
                author=(authors[0] or {}).get("name") if authors else None,
                journal=data.get("venue"),
            ),
            external_ids={
                str(key): str(value)
                for key, value in (data.get("externalIds") or {}).items()
                if value
            },
        )

    def arxiv(
        self,
        doi: str,
        external_ids: dict[str, str],
        *,
        timeout: float,
    ) -> ChannelResolution:
        arxiv_id = external_ids.get("ArXiv") or external_ids.get("ARXIV")
        if not arxiv_id and doi.casefold().startswith("10.48550/arxiv."):
            arxiv_id = doi[len("10.48550/arxiv.") :]
        if not arxiv_id:
            return ChannelResolution()
        bare_id = arxiv_id.rsplit("v", 1)[0] if arxiv_id.rsplit("v", 1)[-1].isdigit() else arxiv_id
        metadata = PaperMetadata()
        license_url = "unknown"
        try:
            api_url = "https://export.arxiv.org/api/query?" + urllib.parse.urlencode({"id_list": bare_id})
            xml = self.transport.get_text(api_url, timeout=timeout)
            root = ET.fromstring(xml)
            namespace = {"atom": "http://www.w3.org/2005/Atom"}
            entry = root.find("atom:entry", namespace)
            if entry is not None:
                title = (entry.findtext("atom:title", default="", namespaces=namespace) or "").strip()
                published = entry.findtext("atom:published", default="", namespaces=namespace) or ""
                metadata = PaperMetadata(
                    title=title or None,
                    year=int(published[:4]) if published[:4].isdigit() else None,
                    author=entry.findtext("atom:author/atom:name", default=None, namespaces=namespace),
                )
                for link in entry.findall("atom:link", namespace):
                    if (link.get("rel") or "").casefold() == "license" and link.get("href"):
                        license_url = str(link.get("href"))
                        break
        except (PaperFetchError, ET.ParseError):
            pass
        return ChannelResolution(
            candidate=Candidate(
                "arxiv",
                f"https://arxiv.org/pdf/{arxiv_id}.pdf",
                access_basis="open_access",
                license_status="declared" if license_url != "unknown" else "unknown",
                license_url=license_url,
                host_type="repository",
                article_version="unknown",
            ),
            metadata=metadata,
            external_ids={"ArXiv": arxiv_id},
        )

    def resolve_title(
        self,
        title: str,
        *,
        timeout: float,
        author: str | None = None,
        year: int | str | None = None,
    ) -> TitleResolution:
        query = title.strip()
        if len(query) < 6:
            raise PaperFetchError("validation_error", "Paper title is too short", field="title")
        crossref_url = "https://api.crossref.org/works?" + urllib.parse.urlencode(
            {
                "query.title": query,
                "rows": "5",
                "select": "DOI,title,author,issued,container-title,score",
                **({"mailto": self.unpaywall_email} if self.unpaywall_email else {}),
            }
        )
        candidates: list[dict[str, Any]] = []
        resolution_errors: list[dict[str, Any]] = []
        try:
            data = self.transport.get_json(crossref_url, timeout=timeout)
            for item in ((data.get("message") or {}).get("items") or [])[:5]:
                metadata = _crossref_metadata(item)
                candidate = {
                    "doi": item.get("DOI"),
                    **metadata.as_dict(),
                    "score": item.get("score"),
                    "title_similarity": title_similarity(query, metadata.title or ""),
                    "resolver": "crossref",
                }
                candidates.append(candidate)
        except PaperFetchError as exc:
            resolution_errors.append({"resolver": "crossref", "error": exc.as_dict()})

        s2_url = "https://api.semanticscholar.org/graph/v1/paper/search/match?" + urllib.parse.urlencode(
            {"query": query, "fields": "title,authors,year,venue,externalIds"}
        )
        try:
            data = self.transport.get_json(s2_url, timeout=timeout, headers=self._s2_headers())
        except PaperFetchError as exc:
            resolution_errors.append({"resolver": "semantic_scholar", "error": exc.as_dict()})
            data = {}
        items = data.get("data") or []
        if items:
            top = items[0]
            external_ids = top.get("externalIds") or {}
            doi = external_ids.get("DOI")
            if not doi and external_ids.get("ArXiv"):
                doi = f"10.48550/arXiv.{external_ids['ArXiv']}"
            similarity = title_similarity(query, top.get("title") or "")
            if doi:
                authors = top.get("authors") or []
                metadata = PaperMetadata(
                    title=top.get("title"),
                    year=top.get("year"),
                    author=(authors[0] or {}).get("name") if authors else None,
                    journal=top.get("venue"),
                )
                candidates.append(
                    {
                        "doi": doi,
                        **metadata.as_dict(),
                        "title_similarity": similarity,
                        "resolver": "semantic_scholar",
                    }
                )

        candidates = _deduplicate_title_candidates(candidates)
        strong = [
            candidate
            for candidate in candidates
            if float(candidate.get("title_similarity") or 0) >= TITLE_SIMILARITY_MIN
        ]
        if author:
            strong = [candidate for candidate in strong if _author_matches(author, candidate.get("author"))]
        if year is not None:
            strong = [candidate for candidate in strong if _year_matches(year, candidate.get("year"))]

        qualifiers = {
            key: value
            for key, value in {"author": author, "year": year}.items()
            if value not in (None, "")
        }
        if len(strong) > 1:
            raise PaperFetchError(
                "title_ambiguous",
                "Multiple papers match this title; add --author and/or --year or choose a DOI",
                retryable=False,
                query=query,
                qualifiers=qualifiers,
                candidates=strong,
            )
        if len(strong) == 1:
            selected = strong[0]
            return TitleResolution(
                str(selected["doi"]),
                PaperMetadata(
                    title=selected.get("title"),
                    year=selected.get("year"),
                    author=selected.get("author"),
                    journal=selected.get("journal"),
                ),
                {
                    "query": query,
                    "qualifiers": qualifiers,
                    "resolver": selected.get("resolver"),
                    "confirmed_by": selected.get("resolvers"),
                    "selected": selected,
                    "candidates": candidates,
                    "resolution_errors": resolution_errors,
                    "low_confidence": False,
                },
            )

        if not candidates and any(
            bool((attempt.get("error") or {}).get("retryable")) for attempt in resolution_errors
        ):
            raise PaperFetchError(
                "title_resolution_transport",
                "Title resolvers were unavailable",
                retryable=True,
                query=query,
                attempts=resolution_errors,
            )
        raise PaperFetchError(
            "title_low_confidence",
            "No title match was confident enough to download automatically",
            retryable=False,
            query=query,
            qualifiers=qualifiers,
            candidates=candidates,
            resolution_errors=resolution_errors,
            threshold=TITLE_SIMILARITY_MIN,
        )
