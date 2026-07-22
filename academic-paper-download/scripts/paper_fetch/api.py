from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .artifact import ArtifactStore
from .errors import PaperFetchError
from .http import HttpClient, PaperTransport
from .models import FetchRequest, PaperMetadata
from .pipeline import PaperFetcher, Progress, _noop_progress
from .resolvers import OpenAccessResolvers
from .sanitize import sanitize_data


def _request(value: FetchRequest | Mapping[str, Any]) -> FetchRequest:
    if isinstance(value, FetchRequest):
        return value
    if isinstance(value, Mapping):
        try:
            return FetchRequest(**dict(value))
        except TypeError as exc:
            raise PaperFetchError("validation_error", f"Invalid fetch request: {exc}") from exc
    raise PaperFetchError("validation_error", "request must be a FetchRequest or mapping")


def _validation_failure(identifier: str, error: PaperFetchError) -> dict[str, Any]:
    return sanitize_data(
        {
            "doi": identifier,
            "success": False,
            "source": None,
            "source_url": None,
            "file": None,
            "manifest": None,
            "size": None,
            "sha256": None,
            "meta": {},
            "sources_tried": [],
            "error": error.as_dict(),
        }
    )


def fetch_paper(
    request: FetchRequest | Mapping[str, Any],
    *,
    transport: PaperTransport | None = None,
    progress: Progress | None = None,
) -> dict[str, Any]:
    """Resolve and atomically fetch one paper using an injectable transport."""

    try:
        resolved_request = _request(request)
    except PaperFetchError as exc:
        return _validation_failure("", exc)
    selected = sum(bool(value and str(value).strip()) for value in (resolved_request.doi, resolved_request.title))
    identifier = str(resolved_request.doi or resolved_request.title or "").strip()
    if selected != 1:
        return _validation_failure(
            identifier,
            PaperFetchError("validation_error", "Pass exactly one of doi or title"),
        )
    if not str(resolved_request.output_dir).strip():
        return _validation_failure(
            identifier,
            PaperFetchError("validation_error", "output_dir is required"),
        )
    if resolved_request.timeout <= 0:
        return _validation_failure(
            identifier,
            PaperFetchError("validation_error", "timeout must be positive"),
        )
    if (resolved_request.author or resolved_request.year is not None) and not resolved_request.title:
        return _validation_failure(
            identifier,
            PaperFetchError("validation_error", "author and year require title"),
        )

    active_transport = transport or HttpClient(user_agent="academic-paper-download-library")
    if not isinstance(active_transport, PaperTransport):
        return _validation_failure(
            identifier,
            PaperFetchError(
                "validation_error",
                "transport must implement get_json, get_text, and download_to",
            ),
        )
    resolvers = OpenAccessResolvers(
        active_transport,
        unpaywall_email=resolved_request.unpaywall_email,
        semantic_scholar_api_key=resolved_request.semantic_scholar_api_key,
    )
    fetcher = PaperFetcher(
        resolvers,
        ArtifactStore(Path(resolved_request.output_dir), active_transport),
        progress=progress or _noop_progress,
    )
    seed = PaperMetadata()
    doi = resolved_request.doi
    title_resolution: dict[str, Any] | None = None
    if resolved_request.title:
        try:
            resolution = resolvers.resolve_title(
                resolved_request.title,
                timeout=resolved_request.timeout,
                author=resolved_request.author,
                year=resolved_request.year,
            )
        except PaperFetchError as exc:
            return _validation_failure(identifier, exc)
        doi = resolution.doi
        seed = resolution.metadata
        title_resolution = resolution.details
    assert doi is not None
    result = fetcher.fetch(
        doi,
        timeout=resolved_request.timeout,
        dry_run=resolved_request.dry_run,
        seed_metadata=seed,
    )
    if title_resolution is not None:
        result["title_resolution"] = sanitize_data(title_resolution)
    return sanitize_data(result)
