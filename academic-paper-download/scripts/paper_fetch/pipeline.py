from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .artifact import ArtifactStore
from .errors import PaperFetchError
from .models import Candidate, ChannelResolution, PaperMetadata
from .normalize import normalize_doi
from .resolvers import OpenAccessResolvers
from .sanitize import sanitize_data, sanitize_url


Progress = Callable[[str, dict[str, Any]], None]
SOURCE_ORDER = ("unpaywall", "semantic_scholar", "arxiv", "browser_handoff")


def _noop_progress(event: str, fields: dict[str, Any]) -> None:
    return None


class PaperFetcher:
    """Resolve and download through legal open-access sources in fixed order."""

    def __init__(
        self,
        resolvers: OpenAccessResolvers,
        artifacts: ArtifactStore,
        *,
        progress: Progress = _noop_progress,
    ) -> None:
        self.resolvers = resolvers
        self.artifacts = artifacts
        self.progress = progress

    def _emit(self, event: str, **fields: Any) -> None:
        self.progress(event, sanitize_data(fields))

    def fetch(
        self,
        raw_doi: str,
        *,
        timeout: float,
        dry_run: bool = False,
        seed_metadata: PaperMetadata | None = None,
    ) -> dict[str, Any]:
        try:
            doi = normalize_doi(raw_doi)
        except PaperFetchError as exc:
            return self._failure(raw_doi.strip(), [], PaperMetadata(), exc)
        metadata = seed_metadata or PaperMetadata()
        sources_tried: list[str] = []
        attempts: list[dict[str, Any]] = []
        external_ids: dict[str, str] = {}
        self._emit("start", doi=doi)

        def resolve_and_attempt(
            source: str,
            resolver: Callable[[], ChannelResolution],
        ) -> dict[str, Any] | None:
            sources_tried.append(source)
            self._emit("source_try", doi=doi, source=source)
            try:
                resolution = resolver()
            except PaperFetchError as exc:
                attempts.append({"source": source, "stage": "resolve", "error": exc.as_dict()})
                self._emit("source_error", doi=doi, source=source, code=exc.code)
                return None
            metadata.merge(resolution.metadata)
            external_ids.update(resolution.external_ids)
            candidate = resolution.candidate
            if candidate is None:
                self._emit("source_miss", doi=doi, source=source)
                return None
            self._emit("source_hit", doi=doi, source=source, pdf_url=sanitize_url(candidate.url))
            if dry_run:
                return self._success(
                    doi,
                    candidate,
                    metadata,
                    sources_tried,
                    {"dry_run": True, "file": None, "manifest": None, "sha256": None, "size": None},
                )
            try:
                artifact = self.artifacts.save(doi, candidate, metadata, timeout=timeout)
            except PaperFetchError as exc:
                attempts.append(
                    {
                        "source": source,
                        "stage": "download",
                        "url": sanitize_url(candidate.url),
                        "error": exc.as_dict(),
                    }
                )
                self._emit("download_error", doi=doi, source=source, code=exc.code)
                if exc.code in {
                    "manifest_write_error",
                    "output_dir_error",
                    "pdf_validator_unavailable",
                }:
                    return self._failure(doi, sources_tried, metadata, exc)
                return None
            self._emit("download_ok", doi=doi, source=source, file=artifact["file"])
            return self._success(doi, candidate, metadata, sources_tried, artifact)

        if self.resolvers.unpaywall_email:
            result = resolve_and_attempt(
                "unpaywall",
                lambda: self.resolvers.unpaywall(doi, timeout=timeout),
            )
            if result:
                return result
        else:
            self._emit("source_skip", doi=doi, source="unpaywall", reason="UNPAYWALL_EMAIL not set")

        result = resolve_and_attempt(
            "semantic_scholar",
            lambda: self.resolvers.semantic_scholar(doi, timeout=timeout),
        )
        if result:
            return result

        result = resolve_and_attempt(
            "arxiv",
            lambda: self.resolvers.arxiv(doi, external_ids, timeout=timeout),
        )
        if result:
            return result

        retryable = any(
            bool((attempt.get("error") or {}).get("retryable")) for attempt in attempts
        )
        error = PaperFetchError(
            "download_unresolved" if attempts else "not_found",
            "No configured source produced a verified PDF",
            retryable=retryable,
            attempts=attempts,
        )
        return self._failure(doi, sources_tried, metadata, error, browser_handoff=True)

    @staticmethod
    def _success(
        doi: str,
        candidate: Candidate,
        metadata: PaperMetadata,
        sources_tried: list[str],
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        committed_manifest = artifact.get("committed_manifest") or {}
        clean_artifact = {
            key: value for key, value in artifact.items() if key != "committed_manifest"
        }
        source = committed_manifest.get("source") or candidate.source
        source_url = committed_manifest.get("source_url") or sanitize_url(candidate.url)
        source_detail = committed_manifest.get("source_detail") or candidate.detail or None
        access_fields = {
            key: committed_manifest.get(key, getattr(candidate, key))
            for key in (
                "access_basis",
                "license_status",
                "license",
                "license_url",
                "host_type",
                "article_version",
            )
        }
        return sanitize_data({
            "doi": doi,
            "success": True,
            "source": source,
            "source_url": source_url,
            "source_detail": source_detail,
            "meta": metadata.as_dict(),
            "sources_tried": list(sources_tried),
            **access_fields,
            "retrieved_at": committed_manifest.get("retrieved_at"),
            **clean_artifact,
        })

    @staticmethod
    def _failure(
        doi: str,
        sources_tried: list[str],
        metadata: PaperMetadata,
        error: PaperFetchError,
        *,
        browser_handoff: bool = False,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "doi": doi,
            "success": False,
            "source": None,
            "source_url": None,
            "file": None,
            "manifest": None,
            "size": None,
            "sha256": None,
            "meta": metadata.as_dict(),
            "sources_tried": list(sources_tried),
            "error": error.as_dict(),
        }
        if browser_handoff:
            result["browser_handoff"] = {
                "doi_url": f"https://doi.org/{doi}",
                "title": metadata.title,
                "reason": "Automated sources were exhausted; continue with the exact-download browser handoff.",
            }
        return sanitize_data(result)
