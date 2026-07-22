from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Candidate:
    source: str
    url: str
    access_basis: str = "unknown"
    license_status: str = "unknown"
    license: str = "unknown"
    license_url: str = "unknown"
    host_type: str = "unknown"
    article_version: str = "unknown"
    detail: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class FetchRequest:
    output_dir: str | Path
    doi: str | None = None
    title: str | None = None
    author: str | None = None
    year: int | str | None = None
    timeout: float = 30.0
    dry_run: bool = False
    unpaywall_email: str = ""
    semantic_scholar_api_key: str = ""


@dataclass
class PaperMetadata:
    title: str | None = None
    year: int | str | None = None
    author: str | None = None
    journal: str | None = None

    def merge(self, other: "PaperMetadata | None") -> None:
        if other is None:
            return
        for name in ("title", "year", "author", "journal"):
            if not getattr(self, name) and getattr(other, name):
                setattr(self, name, getattr(other, name))

    def as_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "title": self.title,
                "year": self.year,
                "author": self.author,
                "journal": self.journal,
            }.items()
            if value not in (None, "")
        }


@dataclass
class ChannelResolution:
    candidate: Candidate | None = None
    metadata: PaperMetadata = field(default_factory=PaperMetadata)
    external_ids: dict[str, str] = field(default_factory=dict)
