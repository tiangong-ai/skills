from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import PaperFetchError
from .http import PaperTransport
from .models import Candidate, PaperMetadata
from .normalize import normalize_doi
from .sanitize import sanitize_data


MANIFEST_SCHEMA = "academic-paper-download.artifact.v2"
DEFAULT_MAX_BYTES = 100 * 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_pdf(path: Path, max_bytes: int = DEFAULT_MAX_BYTES) -> tuple[int, str]:
    size = path.stat().st_size
    if size <= 0:
        raise PaperFetchError("invalid_pdf", "Downloaded PDF is empty", path=str(path))
    if size > max_bytes:
        raise PaperFetchError(
            "download_size_exceeded",
            f"PDF exceeds size limit: {size} > {max_bytes}",
            path=str(path),
        )
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise PaperFetchError("invalid_pdf", "Downloaded file does not start with %PDF-", path=str(path))
        handle.seek(max(0, size - 8192))
        if b"%%EOF" not in handle.read():
            raise PaperFetchError("invalid_pdf", "Downloaded PDF has no final %%EOF marker", path=str(path))
    try:
        pdf_module = importlib.import_module("pypdf")
    except ImportError as exc:
        raise PaperFetchError(
            "pdf_validator_unavailable",
            "PDF structural validation requires pypdf; install academic-paper-download/requirements.txt",
            retryable=False,
            path=str(path),
        ) from exc
    try:
        reader = pdf_module.PdfReader(str(path), strict=False)
        if reader.is_encrypted:
            decrypt_result = reader.decrypt("")
            if not decrypt_result:
                raise ValueError("encrypted PDF cannot be opened without a password")
        page_count = len(reader.pages)
        if page_count < 1:
            raise ValueError("PDF contains no pages")
        reader.pages[0].mediabox
    except Exception as exc:
        raise PaperFetchError(
            "invalid_pdf",
            f"PDF parser rejected the downloaded file: {exc}",
            path=str(path),
        ) from exc
    return size, sha256_file(path)


def manifest_path(pdf_path: Path) -> Path:
    return pdf_path.with_name(pdf_path.name + ".json")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(sanitize_data(payload), handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _slug(value: str, byte_limit: int) -> str:
    value = unicodedata.normalize("NFKC", value)
    value = "".join(char if char.isalnum() else "_" for char in value)
    value = re.sub(r"_+", "_", value).strip("_") or "paper"
    while len(value.encode("utf-8")) > byte_limit:
        value = value[:-1]
    return value.rstrip("_") or "paper"


def filename_for(metadata: PaperMetadata, doi: str) -> str:
    author = (metadata.author or "unknown").split(";")[0].strip().split()[-1]
    year = str(metadata.year or "nd")
    title = metadata.title or doi
    return f"{_slug(author, 40)}_{_slug(year, 12)}_{_slug(title, 110)}.pdf"


def _matching_manifest(path: Path, doi: str) -> dict[str, Any] | None:
    sidecar = manifest_path(path)
    if path.is_symlink() or sidecar.is_symlink() or not path.is_file() or not sidecar.is_file():
        return None
    try:
        manifest = json.loads(sidecar.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != MANIFEST_SCHEMA:
            return None
        if normalize_doi(str(manifest.get("doi", ""))) != normalize_doi(doi):
            return None
        if int(manifest.get("size", -1)) != path.stat().st_size:
            return None
        if manifest.get("sha256") != sha256_file(path):
            return None
    except (OSError, ValueError, TypeError, json.JSONDecodeError, PaperFetchError):
        return None
    return manifest


def verify_existing(path: Path, doi: str) -> dict[str, Any] | None:
    return _matching_manifest(path, doi)


def choose_available_path(directory: Path, filename: str) -> Path:
    candidate = directory / filename
    counter = 2
    while candidate.exists() or candidate.is_symlink() or manifest_path(candidate).exists():
        candidate = directory / f"{Path(filename).stem}-{counter}.pdf"
        counter += 1
    return candidate


def build_manifest(
    *,
    doi: str,
    candidate: Candidate,
    metadata: PaperMetadata,
    path: Path,
    size: int,
    digest: str,
    access_mode: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    retrieved_at = datetime.now(timezone.utc).isoformat()
    return sanitize_data({
        "schema_version": MANIFEST_SCHEMA,
        "doi": normalize_doi(doi),
        "title": metadata.title,
        "author": metadata.author,
        "year": metadata.year,
        "journal": metadata.journal,
        "source": candidate.source,
        "source_url": candidate.url,
        "source_detail": candidate.detail or None,
        "access_basis": candidate.access_basis,
        "license_status": candidate.license_status,
        "license": candidate.license,
        "license_url": candidate.license_url,
        "host_type": candidate.host_type,
        "article_version": candidate.article_version,
        "access_mode": access_mode,
        "retrieved_at": retrieved_at,
        "downloaded_at": retrieved_at,
        "file": str(path),
        "size": size,
        "sha256": digest,
        **(extra or {}),
    })


class ArtifactStore:
    def __init__(
        self,
        output_dir: Path,
        transport: PaperTransport,
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> None:
        self.output_dir = output_dir.expanduser().absolute()
        self.transport = transport
        self.max_bytes = max_bytes

    def _ensure_output_directory(self) -> None:
        if self.output_dir.is_symlink():
            raise PaperFetchError(
                "output_dir_error",
                "Output directory must not be a symbolic link",
                retryable=False,
                output_dir=str(self.output_dir),
            )
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PaperFetchError(
                "output_dir_error",
                f"Could not create output directory: {exc}",
                retryable=False,
                output_dir=str(self.output_dir),
            ) from exc
        if not self.output_dir.is_dir():
            raise PaperFetchError(
                "output_dir_error",
                "Output path is not a directory",
                retryable=False,
                output_dir=str(self.output_dir),
            )

    def save(
        self,
        doi: str,
        candidate: Candidate,
        metadata: PaperMetadata,
        *,
        timeout: float,
    ) -> dict[str, Any]:
        self._ensure_output_directory()
        filename = filename_for(metadata, doi)
        preferred = self.output_dir / filename
        existing = verify_existing(preferred, doi)
        if existing is not None:
            return {
                "file": str(preferred),
                "manifest": str(manifest_path(preferred)),
                "size": existing["size"],
                "sha256": existing["sha256"],
                "skipped": True,
                "verified_existing": True,
                "committed_manifest": existing,
            }
        destination = choose_available_path(self.output_dir, filename)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self.output_dir,
            prefix=f".{destination.stem}.",
            suffix=".part",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            self.transport.download_to(
                candidate.url,
                temporary,
                timeout=timeout,
                max_bytes=self.max_bytes,
                headers=candidate.headers or None,
            )
            size, digest = validate_pdf(temporary, self.max_bytes)
            manifest = build_manifest(
                doi=doi,
                candidate=candidate,
                metadata=metadata,
                path=destination,
                size=size,
                digest=digest,
                access_mode="http",
            )
            os.replace(temporary, destination)
            try:
                atomic_write_json(manifest_path(destination), manifest)
            except Exception as exc:
                destination.unlink(missing_ok=True)
                raise PaperFetchError(
                    "manifest_write_error",
                    f"Could not commit artifact manifest: {exc}",
                    retryable=True,
                ) from exc
            return {
                "file": str(destination),
                "manifest": str(manifest_path(destination)),
                "size": size,
                "sha256": digest,
                "skipped": False,
                "verified_existing": False,
                "committed_manifest": manifest,
            }
        finally:
            temporary.unlink(missing_ok=True)
