#!/usr/bin/env python3
"""Run a network-free structural and identity PDF smoke inside the locked runtime."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any


def _deny_network(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("network access is disabled during the smoke test")


class _OfflineSocket(socket.socket):
    def connect(self, *args: Any, **kwargs: Any) -> None:
        _deny_network(*args, **kwargs)

    def connect_ex(self, *args: Any, **kwargs: Any) -> int:
        _deny_network(*args, **kwargs)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if os.environ.get("ACADEMIC_PAPER_DOWNLOAD_OFFLINE") != "1":
        raise SystemExit("smoke_test.py must be invoked by runtime.py smoke --offline")

    socket.create_connection = _deny_network
    socket.getaddrinfo = _deny_network
    socket.socket = _OfflineSocket
    pypdf = importlib.import_module("pypdf")
    artifact = importlib.import_module("paper_fetch.artifact")
    identity_validator = importlib.import_module("paper_fetch.identity")
    models = importlib.import_module("paper_fetch.models")
    expected = os.environ.get("ACADEMIC_PAPER_DOWNLOAD_EXPECTED_PYPDF", "")
    installed = importlib.metadata.version("pypdf")
    if installed != expected:
        raise SystemExit(
            f"locked pypdf mismatch: expected {expected}, found {installed}"
        )

    with tempfile.TemporaryDirectory(
        prefix="academic-paper-download-smoke-"
    ) as temporary:
        pdf = Path(temporary) / "smoke.pdf"
        writer = pypdf.PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.add_metadata(
            {
                "/DOI": "10.1234/smoke",
                "/Title": "Academic Paper Download Smoke",
                "/Author": "Tiangong AI",
                "/Year": "2026",
            }
        )
        with pdf.open("wb") as handle:
            writer.write(handle)
        size, digest = artifact.validate_pdf(pdf)
        identity = identity_validator.validate_pdf_identity(
            pdf,
            "10.1234/smoke",
            models.PaperMetadata(
                title="Academic Paper Download Smoke",
                author="Tiangong AI",
                year=2026,
            ),
        )

    payload = {
        "ok": True,
        "data": {
            "schema_version": "academic-paper-download.smoke.v1",
            "python": sys.version.split()[0],
            "pypdf": installed,
            "dependency_mode": "locked",
            "network_used": False,
            "pdf_pages": 1,
            "pdf_size": size,
            "sha256": digest,
            "identity_status": identity["status"],
        },
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("Academic Paper Download smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
