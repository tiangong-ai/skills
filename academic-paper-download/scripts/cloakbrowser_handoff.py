#!/usr/bin/env python3
"""Run an explicit, exact-bound CloakBrowser publisher download handoff."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from paper_fetch.artifact import DEFAULT_MAX_BYTES
from paper_fetch.cloakbrowser_handoff import HandoffConfig, LocatorSpec, execute_handoff
from paper_fetch.errors import PaperFetchError
from paper_fetch.sanitize import sanitize_data


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise PaperFetchError("validation_error", message, retryable=False)


def build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--browser-backend", choices=["cloakbrowser"], required=True)
    parser.add_argument("--profile-dir", required=True)
    parser.add_argument("--url", required=True, dest="publisher_url")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--doi", required=True)
    parser.add_argument("--title")
    parser.add_argument("--author")
    parser.add_argument("--year")
    parser.add_argument("--journal")
    parser.add_argument("--filename")
    parser.add_argument("--browser-version")
    locator = parser.add_mutually_exclusive_group(required=True)
    locator.add_argument("--download-text")
    locator.add_argument("--download-test-id")
    locator.add_argument("--download-selector")
    locator.add_argument("--download-role-name")
    parser.add_argument("--download-role", default="link", choices=["link", "button"])
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--license-status", choices=["verified", "declared", "unknown"], default="unknown")
    parser.add_argument("--license")
    parser.add_argument("--license-url")
    parser.add_argument("--host-type")
    parser.add_argument("--article-version")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except PaperFetchError as exc:
        print(json.dumps(sanitize_data({"ok": False, "error": exc.as_dict()}), ensure_ascii=False))
        return 3
    locator = LocatorSpec(
        role=args.download_role if args.download_role_name else None,
        name=args.download_role_name,
        text=args.download_text,
        test_id=args.download_test_id,
        selector=args.download_selector,
    )
    config = HandoffConfig(
        browser_backend=args.browser_backend,
        profile_dir=Path(args.profile_dir),
        publisher_url=args.publisher_url,
        output_dir=Path(args.output_dir),
        doi=args.doi,
        locator=locator,
        title=args.title,
        author=args.author,
        year=args.year,
        journal=args.journal,
        filename=args.filename,
        browser_version=args.browser_version,
        timeout_seconds=args.timeout,
        max_bytes=args.max_bytes,
        license_status=args.license_status,
        license=args.license,
        license_url=args.license_url,
        host_type=args.host_type,
        article_version=args.article_version,
    )
    try:
        payload = execute_handoff(config)
    except PaperFetchError as exc:
        print(json.dumps(sanitize_data({"ok": False, "error": exc.as_dict()}), ensure_ascii=False))
        if exc.code == "human_action_required":
            return 2
        if exc.code.startswith("cloakbrowser_") or exc.code.endswith("_required"):
            return 3
        return 4
    except Exception:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "code": "browser_handoff_internal_error",
                        "message": "The CloakBrowser handoff failed without a safe classified error",
                        "retryable": False,
                    },
                },
                ensure_ascii=False,
            )
        )
        return 4
    print(json.dumps(sanitize_data(payload), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
