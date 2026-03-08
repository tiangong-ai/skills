#!/usr/bin/env python3
import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


TEMP_DOWNLOAD_SUFFIXES = {".crdownload", ".part", ".tmp"}


def is_temp_download(path: Path) -> bool:
    return any(path.name.endswith(sfx) for sfx in TEMP_DOWNLOAD_SUFFIXES)


def list_candidate_files(downloads_dir: Path, expected_name: Optional[str]) -> list[Path]:
    if not downloads_dir.exists():
        return []

    files = [
        p
        for p in downloads_dir.iterdir()
        if p.is_file() and not p.name.startswith(".~") and not is_temp_download(p)
    ]

    if expected_name:
        exact = [p for p in files if p.name == expected_name]
        if exact:
            return sorted(exact, key=lambda p: p.stat().st_mtime, reverse=True)

        fuzzy = [p for p in files if expected_name.lower() in p.name.lower()]
        if fuzzy:
            return sorted(fuzzy, key=lambda p: p.stat().st_mtime, reverse=True)

    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def wait_for_download(
    downloads_dir: Path,
    expected_name: Optional[str],
    started_at: float,
    wait_seconds: int,
    poll_interval: float,
) -> Optional[Path]:
    deadline = time.time() + max(wait_seconds, 0)

    while True:
        candidates = list_candidate_files(downloads_dir, expected_name)
        for p in candidates:
            try:
                st = p.stat()
            except FileNotFoundError:
                continue

            # Prefer files created/updated after this script started.
            if st.st_mtime >= started_at and st.st_size > 0:
                return p

        if time.time() >= deadline:
            break

        time.sleep(max(poll_interval, 0.2))

    # Timeout fallback: pick newest matching non-temp file if any.
    fallback = list_candidate_files(downloads_dir, expected_name)
    for p in fallback:
        try:
            if p.stat().st_size > 0:
                return p
        except FileNotFoundError:
            continue
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Figshare browser-assisted standardizer: open link, wait, copy, verify.")
    ap.add_argument("url", help="Figshare DOI/item/ndownloader URL to open in browser")
    ap.add_argument("--output", required=True, help="Final output file path")
    ap.add_argument("--downloads-dir", default=str(Path.home() / "Downloads"), help="Browser downloads directory")
    ap.add_argument("--expected-name", default="", help="Expected downloaded filename")
    ap.add_argument("--open-browser", action="store_true", help="Open URL with system browser automatically")
    ap.add_argument("--wait-seconds", type=int, default=120, help="Max seconds to wait for a new download")
    ap.add_argument("--poll-interval", type=float, default=2.0, help="Polling interval (seconds)")
    args = ap.parse_args()

    started_at = time.time()
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    downloads_dir = Path(args.downloads_dir).expanduser().resolve()

    print("[1/4] Browser-only mode (no curl/web_fetch probing).")
    print("[2/4] Open URL in a real browser, complete any challenge, and trigger download:")
    print(args.url)

    if args.open_browser:
        try:
            subprocess.run(["open", args.url], check=False)
            print("Opened URL in default browser.")
        except Exception as e:
            print(f"Warning: failed to auto-open browser: {e}")

    print(f"[3/4] Waiting up to {args.wait_seconds}s for download in: {downloads_dir}")
    pick = wait_for_download(
        downloads_dir=downloads_dir,
        expected_name=args.expected_name or None,
        started_at=started_at,
        wait_seconds=args.wait_seconds,
        poll_interval=args.poll_interval,
    )
    if not pick:
        print("ERROR: no candidate file found in downloads directory.")
        return 2

    if pick.resolve() != output:
        shutil.copy2(pick, output)

    print("[4/4] Verifying output...")
    if not output.exists() or output.stat().st_size == 0:
        print("ERROR: copied file is empty or missing.")
        return 3

    print(f"OK: browser-assisted download copied -> {output}")
    print(f"Source file: {pick}")
    print(f"Size: {output.stat().st_size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
