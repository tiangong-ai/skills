#!/usr/bin/env python3
"""Show a persistent macOS dialog when paper download needs human action."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys


DEFAULT_TIMEOUT_SECONDS = 3600
DIALOG_STARTUP_GRACE_SECONDS = 0.25
OSASCRIPT = "/usr/bin/osascript"

APPLESCRIPT = r'''
on run argv
    set dialogTitle to item 1 of argv
    set dialogMessage to item 2 of argv
    set buttonLabel to item 3 of argv
    set timeoutSeconds to (item 4 of argv) as integer
    beep 2
    if timeoutSeconds > 0 then
        display dialog dialogMessage with title dialogTitle buttons {buttonLabel} default button buttonLabel with icon caution giving up after timeoutSeconds
    else
        display dialog dialogMessage with title dialogTitle buttons {buttonLabel} default button buttonLabel with icon caution
    end if
end run
'''.strip()


def emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def failure_payload(
    payload: dict,
    *,
    code: str,
    message: str,
    details: dict | None = None,
) -> dict:
    return {
        "ok": False,
        "error": {"code": code, "message": message, **(details or {})},
        "data": {
            **payload,
            "shown": False,
            "chat_fallback_required": True,
            "chat_fallback_message": payload["message"],
        },
    }


def command(args: argparse.Namespace) -> list[str]:
    return [
        OSASCRIPT,
        "-e",
        APPLESCRIPT,
        args.title,
        args.message,
        args.button,
        str(args.timeout),
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show a persistent macOS modal dialog for required human action."
    )
    parser.add_argument("--title", required=True, help="Localized dialog title")
    parser.add_argument("--message", required=True, help="Localized action the user must take")
    parser.add_argument("--button", required=True, help="Localized acknowledgement button")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Auto-dismiss after this many seconds; use 0 to wait indefinitely",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for the dialog to close instead of returning immediately",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.timeout < 0:
        parser.error("--timeout must be zero or greater")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = {
        "schema_version": "academic-paper-download.human-action.v1",
        "event": "human_action_required",
        "title": args.title,
        "message": args.message,
        "timeout_seconds": args.timeout,
    }

    if args.dry_run:
        emit(
            {
                "ok": True,
                "data": {
                    **payload,
                    "shown": False,
                    "dry_run": True,
                    "chat_fallback_required": False,
                },
            }
        )
        return 0

    if platform.system() != "Darwin" or not os.access(OSASCRIPT, os.X_OK):
        emit(
            failure_payload(
                payload,
                code="macos_dialog_unavailable",
                message="The macOS modal dialog is unavailable; use the chat prompt instead.",
            )
        )
        return 2

    if args.wait:
        try:
            completed = subprocess.run(command(args), check=False, capture_output=True, text=True)
        except OSError as exc:
            emit(
                failure_payload(
                    payload,
                    code="dialog_launch_failed",
                    message=f"Could not launch the macOS dialog: {exc}",
                )
            )
            return 2
        if completed.returncode != 0:
            emit(
                failure_payload(
                    payload,
                    code="dialog_failed",
                    message=completed.stderr.strip() or "osascript failed",
                )
            )
            return 2
        emit(
            {
                "ok": True,
                "data": {
                    **payload,
                    "shown": True,
                    "waited": True,
                    "chat_fallback_required": False,
                },
            }
        )
        return 0

    try:
        process = subprocess.Popen(
            command(args),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        emit(
            failure_payload(
                payload,
                code="dialog_launch_failed",
                message=f"Could not launch the macOS dialog: {exc}",
            )
        )
        return 2

    try:
        returncode = process.wait(timeout=DIALOG_STARTUP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        returncode = None
    if returncode not in (None, 0):
        stderr = process.stderr.read().strip() if process.stderr else ""
        emit(
            failure_payload(
                payload,
                code="dialog_launch_failed",
                message=stderr or f"osascript exited during startup with status {returncode}",
                details={"process_id": process.pid},
            )
        )
        return 2

    emit(
        {
            "ok": True,
            "data": {
                **payload,
                "shown": True,
                "waited": False,
                "process_id": process.pid,
                "chat_fallback_required": False,
            },
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
