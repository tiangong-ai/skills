from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import notify_human


ARGS = [
    "--title",
    "Action required",
    "--message",
    "Complete the CAPTCHA, then return to chat.",
    "--button",
    "OK",
]


class RunningDialog:
    pid = 4321
    stderr = io.StringIO()

    def wait(self, timeout):
        raise subprocess.TimeoutExpired("osascript", timeout)


class FailedDialog:
    pid = 4322
    stderr = io.StringIO("no GUI session")

    def wait(self, timeout):
        return 1


class HumanActionNotificationTests(unittest.TestCase):
    def run_main(self, process):
        output = io.StringIO()
        with (
            mock.patch("notify_human.platform.system", return_value="Darwin"),
            mock.patch("notify_human.os.access", return_value=True),
            mock.patch("notify_human.subprocess.Popen", process),
            contextlib.redirect_stdout(output),
        ):
            code = notify_human.main(ARGS)
        return code, json.loads(output.getvalue())

    def test_running_dialog_proves_native_notification(self):
        code, payload = self.run_main(mock.Mock(return_value=RunningDialog()))
        self.assertEqual(code, 0)
        self.assertTrue(payload["data"]["shown"])
        self.assertFalse(payload["data"]["chat_fallback_required"])

    def test_early_dialog_failure_requires_chat_fallback(self):
        code, payload = self.run_main(mock.Mock(return_value=FailedDialog()))
        self.assertEqual(code, 2)
        self.assertFalse(payload["data"]["shown"])
        self.assertTrue(payload["data"]["chat_fallback_required"])
        self.assertEqual(
            payload["data"]["chat_fallback_message"],
            "Complete the CAPTCHA, then return to chat.",
        )

    def test_launch_exception_requires_chat_fallback(self):
        code, payload = self.run_main(mock.Mock(side_effect=OSError("launch denied")))
        self.assertEqual(code, 2)
        self.assertEqual(payload["error"]["code"], "dialog_launch_failed")
        self.assertTrue(payload["data"]["chat_fallback_required"])

    def test_wait_launch_exception_requires_chat_fallback(self):
        output = io.StringIO()
        with (
            mock.patch("notify_human.platform.system", return_value="Darwin"),
            mock.patch("notify_human.os.access", return_value=True),
            mock.patch("notify_human.subprocess.run", side_effect=OSError("launch denied")),
            contextlib.redirect_stdout(output),
        ):
            code = notify_human.main([*ARGS, "--wait"])
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(payload["error"]["code"], "dialog_launch_failed")
        self.assertTrue(payload["data"]["chat_fallback_required"])

    def test_non_macos_requires_chat_fallback(self):
        output = io.StringIO()
        with (
            mock.patch("notify_human.platform.system", return_value="Linux"),
            contextlib.redirect_stdout(output),
        ):
            code = notify_human.main(ARGS)
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(payload["error"]["code"], "macos_dialog_unavailable")
        self.assertTrue(payload["data"]["chat_fallback_required"])


if __name__ == "__main__":
    unittest.main()
