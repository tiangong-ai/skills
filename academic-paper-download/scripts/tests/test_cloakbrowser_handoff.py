from __future__ import annotations

import contextlib
import importlib.metadata
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from helpers import PDF_BYTES
import cloakbrowser_handoff as cli
from paper_fetch.artifact import manifest_path, sha256_file
from paper_fetch.cloakbrowser_handoff import (
    CloakBrowserAdapter,
    DownloadEvidence,
    HandoffConfig,
    LocatorSpec,
    execute_handoff,
    validate_profile_path,
)
from paper_fetch.errors import PaperFetchError
from paper_fetch.sanitize import sanitize_data


class FakeSession:
    def __init__(
        self,
        *,
        payload: bytes = PDF_BYTES,
        blocker: str | None = None,
        failure: PaperFetchError | None = None,
        navigate_failure: PaperFetchError | None = None,
        evidence_url: str = "https://publisher.example/download.pdf",
        suggested_filename: str = "paper.pdf",
    ) -> None:
        self.payload = payload
        self.blocker_value = blocker
        self.failure = failure
        self.navigate_failure = navigate_failure
        self.evidence_url = evidence_url
        self.suggested_filename = suggested_filename
        self.navigate_calls = 0
        self.capture_calls = 0
        self.destinations: list[Path] = []

    def navigate(self, url: str, *, timeout_ms: int) -> None:
        self.navigate_calls += 1
        if self.navigate_failure:
            raise self.navigate_failure

    def blocker(self) -> str | None:
        return self.blocker_value

    def capture_download(
        self,
        locator: LocatorSpec,
        destination: Path,
        *,
        download_id: str,
        timeout_ms: int,
    ) -> DownloadEvidence:
        self.capture_calls += 1
        self.destinations.append(destination)
        if self.failure:
            raise self.failure
        destination.write_bytes(self.payload)
        (destination.parent / "concurrent-unrelated.pdf").write_bytes(PDF_BYTES + b"other")
        return DownloadEvidence(
            url=self.evidence_url,
            suggested_filename=self.suggested_filename,
            download_id=download_id,
        )


class FakeAdapter:
    def __init__(self, browser: FakeSession) -> None:
        self.browser = browser
        self.preflight_calls: list[str] = []
        self.profile_dirs: list[Path] = []

    def preflight(self, browser_version: str) -> None:
        self.preflight_calls.append(browser_version)

    @contextlib.contextmanager
    def session(self, profile_dir: Path, *, browser_version: str):
        self.profile_dirs.append(profile_dir)
        yield self.browser


class CloakBrowserHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.profile = self.root / "dedicated-profile"
        self.output = self.root / "papers"

    def config(self, **overrides) -> HandoffConfig:
        values = {
            "browser_backend": "cloakbrowser",
            "profile_dir": self.profile,
            "publisher_url": "https://publisher.example/article",
            "output_dir": self.output,
            "doi": "10.1234/example",
            "locator": LocatorSpec(role="link", name="Download PDF"),
            "title": "Example paper",
            "author": "Alice Example",
            "year": 2024,
            "filename": "Example.pdf",
            "browser_version": "145.0.7632.109.2",
            "timeout_seconds": 0.2,
            "max_bytes": 4096,
        }
        values.update(overrides)
        return HandoffConfig(**values)

    def test_backend_must_be_selected_explicitly(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(
                [
                    "--profile-dir", str(self.profile),
                    "--url", "https://publisher.example/article",
                    "--output-dir", str(self.output),
                    "--doi", "10.1234/example",
                    "--download-text", "PDF",
                ]
            )
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "validation_error")
        adapter = FakeAdapter(FakeSession())
        with self.assertRaises(PaperFetchError) as raised:
            execute_handoff(self.config(browser_backend="chrome"), adapter=adapter)
        self.assertEqual(raised.exception.code, "browser_backend_required")
        self.assertEqual(adapter.preflight_calls, [])

    def test_missing_optional_dependency_is_structured(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "CLOAKBROWSER_BINARY_PATH": "",
                "CLOAKBROWSER_DOWNLOAD_URL": "",
                "CLOAKBROWSER_SKIP_CHECKSUM": "false",
            },
            clear=False,
        ), mock.patch(
            "paper_fetch.cloakbrowser_handoff.importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError("cloakbrowser"),
        ):
            with self.assertRaises(PaperFetchError) as raised:
                CloakBrowserAdapter().preflight("145.0.7632.109.2")
        self.assertEqual(raised.exception.code, "cloakbrowser_dependency_missing")
        self.assertEqual(raised.exception.details["required_version"], "0.4.12")

    def test_unverified_binary_overrides_and_checksum_skip_are_rejected(self) -> None:
        cases = (
            ({"CLOAKBROWSER_BINARY_PATH": "/tmp/custom"}, "cloakbrowser_unverified_configuration"),
            ({"CLOAKBROWSER_DOWNLOAD_URL": "https://mirror.example"}, "cloakbrowser_unverified_configuration"),
            ({"CLOAKBROWSER_SKIP_CHECKSUM": "true"}, "cloakbrowser_checksum_required"),
        )
        for environment, code in cases:
            with self.subTest(environment=environment), mock.patch.dict(
                "os.environ", environment, clear=True
            ):
                with self.assertRaises(PaperFetchError) as raised:
                    CloakBrowserAdapter().preflight("145.0.7632.109.2")
                self.assertEqual(raised.exception.code, code)

    def test_license_key_is_not_exposed_to_binary_preflight(self) -> None:
        observed: dict[str, str | None] = {}

        class FakeModule:
            @staticmethod
            def binary_info(*, browser_version: str) -> dict:
                observed["license"] = __import__("os").environ.get("CLOAKBROWSER_LICENSE_KEY")
                observed["auto_update"] = __import__("os").environ.get("CLOAKBROWSER_AUTO_UPDATE")
                return {"version": browser_version, "installed": True}

        with mock.patch.dict(
            "os.environ", {"CLOAKBROWSER_LICENSE_KEY": "license-secret"}, clear=True
        ), mock.patch(
            "paper_fetch.cloakbrowser_handoff.importlib.metadata.version", return_value="0.4.12"
        ), mock.patch(
            "paper_fetch.cloakbrowser_handoff.importlib.import_module", return_value=FakeModule()
        ):
            CloakBrowserAdapter().preflight("145.0.7632.109.2")
            self.assertEqual(__import__("os").environ["CLOAKBROWSER_LICENSE_KEY"], "license-secret")
        self.assertIsNone(observed["license"])
        self.assertEqual(observed["auto_update"], "false")

    def test_profile_is_required_and_symlinks_are_rejected(self) -> None:
        with self.assertRaises(PaperFetchError) as missing:
            validate_profile_path(None)
        self.assertEqual(missing.exception.code, "cloakbrowser_profile_required")
        target = self.root / "profile-target"
        target.mkdir()
        link = self.root / "profile-link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(PaperFetchError) as linked:
            validate_profile_path(link)
        self.assertEqual(linked.exception.code, "cloakbrowser_profile_symlink")

    def test_nonempty_unmanaged_profile_is_rejected(self) -> None:
        self.profile.mkdir()
        (self.profile / "Cookies").write_text("do not read", encoding="utf-8")
        with self.assertRaises(PaperFetchError) as raised:
            validate_profile_path(self.profile)
        self.assertEqual(raised.exception.code, "cloakbrowser_profile_unmanaged")

    def test_exact_download_object_is_saved_to_unique_stage_and_finalized(self) -> None:
        browser = FakeSession()
        result = execute_handoff(self.config(), adapter=FakeAdapter(browser))
        final_path = Path(result["data"]["file"])
        manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(browser.capture_calls, 1)
        self.assertEqual(len(browser.destinations), 1)
        self.assertTrue(browser.destinations[0].name.startswith("cloak-"))
        self.assertEqual(browser.destinations[0].suffix, ".pdf")
        self.assertEqual(final_path.read_bytes(), PDF_BYTES)
        self.assertEqual(manifest["source"], "browser_handoff")
        self.assertEqual(manifest["source_detail"]["browser_backend"], "cloakbrowser")
        self.assertEqual(manifest["source_detail"]["cloakbrowser_version"], "0.4.12")
        self.assertEqual(manifest["source_detail"]["browser_version"], "145.0.7632.109.2")
        self.assertEqual(manifest["source_detail"]["download_id"], result["download"]["download_id"])
        self.assertEqual(manifest["access_basis"], "user_authorized_browser")
        self.assertEqual(manifest["license_status"], "unknown")
        self.assertEqual(manifest["size"], final_path.stat().st_size)
        self.assertEqual(manifest["sha256"], sha256_file(final_path))
        self.assertEqual(list(self.output.glob("*.part")), [])

    def test_concurrent_pdf_is_not_selected(self) -> None:
        browser = FakeSession(payload=PDF_BYTES + b"captured")
        result = execute_handoff(self.config(), adapter=FakeAdapter(browser))
        self.assertEqual(Path(result["data"]["file"]).read_bytes(), PDF_BYTES + b"captured")
        self.assertNotIn(b"other", Path(result["data"]["file"]).read_bytes())

    def test_existing_matching_artifact_does_not_replace_browser_provenance(self) -> None:
        first = execute_handoff(self.config(), adapter=FakeAdapter(FakeSession()))
        second = execute_handoff(self.config(), adapter=FakeAdapter(FakeSession()))
        first_path = Path(first["data"]["file"])
        second_path = Path(second["data"]["file"])
        self.assertNotEqual(first_path, second_path)
        self.assertEqual(second_path.name, "Example-2.pdf")
        second_manifest = json.loads(Path(second["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(second_manifest["source"], "browser_handoff")
        self.assertEqual(second_manifest["source_detail"]["browser_backend"], "cloakbrowser")

    def test_canceled_download_does_not_commit_manifest(self) -> None:
        browser = FakeSession(
            failure=PaperFetchError("browser_download_failed", "Download canceled")
        )
        with self.assertRaises(PaperFetchError):
            execute_handoff(self.config(), adapter=FakeAdapter(browser))
        self.assertEqual(list(self.output.glob("*.json")), [])
        self.assertEqual(list(self.output.glob("*.pdf")), [])

    def test_invalid_pdf_still_fails_pypdf_eof_and_size_pipeline(self) -> None:
        for payload in (b"<html>login</html>", b"%PDF-1.7\ntruncated", b""):
            with self.subTest(payload=payload[:12]):
                browser = FakeSession(payload=payload)
                with self.assertRaises(PaperFetchError):
                    execute_handoff(self.config(), adapter=FakeAdapter(browser))
                self.assertEqual(list(self.output.glob("*.json")), [])
                self.assertEqual(list(self.output.glob("*.pdf")), [])

    def test_authentication_and_challenges_pause_for_human_without_retry(self) -> None:
        for blocker in ("login", "mfa", "captcha", "turnstile", "paywall", "security_warning"):
            with self.subTest(blocker=blocker):
                browser = FakeSession(blocker=blocker)
                notices: list[str] = []

                def notify(kind: str) -> dict:
                    notices.append(kind)
                    return {"ok": True, "data": {"shown": True, "chat_fallback_required": False}}

                with self.assertRaises(PaperFetchError) as raised:
                    execute_handoff(self.config(), adapter=FakeAdapter(browser), notifier=notify)
                self.assertEqual(raised.exception.code, "human_action_required")
                self.assertEqual(notices, [blocker])
                self.assertEqual(browser.navigate_calls, 1)
                self.assertEqual(browser.capture_calls, 0)
                self.assertEqual(list(self.output.glob("*.json")), [])

    def test_post_click_authentication_also_pauses_without_retry(self) -> None:
        browser = FakeSession(
            failure=PaperFetchError(
                "human_action_required",
                "authorization=Bearer super-secret",
                blocker="mfa",
                cookie="cookie-secret",
            )
        )
        notices: list[str] = []
        with self.assertRaises(PaperFetchError) as raised:
            execute_handoff(
                self.config(),
                adapter=FakeAdapter(browser),
                notifier=lambda kind: notices.append(kind) or {"ok": True},
            )
        rendered = json.dumps(raised.exception.as_dict())
        self.assertEqual(notices, ["mfa"])
        self.assertNotIn("super-secret", rendered)
        self.assertNotIn("cookie-secret", rendered)
        self.assertEqual(browser.capture_calls, 1)

    def test_navigation_security_warning_uses_dialog_first_protocol(self) -> None:
        browser = FakeSession(
            navigate_failure=PaperFetchError(
                "human_action_required",
                "A security warning needs review",
                blocker="security_warning",
            )
        )
        notices: list[str] = []
        with self.assertRaises(PaperFetchError) as raised:
            execute_handoff(
                self.config(),
                adapter=FakeAdapter(browser),
                notifier=lambda kind: notices.append(kind) or {"ok": True},
            )
        self.assertEqual(raised.exception.code, "human_action_required")
        self.assertEqual(notices, ["security_warning"])
        self.assertEqual(browser.navigate_calls, 1)
        self.assertEqual(browser.capture_calls, 0)

    def test_sensitive_values_and_profile_path_never_enter_result_or_manifest(self) -> None:
        secrets = ["url-user", "url-password", "query-token", "api-secret", "session-secret", "cookie-secret"]
        source_url = (
            "https://url-user:url-password@publisher.example/article"
            "?token=query-token&api_key=api-secret#session=session-secret"
        )
        browser = FakeSession(
            evidence_url="https://download.example/paper.pdf?token=query-token&session=session-secret",
            suggested_filename="cookie=cookie-secret.pdf",
        )
        result = execute_handoff(
            self.config(publisher_url=source_url, profile_dir=self.root / "profile-api-secret"),
            adapter=FakeAdapter(browser),
        )
        manifest_text = Path(result["manifest"]).read_text(encoding="utf-8")
        rendered = json.dumps(
            {
                "result": result,
                "headers": sanitize_data(
                    {"Authorization": "Bearer api-secret", "Cookie": "session=session-secret"}
                ),
            }
        ) + manifest_text
        for secret in secrets:
            self.assertNotIn(secret, rendered)
        self.assertIn("REDACTED", rendered)


if __name__ == "__main__":
    unittest.main()
