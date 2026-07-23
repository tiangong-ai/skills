from __future__ import annotations

import contextlib
import importlib
import importlib.metadata
import io
import json
import logging
import os
import platform
import stat
import tempfile
import uuid
from argparse import Namespace
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ContextManager, Protocol

from .artifact import DEFAULT_MAX_BYTES, sha256_file
from .errors import PaperFetchError
from .sanitize import sanitize_data, sanitize_text, sanitize_url


PINNED_CLOAKBROWSER_VERSION = "0.4.12"
PINNED_BROWSER_VERSIONS = {
    ("Darwin", "arm64"): "145.0.7632.109.2",
    ("Darwin", "x86_64"): "145.0.7632.109.2",
    ("Linux", "x86_64"): "146.0.7680.177.5",
    ("Linux", "aarch64"): "146.0.7680.177.3",
    ("Windows", "AMD64"): "146.0.7680.177.5",
    ("Windows", "x86_64"): "146.0.7680.177.5",
}
PROFILE_MARKER = ".academic-paper-download-cloakbrowser-profile"
BLOCKERS = {
    "login",
    "mfa",
    "captcha",
    "turnstile",
    "paywall",
    "permission",
    "security_warning",
}


@dataclass(frozen=True)
class LocatorSpec:
    role: str | None = None
    name: str | None = None
    text: str | None = None
    test_id: str | None = None
    selector: str | None = None

    def validate(self) -> None:
        selected = sum(
            bool(value)
            for value in (
                self.name if self.role else None,
                self.text,
                self.test_id,
                self.selector,
            )
        )
        if selected != 1 or bool(self.role) != bool(self.name):
            raise PaperFetchError(
                "browser_locator_required",
                "Specify exactly one download locator: role/name, text, test-id, or selector",
            )


@dataclass(frozen=True)
class DownloadEvidence:
    url: str
    suggested_filename: str
    download_id: str


@dataclass(frozen=True)
class HandoffConfig:
    browser_backend: str
    profile_dir: Path
    publisher_url: str
    output_dir: Path
    doi: str
    locator: LocatorSpec
    title: str | None = None
    author: str | None = None
    year: int | str | None = None
    journal: str | None = None
    filename: str | None = None
    browser_version: str | None = None
    timeout_seconds: float = 180.0
    max_bytes: int = DEFAULT_MAX_BYTES
    license_status: str = "unknown"
    license: str | None = None
    license_url: str | None = None
    host_type: str | None = None
    article_version: str | None = None


class BrowserSession(Protocol):
    def navigate(self, url: str, *, timeout_ms: int) -> None: ...

    def blocker(self) -> str | None: ...

    def capture_download(
        self,
        locator: LocatorSpec,
        destination: Path,
        *,
        download_id: str,
        timeout_ms: int,
    ) -> DownloadEvidence: ...


class BrowserAdapter(Protocol):
    def preflight(self, browser_version: str) -> None: ...

    def session(
        self,
        profile_dir: Path,
        *,
        browser_version: str,
    ) -> ContextManager[BrowserSession]: ...


def pinned_browser_version() -> str:
    key = (platform.system(), platform.machine())
    try:
        return PINNED_BROWSER_VERSIONS[key]
    except KeyError as exc:
        raise PaperFetchError(
            "cloakbrowser_platform_unsupported",
            "No reproducible CloakBrowser binary pin is configured for this platform",
            platform=f"{key[0]}-{key[1]}",
        ) from exc


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _common_chrome_roots() -> tuple[Path, ...]:
    home = Path.home().absolute()
    roots = [
        home / "Library/Application Support/Google/Chrome",
        home / "Library/Application Support/Chromium",
        home / ".config/google-chrome",
        home / ".config/chromium",
    ]
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        roots.append(Path(local_app_data).absolute() / "Google/Chrome/User Data")
    return tuple(roots)


def validate_profile_path(value: Path | str | None) -> Path:
    if value is None or not str(value).strip():
        raise PaperFetchError(
            "cloakbrowser_profile_required",
            "CloakBrowser requires an explicit dedicated persistent profile directory",
        )
    path = Path(value).expanduser().absolute()

    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if current.exists() or current.is_symlink():
            try:
                if stat.S_ISLNK(current.lstat().st_mode):
                    raise PaperFetchError(
                        "cloakbrowser_profile_symlink",
                        "The dedicated CloakBrowser profile path and its parents must not be symbolic links",
                    )
            except OSError as exc:
                raise PaperFetchError(
                    "cloakbrowser_profile_invalid",
                    "The dedicated CloakBrowser profile path could not be inspected",
                ) from exc

    for chrome_root in _common_chrome_roots():
        if _path_is_within(path, chrome_root):
            raise PaperFetchError(
                "cloakbrowser_profile_forbidden",
                "Do not use or import an existing Chrome profile for CloakBrowser handoff",
            )

    try:
        if path.exists() and not path.is_dir():
            raise PaperFetchError(
                "cloakbrowser_profile_invalid",
                "The dedicated CloakBrowser profile path must be a directory",
            )
        marker = path / PROFILE_MARKER
        if marker.is_symlink():
            raise PaperFetchError(
                "cloakbrowser_profile_symlink",
                "The dedicated CloakBrowser profile marker must not be a symbolic link",
            )
        if path.exists() and any(path.iterdir()) and not marker.is_file():
            raise PaperFetchError(
                "cloakbrowser_profile_unmanaged",
                "Use an empty directory or a profile previously created by this handoff",
            )
    except PaperFetchError:
        raise
    except OSError as exc:
        raise PaperFetchError(
            "cloakbrowser_profile_invalid",
            "The dedicated CloakBrowser profile directory could not be inspected",
        ) from exc
    return path


def initialize_profile(path: Path) -> None:
    try:
        created = not path.exists()
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if created:
            path.chmod(0o700)
        marker = path / PROFILE_MARKER
        if marker.exists():
            return
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(marker, flags, 0o600)
        try:
            os.write(descriptor, b"academic-paper-download cloakbrowser profile v1\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise PaperFetchError(
            "cloakbrowser_profile_init_failed",
            "The dedicated CloakBrowser profile directory could not be initialized",
        ) from exc


def _redirection_is_forbidden() -> None:
    forbidden = {
        "CLOAKBROWSER_BINARY_PATH": "local binary overrides",
        "CLOAKBROWSER_DOWNLOAD_URL": "custom binary download URLs",
    }
    for variable, description in forbidden.items():
        if os.environ.get(variable):
            raise PaperFetchError(
                "cloakbrowser_unverified_configuration",
                f"CloakBrowser handoff does not permit {description}; unset {variable}",
            )
    skip_checksum = os.environ.get("CLOAKBROWSER_SKIP_CHECKSUM", "").strip().casefold()
    if skip_checksum not in {"", "false", "0", "no"}:
        raise PaperFetchError(
            "cloakbrowser_checksum_required",
            "CloakBrowser checksum verification must remain enabled; unset CLOAKBROWSER_SKIP_CHECKSUM",
        )


@contextlib.contextmanager
def _locked_runtime_environment() -> Iterator[None]:
    updates = {
        "CLOAKBROWSER_AUTO_UPDATE": "false",
        "CLOAKBROWSER_WIDEVINE": "0",
        "CLOAKBROWSER_FETCH_WIDEVINE": "0",
    }
    names = (*updates.keys(), "CLOAKBROWSER_LICENSE_KEY")
    previous = {name: os.environ.get(name) for name in names}
    previous_logging_disable = logging.root.manager.disable
    try:
        os.environ.update(updates)
        os.environ.pop("CLOAKBROWSER_LICENSE_KEY", None)
        logging.disable(logging.CRITICAL)
        yield
    finally:
        logging.disable(previous_logging_disable)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class CloakBrowserSession:
    def __init__(self, context: Any) -> None:
        self.context = context
        self.page = context.pages[0] if context.pages else context.new_page()

    def navigate(self, url: str, *, timeout_ms: int) -> None:
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception as exc:
            lowered = str(exc).casefold()
            if any(term in lowered for term in ("err_cert", "certificate", "privacy", "unsafe")):
                raise PaperFetchError(
                    "human_action_required",
                    "A browser security warning requires human review; do not bypass it",
                    blocker="security_warning",
                ) from exc
            raise PaperFetchError(
                "browser_navigation_failed",
                "The publisher page could not be opened in CloakBrowser",
                retryable=False,
            ) from exc

    def _body_text(self) -> str:
        try:
            return self.page.locator("body").inner_text(timeout=1500).casefold()[:100_000]
        except Exception:
            return ""

    def blocker(self) -> str | None:
        try:
            url = str(self.page.url).casefold()
            title = str(self.page.title()).casefold()
        except Exception:
            url = ""
            title = ""
        body = self._body_text()
        combined = f"{url}\n{title}\n{body}"
        selectors = {
            "captcha": "iframe[src*='recaptcha'], iframe[src*='hcaptcha'], [class*='captcha'], [id*='captcha']",
            "turnstile": "iframe[src*='challenges.cloudflare.com'], [class*='turnstile'], [id*='turnstile']",
            "mfa": "input[autocomplete='one-time-code']",
            "login": "input[type='password']",
        }
        for kind, selector in selectors.items():
            try:
                if self.page.locator(selector).count() > 0:
                    return kind
            except Exception:
                pass
        phrases = (
            ("security_warning", ("your connection is not private", "deceptive site ahead", "privacy error")),
            ("turnstile", ("turnstile", "verify you are human")),
            ("captcha", ("captcha", "i'm not a robot")),
            ("mfa", ("multi-factor", "two-factor", "verification code", "one-time code")),
            ("paywall", ("purchase this article", "subscribe to access", "rent this article")),
            ("permission", ("access denied", "you do not have access", "subscription unavailable")),
            ("login", ("/login", "/signin", "/sso", "institutional sign in", "sign in to access")),
        )
        for kind, terms in phrases:
            if any(term in combined for term in terms):
                return kind
        return None

    def _locator(self, spec: LocatorSpec) -> Any:
        if spec.role and spec.name:
            return self.page.get_by_role(spec.role, name=spec.name, exact=True)
        if spec.text:
            return self.page.get_by_text(spec.text, exact=True)
        if spec.test_id:
            return self.page.get_by_test_id(spec.test_id)
        assert spec.selector
        return self.page.locator(spec.selector)

    def capture_download(
        self,
        locator: LocatorSpec,
        destination: Path,
        *,
        download_id: str,
        timeout_ms: int,
    ) -> DownloadEvidence:
        target = self._locator(locator)
        try:
            if target.count() != 1:
                raise PaperFetchError(
                    "browser_download_target_ambiguous",
                    "The download locator must resolve to exactly one visible control",
                )
            with self.page.expect_download(timeout=timeout_ms) as download_info:
                target.click(timeout=timeout_ms)
            download = download_info.value
            failure = download.failure()
            if failure:
                raise PaperFetchError(
                    "browser_download_failed",
                    "The exact browser download failed or was canceled",
                    retryable=False,
                )
            evidence = DownloadEvidence(
                url=sanitize_url(str(download.url)),
                suggested_filename=sanitize_text(Path(str(download.suggested_filename)).name[:255]),
                download_id=download_id,
            )
            download.save_as(str(destination))
            return evidence
        except PaperFetchError:
            raise
        except Exception as exc:
            blocker = self.blocker()
            if blocker:
                raise PaperFetchError(
                    "human_action_required",
                    "The publisher flow requires human action before a download can continue",
                    blocker=blocker,
                ) from exc
            raise PaperFetchError(
                "browser_download_failed",
                "The exact browser download did not complete",
                retryable=False,
            ) from exc


class CloakBrowserAdapter:
    def __init__(self) -> None:
        self._module: Any | None = None

    def preflight(self, browser_version: str) -> None:
        _redirection_is_forbidden()
        try:
            installed_version = importlib.metadata.version("cloakbrowser")
            with _locked_runtime_environment(), contextlib.redirect_stdout(
                io.StringIO()
            ), contextlib.redirect_stderr(io.StringIO()):
                module = importlib.import_module("cloakbrowser")
        except (ImportError, importlib.metadata.PackageNotFoundError) as exc:
            raise PaperFetchError(
                "cloakbrowser_dependency_missing",
                "Install the optional pinned dependency from requirements-cloakbrowser.txt in an isolated environment",
                required_version=PINNED_CLOAKBROWSER_VERSION,
            ) from exc
        if installed_version != PINNED_CLOAKBROWSER_VERSION:
            raise PaperFetchError(
                "cloakbrowser_version_mismatch",
                "The installed CloakBrowser wrapper does not match the pinned optional dependency",
                installed_version=installed_version,
                required_version=PINNED_CLOAKBROWSER_VERSION,
            )
        try:
            with _locked_runtime_environment(), contextlib.redirect_stdout(
                io.StringIO()
            ), contextlib.redirect_stderr(io.StringIO()):
                info = module.binary_info(browser_version=browser_version)
        except Exception as exc:
            raise PaperFetchError(
                "cloakbrowser_binary_incompatible",
                "CloakBrowser could not inspect the pinned browser binary; run its pinned info command",
                browser_version=browser_version,
            ) from exc
        if info.get("version") != browser_version:
            raise PaperFetchError(
                "cloakbrowser_binary_incompatible",
                "The available CloakBrowser binary does not match the required version pin",
                browser_version=browser_version,
            )
        if not bool(info.get("installed")):
            raise PaperFetchError(
                "cloakbrowser_binary_missing",
                "Preinstall the pinned, signature-verified CloakBrowser binary before browser handoff",
                browser_version=browser_version,
                action=(
                    f"CLOAKBROWSER_AUTO_UPDATE=false CLOAKBROWSER_VERSION={browser_version} "
                    "python -m cloakbrowser install"
                ),
            )
        self._module = module

    @contextlib.contextmanager
    def session(
        self,
        profile_dir: Path,
        *,
        browser_version: str,
    ) -> Iterator[CloakBrowserSession]:
        if self._module is None:
            raise PaperFetchError(
                "cloakbrowser_not_ready",
                "Run CloakBrowser preflight before opening a browser session",
            )
        context: Any | None = None
        try:
            with _locked_runtime_environment(), contextlib.redirect_stdout(
                io.StringIO()
            ), contextlib.redirect_stderr(io.StringIO()):
                context = self._module.launch_persistent_context(
                    profile_dir,
                    headless=False,
                    accept_downloads=True,
                    browser_version=browser_version,
                    stealth_args=False,
                    humanize=False,
                )
                yield CloakBrowserSession(context)
        except PaperFetchError:
            raise
        except Exception as exc:
            lowered = str(exc).casefold()
            if platform.system() == "Darwin" and any(
                term in lowered for term in ("damaged", "quarantine", "operation not permitted", "cannot be opened")
            ):
                raise PaperFetchError(
                    "cloakbrowser_macos_launch_blocked",
                    "macOS blocked the verified CloakBrowser binary; approve or clear quarantine for that pinned binary, then retry",
                    browser_version=browser_version,
                ) from exc
            raise PaperFetchError(
                "cloakbrowser_launch_failed",
                "The pinned CloakBrowser binary could not start; run the pinned info command for diagnostics",
                browser_version=browser_version,
            ) from exc
        finally:
            if context is not None:
                with _locked_runtime_environment(), contextlib.suppress(
                    Exception
                ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    context.close()


def default_human_notifier(blocker: str) -> dict[str, Any]:
    import notify_human

    message = (
        "Complete the normal publisher login, SSO, MFA, VPN, or challenge in the open browser. "
        "Do not bypass a paywall or security warning. Click OK when finished; this run will stop, "
        "then rerun after authorized access is available."
    )
    output = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
        code = notify_human.main(
            [
                "--title",
                "Paper download needs your action",
                "--message",
                message,
                "--button",
                "OK",
                "--wait",
            ]
        )
    try:
        payload = json.loads(output.getvalue())
    except json.JSONDecodeError:
        payload = {
            "ok": False,
            "error": {"code": "dialog_failed", "message": "Use the chat prompt instead."},
            "data": {"chat_fallback_required": True, "chat_fallback_message": message},
        }
    return sanitize_data({"exit_code": code, "blocker": blocker, **payload})


def _load_finalizer_result(output: str, code: int) -> dict[str, Any]:
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise PaperFetchError(
            "browser_finalize_failed",
            "The browser finalizer returned an invalid structured result",
        ) from exc
    if code != 0 or not payload.get("ok"):
        error = payload.get("error") or {}
        raise PaperFetchError(
            str(error.get("code") or "browser_finalize_failed"),
            str(error.get("message") or "The exact browser download failed validation"),
            retryable=False,
            finalizer=sanitize_data(error),
        )
    return sanitize_data(payload)


def execute_handoff(
    config: HandoffConfig,
    *,
    adapter: BrowserAdapter | None = None,
    notifier: Callable[[str], dict[str, Any]] = default_human_notifier,
) -> dict[str, Any]:
    if config.browser_backend != "cloakbrowser":
        raise PaperFetchError(
            "browser_backend_required",
            "Select the browser backend explicitly; this executor only accepts cloakbrowser",
        )
    if config.timeout_seconds <= 0 or config.max_bytes <= 0:
        raise PaperFetchError(
            "validation_error",
            "Browser timeout and maximum PDF size must be positive",
        )
    config.locator.validate()
    profile_dir = validate_profile_path(config.profile_dir)
    browser_version = config.browser_version or pinned_browser_version()
    selected_adapter = adapter or CloakBrowserAdapter()
    selected_adapter.preflight(browser_version)
    initialize_profile(profile_dir)

    import finalize_browser_download as finalizer

    event_id = f"cloak-{uuid.uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix="academic-paper-download-cloak-") as temporary:
        staging_dir = Path(temporary)
        staged_filename = f"{event_id}.pdf"
        staged_path = staging_dir / staged_filename
        snapshot_path = staging_dir / "snapshot.json"
        with contextlib.redirect_stdout(io.StringIO()):
            snapshot_code = finalizer.snapshot(staging_dir, staged_filename, snapshot_path)
        if snapshot_code != 0:
            raise PaperFetchError(
                "browser_stage_plan_failed",
                "Could not reserve the unique browser staging path",
            )

        with selected_adapter.session(profile_dir, browser_version=browser_version) as browser:
            try:
                browser.navigate(config.publisher_url, timeout_ms=int(config.timeout_seconds * 1000))
            except PaperFetchError as exc:
                blocker = str(exc.details.get("blocker") or "")
                if exc.code == "human_action_required" and blocker:
                    notification = notifier(blocker if blocker in BLOCKERS else "security_warning")
                    raise PaperFetchError(
                        "human_action_required",
                        "CloakBrowser paused for user-authorized human action and did not retry automatically",
                        blocker=blocker,
                        notification=notification,
                    ) from exc
                raise
            blocker = browser.blocker()
            if blocker:
                if blocker not in BLOCKERS:
                    blocker = "permission"
                notification = notifier(blocker)
                raise PaperFetchError(
                    "human_action_required",
                    "CloakBrowser paused for user-authorized human action and did not retry automatically",
                    blocker=blocker,
                    notification=notification,
                )
            try:
                evidence = browser.capture_download(
                    config.locator,
                    staged_path,
                    download_id=event_id,
                    timeout_ms=int(config.timeout_seconds * 1000),
                )
            except PaperFetchError as exc:
                blocker = str(exc.details.get("blocker") or "")
                if exc.code == "human_action_required" and blocker:
                    notification = notifier(blocker if blocker in BLOCKERS else "permission")
                    raise PaperFetchError(
                        "human_action_required",
                        "CloakBrowser paused for user-authorized human action and did not retry automatically",
                        blocker=blocker,
                        notification=notification,
                    ) from exc
                raise

        if not staged_path.is_file():
            raise PaperFetchError(
                "browser_download_missing",
                "The captured Download object did not save the planned staging file",
            )

        finalizer_args = Namespace(
            snapshot=str(snapshot_path),
            downloads_dir=str(staging_dir),
            expected_filename=staged_filename,
            output_dir=str(config.output_dir),
            doi=config.doi,
            title=config.title,
            author=config.author,
            year=config.year,
            journal=config.journal,
            source_url=config.publisher_url,
            download_id=evidence.download_id,
            browser_backend="cloakbrowser",
            download_url=evidence.url,
            suggested_filename=evidence.suggested_filename,
            cloakbrowser_version=PINNED_CLOAKBROWSER_VERSION,
            browser_version=browser_version,
            access_mode="dedicated-browser-profile",
            access_basis="user_authorized_browser",
            license_status=config.license_status,
            license=config.license,
            license_url=config.license_url,
            host_type=config.host_type,
            article_version=config.article_version,
            filename=config.filename,
            timeout=config.timeout_seconds,
            poll_interval=0.01,
            stable_seconds=0,
            max_bytes=config.max_bytes,
        )
        finalizer_output = io.StringIO()
        with contextlib.redirect_stdout(finalizer_output), contextlib.redirect_stderr(io.StringIO()):
            finalizer_code = finalizer.finalize(finalizer_args)
        payload = _load_finalizer_result(finalizer_output.getvalue(), finalizer_code)
        payload["download"] = sanitize_data(
            {
                "url": evidence.url,
                "suggested_filename": evidence.suggested_filename,
                "download_id": evidence.download_id,
                "sha256": sha256_file(Path(payload["data"]["file"])),
            }
        )
        return sanitize_data(payload)
