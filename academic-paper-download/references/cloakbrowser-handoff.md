# CloakBrowser Browser Handoff

Use this optional backend only after the legal OA order is exhausted and the
user explicitly selects CloakBrowser. Prefer the current Chrome session when it
already has authorized institutional access. CloakBrowser is not a way to evade
authentication, entitlements, CAPTCHA, Turnstile, paywalls, or security
warnings.

## Contents

- [Reproducible Setup](#reproducible-setup)
- [Dedicated Profile](#dedicated-profile)
- [Run an Exact Download](#run-an-exact-download)
- [Human Action and Stop Rules](#human-action-and-stop-rules)
- [Provenance and Errors](#provenance-and-errors)

## Reproducible Setup

Install optional dependencies in an isolated Python 3.10+ environment:

```bash
python3 -m pip install -r requirements.txt -r requirements-cloakbrowser.txt
```

The lock uses `cloakbrowser==0.4.12`. The executor pins the official free
browser binary per platform:

| Platform | Browser version |
| --- | --- |
| macOS arm64/x64 | `145.0.7632.109.2` |
| Linux x64 / Windows x64 | `146.0.7680.177.5` |
| Linux arm64 | `146.0.7680.177.3` |

Preinstall the exact browser binary explicitly; first-run downloading is not
allowed inside the handoff:

```bash
CLOAKBROWSER_AUTO_UPDATE=false \
CLOAKBROWSER_VERSION=145.0.7632.109.2 \
python -m cloakbrowser install

CLOAKBROWSER_AUTO_UPDATE=false \
CLOAKBROWSER_VERSION=145.0.7632.109.2 \
python -m cloakbrowser info --json
```

Use the table's version for the host platform. The official installer verifies
the downloaded archive against a signed checksum manifest. Do not set
`CLOAKBROWSER_SKIP_CHECKSUM`, a custom download URL, or a local binary override.
The executor rejects those configurations, verifies that the pinned binary is
already installed, disables background wrapper/binary update checks, and never
runs `pip install`, `playwright install`, or a browser download.

On macOS, if the verified binary is blocked on first launch, follow the minimum
action in the structured `cloakbrowser_macos_launch_blocked` error. Review the
exact pinned app with normal Gatekeeper controls; do not disable system-wide
security. The official package documents targeted quarantine removal as a
fallback for its cached Chromium app.

## Dedicated Profile

Pass a new, explicit absolute directory with `--profile-dir`. The executor:

- rejects symbolic links in the path or marker;
- rejects common Chrome profile locations;
- accepts only an empty directory or one it previously initialized;
- never reads, copies, imports, exports, or asks for existing Chrome cookies,
  passwords, API keys, session tokens, or storage state;
- never writes the profile path to stdout, stderr, events, or manifests.

Use headed mode. Complete ordinary login, SSO, MFA, VPN, and entitlement steps
yourself in that dedicated profile.

## Run an Exact Download

Prefer an accessibility role and exact visible name for the download control:

```bash
python3 scripts/cloakbrowser_handoff.py \
  --browser-backend cloakbrowser \
  --profile-dir /absolute/path/to/dedicated-paper-profile \
  --url 'https://publisher.example/article' \
  --doi '10.1234/example' \
  --title 'Example paper' \
  --output-dir ./papers \
  --download-role link \
  --download-role-name 'Download PDF'
```

Use `--download-text`, `--download-test-id`, or `--download-selector` only when
an exact role/name is unavailable. Exactly one locator is required.

Before clicking, the executor creates a unique event ID, temporary staging
directory, exact PDF filename, and browser-download snapshot. It then uses the
Playwright-compatible `page.expect_download()` event and the resulting
`Download.save_as()` method. It records the sanitized download URL, suggested
filename, and event ID. It never scans Downloads or selects the newest PDF.

The presence of the staged file is not success. The executor passes that exact
path to `finalize_browser_download.py`, which retains the existing pypdf, EOF,
size, SHA-256, atomic-copy, and manifest-last checks. Failed or canceled
downloads do not commit a manifest.

## Human Action and Stop Rules

The executor explicitly sets `humanize=False` and `stealth_args=False`. If it
detects login, SSO, MFA, CAPTCHA, Turnstile, a paywall, insufficient permission,
subscription unavailability, or a security warning, it runs the existing
`notify_human.py` dialog-first protocol, does not click through or retry, closes
cleanly so allowed profile state persists, and returns
`human_action_required`. Rerun only after the user confirms authorized access.

If login or entitlement remains unavailable, stop. Do not rotate identities,
alter fingerprints, solve challenges automatically, or silently switch to a
different browser backend.

## Provenance and Errors

Successful manifests retain `source=browser_handoff` and add safe fields under
`source_detail`: `browser_backend=cloakbrowser`, sanitized download URL,
suggested filename, event ID, pinned wrapper version, and pinned browser
version. Defaults remain
`access_basis=user_authorized_browser` and `license_status=unknown`. Supply
license, license URL, host type, or article version only when the source states
them explicitly.

Configuration errors are structured and actionable:

- `cloakbrowser_dependency_missing`: install the optional lock; no runtime
  installation occurs.
- `cloakbrowser_binary_missing`: run the returned exact pinned install command.
- `cloakbrowser_binary_incompatible`: inspect the pinned binary with
  `python -m cloakbrowser info --json`.
- `cloakbrowser_macos_launch_blocked`: review the pinned app with normal macOS
  security controls.
- `human_action_required`: complete or assess the displayed challenge; no
  automatic retry or bypass occurred.

All errors and events pass through the existing sanitization layer. Never place
profile paths, cookies, Authorization headers, proxy passwords, license keys, or
session data in logs or manifests.

API behavior was checked against the official
[CloakBrowser Python documentation](https://github.com/CloakHQ/CloakBrowser)
and [Playwright Download API](https://playwright.dev/python/docs/api/class-download)
for the pinned wrapper before implementation.
