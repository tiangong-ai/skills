# Environment Configuration

Use Python 3.10 or newer and install the pinned dependency before running the
downloader or browser finalizer:

```bash
python3 -m pip install -r requirements.txt
```

Scripts never install packages at runtime. `pypdf==6.14.2` is required so PDF
validation fails closed when structural parsing is unavailable.

For the optional CloakBrowser handoff, create a separate isolated environment
and install both locks:

```bash
python3 -m pip install -r requirements.txt -r requirements-cloakbrowser.txt
```

The optional lock pins `cloakbrowser==0.4.12`, Playwright, and all transitive
Python dependencies. Ordinary OA downloads require only `requirements.txt`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UNPAYWALL_EMAIL` | unset | Enable Unpaywall with its required contact email. |
| `SEMANTIC_SCHOLAR_API_KEY` | unset | Optional Semantic Scholar `x-api-key`. |

The CloakBrowser executor passes an exact browser version and forces
`CLOAKBROWSER_AUTO_UPDATE=false` while running. It rejects
`CLOAKBROWSER_BINARY_PATH`, `CLOAKBROWSER_DOWNLOAD_URL`, and
`CLOAKBROWSER_SKIP_CHECKSUM=true` because those settings would weaken the
recorded binary provenance or official signature/checksum path. Never print or
record `CLOAKBROWSER_LICENSE_KEY`; the reproducible free-binary executor does
not request or use it and masks it from the child browser process.

The automatic source order is Unpaywall, Semantic Scholar open-access PDF,
arXiv, then browser handoff. URLs, errors, manifests, and events redact contact
email and sensitive query/header values.

Do not store or request API keys, usernames, passwords, cookies, institutional
proxy credentials, or session tokens in skill resources or chat. Publisher
authentication stays in the explicitly selected browser session; see
[browser-handoff.md](browser-handoff.md).
