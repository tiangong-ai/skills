# Environment Configuration

Use Python 3.10 or newer and install the pinned dependency before running the
downloader or browser finalizer:

```bash
python3 -m pip install -r requirements.txt
```

Scripts never install packages at runtime. `pypdf==6.14.2` is required so PDF
validation fails closed when structural parsing is unavailable.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UNPAYWALL_EMAIL` | unset | Enable Unpaywall with its required contact email. |
| `SEMANTIC_SCHOLAR_API_KEY` | unset | Optional Semantic Scholar `x-api-key`. |

The automatic source order is Unpaywall, Semantic Scholar open-access PDF,
arXiv, then browser handoff. URLs, errors, manifests, and events redact contact
email and sensitive query/header values.

Do not store or request API keys, usernames, passwords, cookies, institutional
proxy credentials, or session tokens in skill resources or chat. Publisher
authentication stays in the user's current browser session; see
[browser-handoff.md](browser-handoff.md).
