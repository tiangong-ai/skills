# Environment Configuration

Use Python 3.10 or newer. Install the pinned runtime dependency range before
downloading or finalizing PDFs:

```bash
python3 -m pip install -r requirements.txt
```

`pypdf` is required so PDF validation fails closed when structural parsing is
unavailable; a file header and SHA-256 alone do not prove that a PDF is usable.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UNPAYWALL_EMAIL` | unset | Enable Unpaywall with its required contact email. |
| `SEMANTIC_SCHOLAR_API_KEY` | unset | Optional Semantic Scholar `x-api-key`. |
| `PAPER_FETCH_NO_SCIHUB` | unset | Set any non-empty value to disable Sci-Hub. |
| `PAPER_FETCH_SCIHUB_MIRRORS` | built-in list | Comma-separated Sci-Hub mirror hostnames in priority order. |

The fixed automatic source order is Unpaywall, Semantic Scholar open-access
PDF, arXiv, then Sci-Hub. Disabling Sci-Hub does not reorder the OA sources.

Do not store API keys, usernames, passwords, cookies, or institutional proxy
credentials in the skill. Publisher authentication and institutional access
remain in the user's current browser session and are described only in
[browser-handoff.md](browser-handoff.md).
