# Exact Browser Download Handoff

Use this workflow only after resolving or confirming a DOI and when automatic
OA retrieval is exhausted or the publisher requires the user's current browser
session. Do not pass a publisher URL to `fetch.py`.

## Safety and Binding

Preserve the user's logged-in session without reading, exporting, or requesting
cookies, passwords, API keys, or session tokens. Never bypass CAPTCHA, paywalls,
browser security warnings, or authentication. Keep unrelated tabs open.

Bind the download by browser download ID/GUID when available, otherwise by a
collision-free exact filename planned before download. Never scan Downloads or
select its newest PDF. Stop if neither exact binding is possible.

## Exact Filename Workflow

1. Plan a staging filename in the browser Downloads directory:

```bash
python3 scripts/finalize_browser_download.py plan-save \
  --downloads-dir "$HOME/Downloads" \
  --author 'First Author' --year '2024' \
  --title 'Concise paper title' --doi '10.1038/example'
```

2. Snapshot that exact path before starting the browser download:

```bash
python3 scripts/finalize_browser_download.py snapshot \
  --downloads-dir "$HOME/Downloads" \
  --expected-filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
  --output /tmp/academic-paper-download.snapshot.json
```

3. Start the authorized browser download and retain its ID when exposed.

4. Finalize only the snapshot-bound file into an explicit final directory:

```bash
python3 scripts/finalize_browser_download.py finalize \
  --snapshot /tmp/academic-paper-download.snapshot.json \
  --expected-filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
  --filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
  --output-dir ./papers \
  --doi '10.1038/example' \
  --title 'Concise paper title' \
  --source-url 'https://publisher.example/article'
```

Add `--download-id ID` when available. Browser Downloads is only exact-bound
staging; `finalize` always requires `--output-dir`. By default its manifest uses
`access_basis=user_authorized_browser` and `license_status=unknown`. Supply
`--license-status`, `--license`, `--license-url`, `--host-type`, or
`--article-version` only when the caller has explicitly verified those facts.

The finalizer rejects pre-snapshot files, symbolic links, partial downloads,
HTML, truncated PDFs, and invalid output directories. It parses at least one
page, checks `%%EOF`, copies atomically, verifies size/SHA-256, and commits the
manifest last.

## Human Action

When login, SSO, CAPTCHA, VPN, browser setup, or a security decision requires
the user, stop retries. On macOS first attempt:

```bash
python3 scripts/notify_human.py \
  --title 'Paper download needs your action' \
  --message 'Complete the publisher login, then return to this chat.' \
  --button 'OK'
```

Treat the dialog as shown only with exit code `0`, `ok: true`,
`data.shown: true`, and `data.chat_fallback_required: false`; otherwise present
the same minimum action in chat. Wait for the user to report completion, then
verify the blocker is gone before continuing.
