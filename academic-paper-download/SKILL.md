---
name: academic-paper-download
description: "Resolve and reliably download academic-paper PDFs from a DOI, exact title, or publisher URL. Use when a user wants a local paper PDF, including cases that may require an existing Chrome login or institutional browser session. Tries Unpaywall, Semantic Scholar open-access PDFs, arXiv, then Sci-Hub; loads the browser handoff only after automatic sources are exhausted or when the input is a publisher URL requiring interactive access."
---

# Academic Paper Download

Produce a verified PDF plus an adjacent DOI/source/hash manifest. Preserve the
fixed source order and never report a file as successful merely because it
exists.

## Workflow

1. Collect one DOI, exact title, or publisher URL. For a title, also collect
   author and year when available so duplicate titles can be disambiguated.
   Use the user's requested output directory; otherwise keep the default
   `~/Downloads` destination.
2. Read [references/env.md](references/env.md) only when environment or source
   settings need to change.
3. For a DOI or title, run the downloader. Keep this source order:
   Unpaywall, Semantic Scholar OA, arXiv, then Sci-Hub.
4. Report success only when the result contains a validated file, SHA-256, and
   manifest path. A verified existing file may return `skipped: true`.
5. If the input is a publisher URL requiring interactive access, or a failed
   result contains `browser_handoff`, read
   [references/browser-handoff.md](references/browser-handoff.md) and follow it.
   When login, SSO, CAPTCHA, VPN, browser setup, or a security decision needs
   the user, execute that reference's required dialog-first protocol before
   pausing; never silently continue or make the native dialog optional.

## Download

For a DOI:

```bash
python3 -m pip install -r requirements.txt
python3 scripts/fetch.py '10.1038/s41586-020-2649-2' \
  --out ./papers --format json --pretty
```

For an exact title:

```bash
python3 scripts/fetch.py \
  --title 'A precise paper title' \
  --author 'First Author' --year 2024 \
  --out ./papers --format json --pretty
```

If multiple distinct DOIs still match, stop on `title_ambiguous` and present
the returned candidates. Never choose the first candidate merely because its
normalized title is identical.

Use `--dry-run` to preview the first resolved candidate. Use `schema` to inspect
the machine contract:

```bash
python3 scripts/fetch.py schema --pretty
```

## Result Rules

- Treat exit code `0` as complete, `1` as unresolved, `3` as invalid input or
  unsafe idempotency reuse, and `4` as retryable transport failure.
- Require `pypdf` structural parsing, at least one readable page, and a final
  `%%EOF` marker before committing a PDF or manifest.
- Accept a skip only when the adjacent manifest DOI matches and its SHA-256
  matches the current file bytes.
- Never claim success from HTML, an incomplete file, or a browser download that
  was selected only because it was newest.
- Do not combine `--stream` and `--pretty`; stream output must remain NDJSON.
- Respect publisher terms, institutional authorization, and applicable law.

## Provenance

`scripts/paper_fetch/` selectively adapts MIT-licensed ideas and code from
`Agents365-ai/paper-fetch` at commit
`c3baaa3d5df9a7eecb16fc2b4c8d10416f59bcb7`. See
`LICENSE.paper-fetch.txt` for the retained license notice.
