---
name: academic-paper-download
description: "Fetch and atomically save verified academic-paper PDFs from legal open-access sources using a DOI or exact title, with access/license provenance and an adjacent hash manifest. Use for automatic OA retrieval or for a publisher URL that must first be resolved to a DOI and may then require a user-authorized browser handoff; supports an injectable transport for embedding in research systems."
---

# Academic Paper Download

Produce a structurally verified PDF and adjacent provenance manifest. Require an
explicit final output directory and keep automatic resolution in this order:
Unpaywall, Semantic Scholar OA, arXiv, then browser handoff.

## Workflow

1. For a DOI or exact title, select the caller's final output directory. Never
   guess a research directory. Add author/year when they help disambiguate a
   title.
2. For a publisher URL, first resolve or confirm its DOI. Pass only that DOI to
   `fetch.py`; the CLI does not accept publisher URLs as inputs.
3. Run the downloader and accept success only when the result contains a
   verified file, SHA-256, size, and adjacent manifest.
4. If automatic OA sources are exhausted, or a publisher page requires login,
   institution access, or interaction, read
   [references/browser-handoff.md](references/browser-handoff.md). Do not bypass
   CAPTCHA, paywalls, security warnings, or authentication.
5. Read [references/integration.md](references/integration.md) when embedding
   the library or injecting a provenance-recording transport. Read
   [references/env.md](references/env.md) for runtime configuration.

## CLI

Install the pinned dependency before use; scripts never install packages:

```bash
python3 -m pip install -r requirements.txt
```

Fetch by DOI:

```bash
python3 scripts/fetch.py '10.48550/arXiv.1706.03762' \
  --out ./papers --format json --pretty
```

Fetch by exact title:

```bash
python3 scripts/fetch.py \
  --title 'A precise paper title' \
  --author 'First Author' --year 2024 \
  --out ./papers --format json --pretty
```

Use `schema` to inspect the unchanged machine contract version. A verified
existing artifact may return `skipped: true`.

## Result Rules

- Treat exit code `0` as complete, `1` as unresolved, `3` as invalid input,
  and `4` as retryable transport failure.
- Require `pypdf` parsing, at least one page, a final `%%EOF`, matching size,
  and SHA-256 before committing the PDF and manifest.
- Never infer redistribution permission from successful access. Preserve
  `access_basis`, `license_status`, and source-declared license fields.
- Never select the newest file in Downloads or accept HTML, truncated PDFs,
  symbolic links, partial downloads, credentials, cookies, passwords, or
  session tokens.

## Provenance

`scripts/paper_fetch/` selectively adapts MIT-licensed ideas and code from
`Agents365-ai/paper-fetch` at commit
`c3baaa3d5df9a7eecb16fc2b4c8d10416f59bcb7`. See
`LICENSE.paper-fetch.txt` for the retained license notice.
