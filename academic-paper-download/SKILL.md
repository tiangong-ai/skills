---
name: academic-paper-download
description: "Fetch and atomically save structurally and identity-verified academic-paper PDFs from legal open-access sources using a DOI or exact title, with access/license provenance and an adjacent hash manifest. Use for automatic OA retrieval or for a publisher URL that must first be resolved to a DOI and may then require an explicitly selected Chrome or optional CloakBrowser user-authorized browser handoff; supports an injectable transport for embedding in research systems."
---

# Academic Paper Download

Produce a structurally verified, identity-bound PDF and adjacent provenance
manifest. Require an explicit final output directory and keep automatic
resolution in this order: Unpaywall, Semantic Scholar OA, arXiv, then browser
handoff.

## Workflow

1. For a DOI or exact title, select the caller's final output directory. Never
   guess a research directory. Add author/year when they help disambiguate a
   title.
2. For a publisher URL, first resolve or confirm its DOI. Pass only that DOI to
   `fetch.py`; the CLI does not accept publisher URLs as inputs.
3. Run the downloader and accept success only when the result contains a
   verified file, SHA-256, size, `identity_status: matched`, and adjacent manifest.
4. If automatic OA sources are exhausted, or a publisher page requires login,
   institution access, or interaction, explicitly choose a browser backend and
   read [references/browser-handoff.md](references/browser-handoff.md). Prefer
   the current Chrome session when it already has authorized institutional
   access. Never silently switch backends after login failure.
5. When the user explicitly selects CloakBrowser, also read
   [references/cloakbrowser-handoff.md](references/cloakbrowser-handoff.md).
   Keep it outside `PaperTransport`; do not use its stealth or humanize features
   to solve CAPTCHA, Turnstile, paywalls, security warnings, or authentication.
6. Read [references/integration.md](references/integration.md) when embedding
   the library or injecting a provenance-recording transport. Read
   [references/env.md](references/env.md) for runtime configuration.

## CLI

Resolve the installed skill directory to an absolute path. Use the path exposed
by the skill loader or `npx skills list --json`; do not assume the current
working directory is the skill directory:

```bash
SKILL_DIR='/absolute/path/to/academic-paper-download'
```

After installation, explicitly create the hash-locked CLI runtime and prove it
with the network-free smoke test. `bootstrap` is the only core command that
installs packages; normal commands never install or update dependencies:

```bash
python3 "$SKILL_DIR/scripts/runtime.py" bootstrap --locked --json
python3 "$SKILL_DIR/scripts/runtime.py" smoke --offline --json
```

The runtime lives outside the installed skill directory and works for copy,
symlink, and read-only installations. Install `requirements-cloakbrowser.txt`
only in a separate isolated environment when that optional backend is
explicitly selected. Its browser binary is a separate, preflight-verified
installation; the handoff script never downloads it.

Fetch by DOI:

```bash
python3 "$SKILL_DIR/scripts/runtime.py" fetch \
  '10.48550/arXiv.1706.03762' \
  --out ./papers --format json --pretty
```

Fetch by exact title:

```bash
python3 "$SKILL_DIR/scripts/runtime.py" fetch \
  --title 'A precise paper title' \
  --author 'First Author' --year 2024 \
  --out ./papers --format json --pretty
```

Use `fetch schema` to inspect the machine contract version. Only an artifact
whose current manifest records matched identity evidence may return
`skipped: true`. Read
[references/env.md](references/env.md) when embedding the library or diagnosing
Python/runtime compatibility.

## Result Rules

- Treat exit code `0` as complete, `1` as unresolved, `3` as invalid input,
  and `4` as retryable transport failure.
- Require `pypdf` parsing, at least one page, a final `%%EOF`, matching size,
  and SHA-256 before committing the PDF and manifest.
- Before commit, require the requested DOI in document identity metadata or a
  primary first-page DOI position. If no primary DOI is available, require a
  strong title match and treat available author/year disagreement as a mismatch.
  A title found only in first-page text also needs matching author or year evidence.
- Do not treat arbitrary reference-list DOIs as the paper's primary DOI. A
  scanned/no-text PDF without defensible embedded metadata is unresolved and
  requires manual verification; it is not a successful artifact.
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
