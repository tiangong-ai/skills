# Library Integration

Install the compatible dependency range declared by `pyproject.toml` in the
host application's own environment. The CLI/test `requirements.lock` is exact
for reproducibility, but embedding supports the tested `pypdf>=6.14,<7` range.
Add `scripts/` to the Python import path, then use the stable library entry:

```python
from paper_fetch import FetchRequest, fetch_paper

result = fetch_paper(
    FetchRequest(
        doi="10.48550/arXiv.1706.03762",
        output_dir="/explicit/research/directory",
    )
)
```

`fetch_paper(request, *, transport=None, progress=None)` accepts a
`FetchRequest` or mapping. Exactly one of `doi` and `title` is required, and
`output_dir` is always required. `progress(event, fields)` receives sanitized
structured events.

## Injectable Transport

Implement `paper_fetch.PaperTransport` with:

```python
def get_json(url, *, timeout, headers=None, max_bytes=5 * 1024 * 1024): ...
def get_text(url, *, timeout, headers=None, max_bytes=5 * 1024 * 1024): ...
def download_to(url, destination, *, timeout, max_bytes, headers=None): ...
```

Pass the implementation as `transport=`. Every Crossref, Unpaywall, Semantic
Scholar, arXiv metadata, and PDF request uses that object, so a host system can
write its provenance ledger synchronously per request without parsing stdout.
The default `HttpClient` uses urllib, public-network URL checks, redirect
validation, bounded reads, and structured retryability classifications.

Transport implementations should raise `PaperFetchError(code, message,
retryable=...)` for classified failures and must write downloads only to the
provided temporary `destination`. They must not log sensitive header values or
unsanitized URL query credentials.

## Identity Contract

Automatic and browser commits share the same fail-closed identity validator.
Successful results and manifests use schema v3 and include
`identity_status: matched` plus bounded `identity` evidence. Document DOI fields
and primary first-page DOI positions take precedence. When neither supplies a
DOI, a strong title match may succeed unless available author or year evidence
conflicts; a title found only in first-page text also requires positive author
or year support. Subject, keywords, descriptions, and other DOI strings remain
recorded as non-primary observations and do not by themselves identify or
reject a paper.

`pdf_identity_mismatch` means the available primary identity evidence conflicts
with the requested paper. `pdf_identity_unresolved` means metadata and bounded
first-page text were insufficient. Neither error commits a PDF or success
manifest. The validator performs no OCR and never stores extracted page text.
Legacy v2 manifests have no identity evidence and are ineligible for verified
replay.

## Browser Handoff Boundary

Do not implement Chrome or CloakBrowser as `PaperTransport`. The optional
CloakBrowser executor has a separate injectable `BrowserAdapter` for fake-based
tests and publisher-page interaction. It captures one Playwright-compatible
Download object, saves it to a preplanned unique staging path, and then invokes
`finalize_browser_download.py`; it does not participate in OA resolution or
metadata HTTP traffic. The finalizer identity-checks that exact staged file
before renaming, copying, or writing its manifest.
