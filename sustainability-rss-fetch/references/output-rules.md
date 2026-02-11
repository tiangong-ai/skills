# Output Rules

## DOI Keying
- Use `doi` as the only primary key in `entries`.
- Resolve DOI using direct RSS fields first (`doi`, `prism:doi`, `dc:identifier`), then DOI pattern extraction from link/title/summary/guid.
- If DOI is unavailable, generate deterministic surrogate key: `rss-hash:<sha256-prefix>`.

## Ingestion Order
1. Fetch RSS feed entries.
2. Persist all fetched entries into SQLite first.
3. Run semantic topic screening in agent context.
4. Label selected rows as relevant.
5. Prune non-selected rows to DOI-only payload.

## Relevance Semantics
- `is_relevant=1`: row keeps metadata for downstream enrichment and summary.
- `is_relevant=0`: metadata fields are cleared; keep DOI row only.
- `is_relevant=NULL`: ingested but not labeled yet.

## Update Semantics
- Same DOI + unchanged content hash: update seen/feed pointers only.
- Same DOI + changed content hash: overwrite metadata payload.
- Relevance-pruned DOI + new unlabeled ingest: keep minimized state until explicitly marked relevant.

## Expected Guarantees
- All fetched RSS items are persisted before filtering.
- Storage can be compacted aggressively for non-relevant rows.
- Downstream skills can rely on stable DOI references.
