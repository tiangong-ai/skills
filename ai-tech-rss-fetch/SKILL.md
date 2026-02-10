---
name: ai-tech-rss-fetch
description: Fetch AI and tech RSS content as plain text by prioritizing full-article extraction from item links and falling back to feed-provided content/summary when full text is unavailable. Use when processing RSS feeds or OPML lists, filtering by date/source/keywords, and returning text without any summarization.
---

# AI Tech RSS Fetch

## Core Goal
- Retrieve readable text for AI/tech RSS items.
- Try full article text first.
- Fall back to RSS content/summary when full text is unavailable.
- Return text directly without summarizing or rewriting.

## Triggering Conditions
- Receive a request to pull text from AI/tech RSS feeds.
- Receive RSS/OPML inputs and need batch text extraction.
- Need date/source/keyword filtering before returning text.
- User explicitly requires "full text first, summary fallback."

## Workflow
1. Confirm input sources and selection scope.
- Accept feed URLs, OPML list, or pre-collected item files.
- Resolve date/range and item limit before extraction.
- Time handling rules: `references/time-range-rules.md`.

2. Normalize incoming items.
- Map each item into the normalized schema.
- Keep `title`, `link`, and `published_at` when available.
- Normalization details: `references/input-model.md`.

3. Extract text with strict fallback order.
- Try full article extraction from item `link` first.
- If full text is unavailable, blocked, or below length threshold, fall back in order:
  1) RSS `content:encoded` or `content`
  2) RSS `description`
  3) RSS `summary`
- Mark `text_source` as `fulltext`, `rss_content`, `rss_summary`, or `none`.

4. Filter and deduplicate.
- Remove duplicates by normalized `title + link`.
- Apply include/exclude keyword filters if provided.
- Keep descending publish-time order by default.
- Use persistent dedupe state and output only new/updated items.
- Rules: `references/output-rules.md`.

5. Return plain text output.
- Output format is `text`.
- Keep source wording; only perform minimal cleanup (remove HTML/noise).
- Do not generate any report/summary structure.

## Input Requirements
- Supported inputs:
  - RSS XML feed URLs.
  - OPML feed lists (`assets/hn-popular-blogs-2025.opml` as default pool).
  - Pre-collected files (`.json`, `.jsonl`, `.csv`, `.md`, `.txt`).
- Minimum per item: `title` and at least one of `link` / `content` / `summary`.

## Output Contract (Text Only)
- Return one plain-text block per item.
- Include these fields when available: `title`, `source`, `published_at`, `url`, `text_source`, `text`.
- If no usable text exists, output `text: [no text available]`.

## Configurable Parameters
- `date` or `date_range.start` + `date_range.end`
- `timezone`
- `lookback_hours`
- `seen_store_path`
- `seen_ttl_days`
- `enable_conditional_get`
- `emit_no_new_items`
- `max_items`
- `include_keywords` / `exclude_keywords`
- `dedupe` (`true|false`)
- `min_fulltext_chars` (threshold before fallback)
- `fallback_to_summary` (`true|false`, default `true`)
- `include_metadata` (`true|false`)
- Example config: `assets/config.example.json`

## Error and Boundary Handling
- Feed/network failure: continue with other sources and report failed sources.
- Full text extraction failure: use fallback RSS text (if enabled).
- Missing `link` but RSS text exists: use RSS text directly.
- Missing all text fields: emit `[no text available]` and continue.
- No items after filtering: return `no rss items in selected range`.
- No new/updated items after dedupe: return `no new items`.

## Final Output Checklist (Required)
- core goal
- trigger conditions
- input requirements
- fulltext-first fallback order
- filtering and dedupe
- text-only output contract
- configurable parameters
- error handling

Use the following simplified checklist verbatim when the user requests it:

```text
核心目标
输入需求
触发条件
全文优先回退规则
内容筛选与去重
文本输出约定
可配置参数
错误处理
```

## References
- `references/input-model.md`
- `references/time-range-rules.md`
- `references/output-rules.md`

## Assets
- `assets/hn-popular-blogs-2025.opml` (candidate feed pool)
- `assets/config.example.json`
