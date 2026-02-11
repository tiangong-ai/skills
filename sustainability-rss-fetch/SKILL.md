---
name: sustainability-rss-fetch
description: Subscribe sustainability journal RSS feeds, collect time-windowed candidate articles, and persist only confirmed sustainability-relevant metadata into SQLite. Use when ingesting feeds from OPML/URLs and when topic screening must be prompt-based (default LCA/MFA/green supply chain/green electricity/green design/pollution-carbon reduction, or user-defined themes) before writing to the database.
---

# Sustainability RSS Fetch

## Core Goal
- Subscribe RSS/Atom sources for sustainability journals.
- Collect a full candidate window from source feeds first.
- Perform prompt-based semantic topic screening in the agent context (no regex filtering).
- Show candidate metadata for confirmation, then persist only confirmed entries into SQLite.

## Triggering Conditions
- Receive a request to import sustainability journal feeds from OPML/URLs.
- Receive a request to screen feed articles by sustainability topics before database write.
- Receive a request to run incremental sync with deduplication for already-confirmed entries.
- Need stable metadata persistence for downstream fulltext and summarization.

## Mandatory Screening Workflow (Use This by Default)
1. Prepare runtime and database.
- Ensure dependency is installed: `python3 -m pip install feedparser`.
- In multi-agent runtimes, pin DB to an absolute path before any command:

```bash
export SUSTAIN_RSS_DB_PATH="/absolute/path/to/workspace-rss-bot/sustainability_rss.db"
```

- Initialize SQLite schema once:

```bash
python3 scripts/rss_subscribe.py init-db --db "$SUSTAIN_RSS_DB_PATH"
```

2. Collect candidate window from source feeds (do not write entries yet).
- Use OPML source (recommended for this skill):

```bash
python3 scripts/rss_subscribe.py collect-window \
  --opml assets/journal.opml \
  --start 2026-02-01 \
  --end 2026-02-10 \
  --max-items-per-feed 150 \
  --topic-prompt "筛选与可持续主题相关的文章：生命周期评价、物质流分析、绿色供应链、绿电、绿色设计、减污降碳" \
  --output /tmp/sustainability-candidates.json \
  --pretty
```

3. Screen candidates in agent context with prompt semantics.
- Load `/tmp/sustainability-candidates.json`.
- Use `topic_prompt` + user instructions for semantic matching.
- Do not use regex-only matching; evaluate title/summary/categories/feed context together.
- Produce a confirmation list including at least:
  - `candidate_id`
  - `title`
  - `published_at`
  - `feed_title`
  - `url`

4. Ask user confirmation on selected candidates.
- Confirm selected IDs before any DB write.
- If user changes themes, re-run semantic screening first.

5. Persist only confirmed candidates.

```bash
python3 scripts/rss_subscribe.py insert-selected \
  --db "$SUSTAIN_RSS_DB_PATH" \
  --candidates /tmp/sustainability-candidates.json \
  --selected-ids 3,7,12,21
```

## Optional Classic Sync Workflow
- For operational maintenance when selection is already handled upstream, you can still use:

```bash
python3 scripts/rss_subscribe.py sync --db "$SUSTAIN_RSS_DB_PATH" --max-feeds 20 --max-items-per-feed 100
```

## Source Management
- Add one feed URL:

```bash
python3 scripts/rss_subscribe.py add-feed --db "$SUSTAIN_RSS_DB_PATH" --url "https://example.com/feed.xml"
```

- Import feeds from OPML:

```bash
python3 scripts/rss_subscribe.py import-opml --db "$SUSTAIN_RSS_DB_PATH" --opml assets/journal.opml
```

## Query Persisted Metadata
- List feeds:

```bash
python3 scripts/rss_subscribe.py list-feeds --db "$SUSTAIN_RSS_DB_PATH" --limit 50
```

- List recent entries:

```bash
python3 scripts/rss_subscribe.py list-entries --db "$SUSTAIN_RSS_DB_PATH" --limit 100
```

## Input Requirements
- Supported inputs:
  - RSS XML feed URLs.
  - OPML feed list files.
  - Optional user-defined sustainability topic prompt.

## Output Contract (Metadata Only)
- Persist `feeds` metadata to SQLite:
  - `feed_url`, `feed_title`, `site_url`, `etag`, `last_modified`, status fields.
- Persist `entries` metadata to SQLite:
  - `dedupe_key`, `guid`, `url`, `canonical_url`, `title`, `author`,
    `published_at`, `updated_at`, `summary`, `categories`, timestamps.
- Do not store generated summaries and do not fetch full article bodies in this skill.

## Configurable Parameters
- `db_path`
- `SUSTAIN_RSS_DB_PATH` (recommended absolute path in multi-agent runtime)
- `opml_path`
- `feed_urls`
- `topic_prompt`
- `start/end window`
- `max_feeds_per_run`
- `max_items_per_feed`
- `user_agent`
- `seen_ttl_days`
- `enable_conditional_get`
- Example config: `assets/config.example.json`

## Error and Boundary Handling
- Feed HTTP/network failure: keep collecting/syncing other feeds and record errors.
- Feed `304 Not Modified`: skip entry parsing in `sync` mode.
- Missing `guid` and `link`: use hashed fallback dedupe key.
- Dependency missing (`feedparser`): return install guidance.
- Empty or invalid selected IDs in `insert-selected`: fail fast before DB write.

## Final Output Checklist (Required)
- core goal
- triggering conditions
- input requirements
- metadata schema
- semantic screening and confirmation rules
- command workflow
- configurable parameters
- error handling

Use the following simplified checklist verbatim when the user requests it:

```text
核心目标
输入需求
触发条件
元数据模型
语义筛选与确认规则
命令流程
可配置参数
错误处理
```

## References
- `references/input-model.md`
- `references/output-rules.md`
- `references/time-range-rules.md`

## Assets
- `assets/journal.opml` (default sustainability journal feed source)
- `assets/config.example.json`

## Scripts
- `scripts/rss_subscribe.py`
