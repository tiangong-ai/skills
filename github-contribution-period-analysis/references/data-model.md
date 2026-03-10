# Data Model and Contribution Rules

## Time Window
- Input window is interpreted in UTC.
- Boundaries are left-closed, right-open: `[start, end)`.
- Date-only input (`YYYY-MM-DD`) is expanded to full UTC-day boundaries.

## Included Activity Types
1. Authored merged PRs
- Source: `search/issues` with `is:pr author:<user> merged:<start>..<end>`.
- Enriched by PR detail API for `additions`, `deletions`, `changed_files`, `commits`.

2. PRs merged by target user
- Source: `search/issues` with `is:pr merged-by:<user> merged:<start>..<end>`.
- Used to show maintainer merge work, especially merges for other authors.

3. Authored commits
- Source: `search/commits` with `author:<user> author-date:<start>..<end>`.
- Commit stats (`additions`, `deletions`, changed file count) are loaded for a bounded subset.
- Commit file paths are collected from commit detail API when stats are loaded.

## Excluded Activity Types
- Comment events.
- Review events.
- Star/watch/fork events.

## Dedupe Rule
- By default, commits that belong to authored merged PRs are removed from the direct-commit bucket.
- Disable with `--no-pr-commit-dedupe` if needed.

## Repository Classification
- `own`: repository owner login equals target user login.
- `other`: all other repositories.

## Work Content Inference
- The report infers substantive work categories from PR titles, commit messages, and changed file paths.
- Primary categories:
  - Feature Development
  - Bug Fixes
  - Refactor and Cleanup
  - Infrastructure and CI
  - Testing and Quality
  - Dependency and Security
  - Documentation
  - Data and Content Updates
  - Release and Versioning
  - Maintainer Merge Work
  - General Engineering Work
- The inferred category is heuristic and evidence-backed (each row links to PR/commit URL).
- For each changed repository, the report emits concrete change rows with:
  - Change type (`Added Feature`, `Bug Fix`, `Refactor/Cleanup`, etc.)
  - Concrete change summary text
  - Evidence link (PR or commit)
  - Code delta and touched file count

## Coverage Limits
- GitHub Search API cap is 1000 items per query.
- Report includes coverage notes and truncation signals.
