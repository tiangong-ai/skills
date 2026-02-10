---
name: ai-tech-rss-summary
description: Generate AI and tech daily reports (日报) and weekly reports (周报) from mixed inputs including logs (txt/markdown/json/csv), schedule/event data, task/activity status, and external subscription summaries (for example RSS). Use when asked to manually produce daily/weekly reports, run scheduled report generation, summarize by date or week range, apply dedup/keyword/importance filtering, and output markdown/text/html/pdf reports.
---

# AI Tech RSS Summary

## Core Goal
- Generate daily reports.
- Generate weekly reports.

## Triggering Conditions
- Receive a manual instruction to generate a daily report.
- Receive a manual instruction to generate a weekly report.
- Receive a scheduled execution context (daily or weekly).

## Workflow
1. Confirm report type and scope.
- `daily`: use a single date or "today".
- `weekly`: use a week range (Monday to Sunday).
- custom range: use explicit start/end dates when provided.

2. Parse inputs and normalize to one event timeline.
- logs: txt/markdown/json/csv
- schedule and event records
- task/activity status records
- RSS/external subscription summaries
- Input normalization rules: `references/input-model.md`.

3. Filter and rank content.
- remove duplicates
- remove irrelevant content
- apply keyword include/exclude filters
- rank by importance for section ordering
- Filtering rules and scoring: `references/output-rules.md`.

4. Generate report body with required sections.
- Use the exact daily/weekly section structure below.
- Fill missing sections with explicit "no data" notes instead of dropping sections.
- Output templates: `assets/daily-template.md`, `assets/weekly-template.md`.

5. Render requested output format.
- Canonical format is Markdown.
- If requested format is text/html/pdf, convert from the canonical Markdown output.

6. Run quality checks before final output.
- completeness check
- information coverage check
- structure clarity check
- Quality checklist: `references/output-rules.md`.

## Input Requirements
- Support log text input (`.txt`, `.md`, `.json`, `.csv`).
- Support schedule and event input.
- Support task and activity status input.
- Support external subscription summaries (for example RSS item lists).

## Daily Report Structure
1. `标题` (example: `日报 YYYY-MM-DD`)
2. `快速摘要` (3 to 5 sentences)
3. `今日完成事项列表`
4. `今日未完成/延期任务`
5. `当前问题/阻碍`
6. `明日计划`
7. `参考链接/附件` (optional)

## Weekly Report Structure
1. `标题` (example: `周报 YYYY-W##`)
2. `本周概要`
3. `本周完成事项列表`
4. `本周主要趋势/指标`
5. `本周问题/风险点`
6. `下周重点计划`
7. `附件/链接` (optional)

## Time and Range Rules
- Daily report: use specified date or "today".
- Weekly report: use specified week range (Monday to Sunday).
- Flexible range: accept explicit date ranges for either report type.
- Date parsing details and defaults: `references/time-range-rules.md`.

## Configurable Parameters
- report type (`daily` or `weekly`)
- date or date range
- output style (`concise`, `detailed`, `management`)
- include stats/charts (`true` or `false`)
- include links (`true` or `false`)
- keyword filters (`include_keywords`, `exclude_keywords`)
- output format (`markdown`, `text`, `pdf`, `html`)
- Example config: `assets/config.example.json`

## Content Selection Rules
- Exclude duplicate content.
- Exclude unrelated logs.
- Sort by importance.
- Apply keyword filtering when provided.

## Error and Boundary Handling
- No data: output a structured report with "no data in selected range" notes.
- Invalid input format: explain format issue and continue with parseable inputs.
- Missing fields: ask for clarification when critical; otherwise ignore missing optional fields with a note.

## Context Understanding Requirements
- Recognize date and week range expressions.
- Understand task status transitions (`todo`, `in_progress`, `done`, `blocked`, `delayed`).
- Detect key event keywords to populate trends, risks, and next plans.

## Final Output Checklist (Required)
- core goal
- input requirements
- daily report structure
- weekly report structure
- triggering conditions
- time/range rules
- output formats
- configurable parameters
- content selection rules
- error handling
- context understanding
- quality control

Use the following simplified checklist verbatim when the user requests it:

```text
核心目标
输入需求
日报输出结构
周报输出结构
触发条件
时间范围规则
输出格式
可配置参数
内容筛选规则
错误处理
语境理解
质量控制
```

## References
- `references/input-model.md`
- `references/time-range-rules.md`
- `references/output-rules.md`

## Assets
- `assets/hn-popular-blogs-2025.opml` (source from the provided gist)
- `assets/config.example.json`
- `assets/daily-template.md`
- `assets/weekly-template.md`
