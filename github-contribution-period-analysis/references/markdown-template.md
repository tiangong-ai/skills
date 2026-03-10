# Markdown Output Structure

The report is generated in this fixed order:

1. `Analysis Scope`
- Target user, UTC window, generation time.
- Auth source and scope-based private coverage hint.
- Included and excluded activity types.

2. `Summary`
- Merged PR and commit totals.
- Repository reach (`own` vs `other`).
- Code delta aggregate.

3. `Substantive Contribution Work`
- Category-level summary of practical work content.
- Each category includes item count, repo span, code delta, and representative evidence links.

4. `Work Content Highlights`
- Row-level evidence with date, type, inferred category, repository, work content, and code delta.

5. `Data Coverage Notes`
- Query-by-query fetched counts.
- Search cap note.

6. `Warnings` (conditional)
- Auth gaps.
- API incomplete results.
- Detail-fetch failures.

7. `Repository Breakdown`
- Repo-level contribution counters and code delta.

8. `Repository Change Details`
- For each changed repository, list concrete change rows.
- Include change type, concrete content summary, evidence link, and code delta.

9. `Contribution Timeline (UTC)`
- Per-day activity counts.

10. `Top Authored Merged PRs`
- PR links, code delta, files, commit count.

11. `Top Maintainer Merges (Merged Others' PRs)`
- PR links and source author.

12. `Top Direct Commits`
- Commit links and optional stats.

13. `Evidence Appendix`
- URL lists for authored merged PRs and direct commits.
