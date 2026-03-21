# YouTube Comments Fetch Constraints and Safety Notes

## Time-Window Reality

- The API does not provide server-side `publishedAfter` / `publishedBefore` filters for comment threads or replies.
- This skill therefore filters time windows client-side after fetching comments.
- Top-level comment coverage is strongest when `--order time` is used.
- Reply coverage inside a time window is best-effort because replies are discovered through threads found on selected pages.

## Built-in Protections

- Retry transient failures with exponential backoff.
- Respect `Retry-After` with a configurable upper cap.
- Throttle request rate with a minimum interval.
- Enforce hard caps on videos, pages, threads, and comments.
- Validate transport and response structure before emitting records.
- Deduplicate comments by `comment_id`.

## Scope Boundaries

- No video discovery in this skill.
- No scheduler/polling loop.
- No moderation or author-only views.
- No guarantee of full reply completeness across every historical thread; inspect `reply_window_completeness` and stop reasons.
