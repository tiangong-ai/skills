# Regulations.gov Comment Detail API Notes

## Endpoint

- `GET /comments/{commentId}`
- Base URL: `https://api.regulations.gov/v4`
- Authentication: `X-Api-Key: <REGGOV_API_KEY>`

## Query Parameters

- `include=attachments` (optional)
  - Include attachment relationship details when supported.

## Response Shape

- Typical response is JSON:API-like wrapper:
  - top-level `data` object
  - `data.id`
  - `data.type` (`comments`)
  - `data.attributes`
  - `data.relationships` (optional)

## Input Source Patterns

This skill supports IDs from:

- CLI repeated args: `--comment-id ...`
- File input (`--comment-ids-file`), repeatable:
  - `.txt`: one ID per line
  - `.jsonl`: each line can be ID string or JSON object containing `id`
  - `.json`: array/object with IDs (`records`, `data`, `id`, `comment_id`, `commentId`)

## Typical Pipeline

1. Use `$regulationsgov-comments-fetch` to pull a window of comments.
2. Feed its JSONL output to this skill to enrich selected IDs.
3. Run downstream NLP/sentiment classification on enriched detail payloads.
