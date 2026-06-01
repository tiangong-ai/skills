---
name: tiangong-kb-course-fulltext-fetch
description: "Fetch full text for a Tiangong KB course document through the Tiangong AI CLI. Use when a task has a course document_id and tags and needs the complete processed text."
---

# Tiangong KB Course Fulltext Fetch

Use this skill when the task has a Tiangong KB course `document_id` and `tags`,
and needs the full processed text for that document.

## Boundary

Always call the Tiangong AI CLI:

```bash
npx @tiangong-ai/cli@latest kb course fulltext --document-id <document_id> --tags <tags>
```

Do not call S3, NAS, Supabase, Pinecone, or OpenSearch directly from the skill.
The CLI owns the S3 path rule, list/get behavior, and AWS SDK credential
handling.

## Prerequisites

- The wrapper defaults to `npx @tiangong-ai/cli@latest`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint.
- AWS credentials must already be available in the environment through the AWS
  SDK default chain, such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION`, or `AWS_DEFAULT_REGION`.
- If credentials live in a dotenv file, pass `env_file`. Loaded dotenv values
  only fill unset environment variables; explicit JSON fields such as `bucket`,
  `prefix`, and `region` are passed as CLI flags and take precedence. Never
  print secret values.

## Fetch Full Text

Pass the course `document_id` and `tags`:

```bash
./scripts/course_fulltext_fetch.sh '{
  "document_id": "000125ed-c4d9-4fe3-9383-281162406d66",
  "tags": "thu_humanities"
}'
```

The wrapper calls:

```bash
npx @tiangong-ai/cli@latest kb course fulltext --document-id <document_id> --tags <tags>
```

To save the full text:

```bash
./scripts/course_fulltext_fetch.sh '{
  "document_id": "000125ed-c4d9-4fe3-9383-281162406d66",
  "tags": "thu_humanities"
}' ./fulltext.txt
```

To return CLI metadata and text as JSON:

```bash
./scripts/course_fulltext_fetch.sh '{
  "document_id": "000125ed-c4d9-4fe3-9383-281162406d66",
  "tags": "thu_humanities",
  "json": true
}'
```

## Input Fields

- `document_id` or `documentId`: required course document id.
- `tags` or `tag`: required course tag, such as `thu_humanities`.
- `output_file`: optional output path. The second wrapper argument takes
  precedence when both are provided.
- `env_file`: optional dotenv file to load before calling the CLI.
- `json`: true to return the CLI JSON payload instead of plain text.
- `bucket`, `prefix`, `region`: optional overrides for the CLI command.

When using the returned text in reader-facing prose, cite source titles or
paths when useful, but do not describe internal S3 paths, AWS credentials, or
retrieval mechanics unless the user asks for implementation details.
