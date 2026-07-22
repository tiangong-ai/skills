# Exact Browser Download Handoff

Read this reference only when automatic sources are exhausted or the user
supplies a publisher URL that requires the current browser session.

## Browser Rules

Use the available Chrome-control skill and follow its current runtime
documentation. Do not hard-code browser API method names. Preserve the user's
logged-in session, do not inspect cookies or passwords, and keep unrelated tabs
open.

Identify the download with one of these mechanisms, in priority order:

1. Use the browser download ID/GUID and resolve it to the exact final path.
2. Set a collision-free filename in the native Save dialog before saving.

Never scan Downloads and select the newest PDF. If neither a download ID nor an
exact filename is available, stop and request the minimum user action instead
of guessing.

## Exact Filename Workflow

1. Plan a unique filename before clicking the download control:

```bash
python3 scripts/finalize_browser_download.py plan-save \
  --author 'First Author; Second Author' \
  --year '2024' \
  --title 'Concise paper title' \
  --doi '10.1038/example'
```

2. Snapshot that exact filename before the download begins:

```bash
python3 scripts/finalize_browser_download.py snapshot \
  --downloads-dir "$HOME/Downloads" \
  --expected-filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
   --output /tmp/academic-paper-download.snapshot.json
```

The snapshot binds finalization to this resolved Downloads directory, exact
basename, and creation time. Do not substitute another downloads directory or
use a symbolic link for the expected file.

3. Start the browser download. If a native Save dialog appears, set the exact
   planned filename and keep the location in Downloads. If the browser API
   supplies a download ID, retain it for provenance and use its resolved final
   basename as `--expected-filename`.

4. Finalize only that expected file:

```bash
python3 scripts/finalize_browser_download.py finalize \
  --snapshot /tmp/academic-paper-download.snapshot.json \
  --expected-filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
  --filename 'FirstAuthor_2024_Concise_paper_title.pdf' \
  --doi '10.1038/example' \
  --title 'Concise paper title' \
  --source-url 'https://publisher.example/article'
```

Add `--download-id ID` when the browser exposes one. Add `--output-dir` only
when the user explicitly requested another destination. The finalizer waits for
the exact snapshot-bound path, rejects pre-snapshot files, symbolic links, and
partial downloads, structurally validates the PDF with `pypdf`, computes
SHA-256, copies atomically when needed, and writes the manifest last as the
commit record.

## Human Action: Required Dialog First

When login, SSO, CAPTCHA, browser setup, VPN connection, or a security decision
requires the user, stop automatic retries immediately. Never request a
password, cookie, CAPTCHA answer, or other secret in chat.

On macOS, you MUST first attempt the native dialog with the exact action the
user needs to take:

```bash
python3 scripts/notify_human.py \
  --title 'Paper download needs your action' \
  --message 'Complete the publisher login, then return to this chat.' \
  --button 'OK'
```

Parse the command's JSON and require all of these signals before treating the
dialog as shown: exit code `0`, `ok: true`, `data.shown: true`, and
`data.chat_fallback_required: false`. Do not skip the command merely because
the agent can already send a chat message.

If the command is unavailable, exits nonzero, emits invalid JSON, or does not
prove `data.shown: true`, immediately give the same actionable instruction in
chat. A failure payload sets `data.chat_fallback_required: true` and includes
`data.chat_fallback_message`; use that message when present.

Whether the dialog succeeds or chat fallback is used, do not continue browser
automation until the user reports that the action is complete. Then verify
that the blocker is actually gone before continuing. Never solve or bypass a
CAPTCHA, paywall, browser security interstitial, or authentication challenge.

After the manifest and final filesystem verification succeed, close only tabs
opened or claimed solely for this download, using the browser API documented at
runtime.
