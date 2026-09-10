# Research setup and runtime environment

## Base prerequisites

- Node.js 24, `git`, and `npx` access to the exact CLI package version in the
  workspace runtime lock and the setup-pinned `skills@1.5.22` package. A clean
  directory needs one user-reviewed exact bootstrap CLI version before that
  lock exists; never infer `latest` for an existing workspace.
- A current interactive Codex, Claude Code, WorkBuddy, or CodeBuddy producer
  host plus an authenticated Codex or Claude executable for the independent
  reviewer route.
- macOS `/usr/bin/sandbox-exec` or Linux Bubblewrap (`bwrap`) on the process
  that actually runs the reviewer capsule. For an outer-sandboxed IDE this is
  the owner-started sidecar, not the IDE process; see
  [sandboxed-ide.md](sandboxed-ide.md).
- Python 3.10+ only for selected Python-based companions. Setup reports missing
  Python dependencies but never installs them.

Authenticate the reviewer CLI through its normal interactive login outside the
research workspace. Do not copy browser profiles, keychains, full agent HOME
directories, cookies, passwords, or session tokens into a workspace. Native
producer preparation does not copy host authentication. Reviewer runtime creates
a capsule HOME and copies/extracts only its documented minimal authentication
material.

Production doctor must run real capability checks and a reviewer smoke before a
costly review package. These checks may use quota and therefore require explicit
flags and cost confirmation. It never launches the current native producer.

## Setup credential input

For every selected logical credential, the Wizard offers hidden TTY input
(recommended), a named environment variable, preloaded stdin/password-manager
input, or an explicit skip. Hidden input is not echoed. Stdin is bounded and
must contain exactly one line per logical ID listed with
`--credential-stdin <id[,id...]>`. Neither path writes a value to the command
line, setup plan, stdout, stderr, journal, report, or shell history.

### Declarative credential input

`research setup init` creates
`.tiangong-research/setup.env.example` beside the non-secret
`.tiangong-research/setup.yaml`. If file-based input is needed, copy it to the
fixed `.tiangong-research/setup.env` path and restrict it before setup:

```bash
cp .tiangong-research/setup.env.example .tiangong-research/setup.env
chmod 600 .tiangong-research/setup.env
```

The real file must be regular, non-symlink, no larger than 64 KiB, and
owner-only on POSIX. It exposes all catalog credentials with one literal `NAME=value`
line per variable declared under `credentials` in `setup.yaml`.
Leave every disabled credential empty. A non-empty disabled credential is
rejected rather than selected implicitly; enable it explicitly in YAML or
remove the value. The parser does not execute `export`, interpolation, command
substitution, or other shell syntax. Extra and duplicate names are rejected. If
the same name also exists in the owner shell, the two values must not differ;
the CLI never chooses one silently.

The YAML is the choice authority. `requirement` is derived from the catalog and
the current Skill selection, `appliesTo` is catalog metadata, and `enabled`
records the owner decision. A required credential must be enabled whenever its
applicable required Skill is enabled; the same credential is conditional while
none of those Skills is selected. An optional credential may remain
`enabled: false` even when its
Skill is selected; changing it to true makes that credential explicitly
selected and therefore required from `setup.env`, the same named owner
environment variable, or an already persisted logical store.

The control-directory `.gitignore` excludes `setup.env`. Values are held only
in memory during declarative apply, sanitized as configured secrets, and then
written to the same owner-only logical stores used by interactive setup. They
never enter YAML, immutable plans, declaration bindings, output, reports, or
journals. After setup reaches full readiness, the source env file is no longer
required because subsequent checks can use the owner-only logical stores; the
owner may securely remove it unless repeatable provisioning still needs it.

Environment-variable mode remains available for developers and CI. Recommended
default names are:

| Variable | Logical ID | Requirement |
| --- | --- | --- |
| `BRAVE_API_KEY` | `brave.search.api-key` | Required for a selected Brave public-internet profile. |
| `TIANGONG_SCI_APIKEY` | `tiangong.sci.api-key` | Required only when the owner-authorized Tiangong SCI capability is selected. |
| `TIANGONG_REPORT_APIKEY` | `tiangong.report.api-key` | Required only when the owner-authorized Tiangong report capability is selected. |
| `TIANGONG_PATENT_APIKEY` | `tiangong.patent.api-key` | Required only when the owner-authorized Tiangong patent capability is selected. |
| `UNSTRUCTURED_AUTH_TOKEN` | `tiangong.unstructure.auth-token` | Required only when document preprocessing is selected. |
| `SEMANTIC_SCHOLAR_API_KEY` | `semantic-scholar.api-key` | Optional for academic paper acquisition. |

The variable name can differ, but it must appear in the explicit credential
entry and is then bound in the immutable plan only when that entry is enabled.
Enabled values must be non-trivial. After a successful apply, later
status/doctor/run commands use the owner-only logical stores and do not require
those source shell variables to remain exported. Do not put values in setup
JSON files, CLI arguments, shell history, Skill files, capability declarations,
or chat.

Broker credentials are written to the owner-only
`.tiangong-research/.env` logical credential store. Adapter credentials are
written separately to owner-only `.tiangong-research/setup-adapters.env`.
Neither file supports arbitrary dotenv keys or shell interpolation. Both reject
symlinks, duplicate/undeclared entries, malformed JSON, and group/other access.
An explicitly reviewed replacement reconciles both stores to the new selection,
so deselected logical entries do not invalidate the new configuration; values
are never printed during reconciliation.

Use exactly one supported source to rotate a selected credential:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/installed/tiangong-auto-research/scripts/research_cli.mjs

## Ordinary interactive use (hidden input)
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup credential set \
  --id brave.search.api-key --prompt \
  --workspace /absolute/path/to/workspace --json

## Password manager or CI stdin
op read 'op://Research/Brave/api-key' | \
  node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
    research setup credential set \
    --id brave.search.api-key --from-stdin \
    --workspace /absolute/path/to/workspace --json

## Existing owner environment
export OWNER_NEW_BRAVE_KEY='new owner value'
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup credential set \
  --id brave.search.api-key --from-env OWNER_NEW_BRAVE_KEY \
  --workspace /absolute/path/to/workspace --json
```

The value is never echoed. Journal events record only the logical ID, storage
class, safe input method, and—only for environment mode—a hash of the variable
name.

## Non-secret settings

- `tiangong.sci.endpoint`: exact HTTPS SCI endpoint.
- `tiangong.sci.region`: safe region header.
- `tiangong.unstructure.base-url`: exact HTTPS base URL.
- `tiangong.unstructure.provider` and `.model`: optional identifiers.
- `unpaywall.contact-email`: optional real contact email.

Settings are frozen in the setup plan and may be printed. Never embed a token,
username/password, signed query parameter, or proxy password in a setting URL.

## Companion child environments

`research setup companion run` constructs a minimal environment. It keeps only
basic process/network/TLS/Python routing variables and injects the exact selected
adapter variables:

- document preprocessing: `UNSTRUCTURED_AUTH_TOKEN`,
  `UNSTRUCTURED_API_BASE_URL`, and optional provider/model;
- paper acquisition: optional `SEMANTIC_SCHOLAR_API_KEY` and
  `UNPAYWALL_EMAIL`.

The owner variable used during setup is not forwarded under its original name,
and unrelated Authorization, Cookie, API-key, token, or session variables are
excluded.

## Direct Tiangong SCI Skill use

Outside Auto Research, the separately installed `tiangong-kb-sci-search`
wrapper uses the existing Tiangong CLI variables
`TIANGONG_SCI_APIKEY` (preferred source-specific key) or
`TIANGONG_AI_APIKEY` (fallback). Its optional dotenv file must be a regular
owner-only file and only documented Tiangong endpoint/key/region/timeout names
are loaded. Credentials are forbidden in wrapper JSON and request files.

Inside Auto Research, never run that wrapper. The wrapper detects an ancestor
immutable setup plan or runtime lock and returns
`AUTO_RESEARCH_BROKER_REQUIRED` before credentials or network unless the user
explicitly narrowed the task and supplied
`"execution_mode":"standalone"`. That explicit mode emits only a non-secret
audit event and still cannot read the broker store. Setup maps the owner
variable to logical ID `tiangong.sci.api-key`; discovery sends a non-secret
POST `request_body` to the broker, which injects `x-api-key` for the exact host.

## Reviewer wrappers and capsule authentication

The tested macOS/Linux template is
`scripts/agent-wrapper-posix.sh`; validate it with
`scripts/test-agent-wrapper-posix.sh`. A custom reviewer route must use absolute
paths, an immutable wrapper for the run, explicit model ID and prices, and the
real target binary:

```json
{
  "agent": "codex",
  "binary": "/absolute/path/to/agent-wrapper-posix.sh",
  "wrapperTargetBinary": "/absolute/path/to/codex",
  "model": "explicit-model-id",
  "effort": "low",
  "verbosity": "low"
}
```

The CLI sets `TIANGONG_RESEARCH_AGENT_BINARY`; do not set or forward it
yourself. Doctor attests the reviewer target, wrapper, internal adapter, OS, and
architecture and invalidates the attestation after relevant drift.

For Claude, the runtime does not copy an owner `settings.json`. It extracts only
documented authentication values, model-mapping variables and a credential-free HTTPS base URL from the
supported environment object; hooks, permissions, extra directories, and other
settings are excluded. On macOS, credentials held in the system keychain stay
in the keychain.

### Reviewer recipient and authorization

Before the first material-bearing review, use the locked CLI's
`research reviewer status --workspace <absolute-path> --json`. Read native
`runtime` or bridge `configuredReviewer`: CLI family, configured model alias,
and `providerRouting.endpointOrigin` / `endpointSource` describe the configured
recipient. Explicit process variables override the imported Claude settings env.
Doctor also reports `reviewer-configured-routing`; a bridge's READY describes
its transport. None of these proves the actual upstream provider/model.
`identityVerification=unverified` and a null or missing endpoint remain unknown;
older records without routing fields cannot establish historical destinations.
Neither a Claude alias nor a `firstParty` auth label proves an official service.

Describe the intended recipient and the particular packet/material scope using
this evidence. Preserve deliberately configured custom providers, including
CC Switch/GLM, and reuse applicable explicit user authorization. If an
official-only description conflicts with an unverified gateway, explain that
specific mismatch and resolve the intended recipient before sending; a vague
request to "continue" does not settle it. Do not silently replace the gateway
with an official endpoint or claim historical disclosure without request evidence.

Reuse the inspected route while its runtime binding remains applicable; do not
add a status/paid-smoke loop to every review. On runtime drift, inspect the new
configured recipient and reconcile it with intent before the explicitly
authorized refresh. Old CLI versions without this binding retain their limits;
use the reviewed upgrade procedure when a binding is needed. Paths, credentials,
URL queries and entire user settings files do not belong in the explanation.
For approval rejection or a host pause, follow
[sandboxed-ide.md](sandboxed-ide.md#host-approval-and-task-pauses).

The current host application is the producer execution boundary. Discovery
uses only the CLI's hash-bound one-shot broker command for admitted evidence;
analyze and synthesize use the complete prepared packet and its on-demand read
route and gather no new evidence. The outer CLI sandbox applies to independent
review. Compatible review packets permit only their two read-only artifact tools;
doctor and formatting repair remain tool-free.

An explicitly requested ordinary Node/Python calculation remains within the
native host's OS restrictions. Its observer forwards no provider credentials,
installs no dependencies and does not create an extra privileged sandbox or a
producer agent. Supply a prepared runtime and exact frozen inputs; stop on a
permission or runtime failure instead of enabling a bypass.

When Codex is the reviewer, it receives a capsule-local project-root marker
override. Its project configuration walk stops inside the capsule and never
requires read access to a parent workspace `.codex/config.toml`.
