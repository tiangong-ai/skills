# Sandboxed IDE producer and reviewer bridge

Use this reference when the native producer runs in WorkBuddy, CodeBuddy, or
another IDE sandbox, or when `native-direct` reports
`RESEARCH_NESTED_SANDBOX_UNSUPPORTED`.

## Keep the boundaries separate

The IDE is the native producer host. It may prepare and submit producer stages,
but the CLI never launches it as a child process. The independent reviewer is
still an authenticated Codex or Claude CLI running inside the existing
macOS `sandbox-exec` or Linux Bubblewrap capsule.

Choose one reviewer transport explicitly during setup:

- `native-direct`: the current process creates the platform capsule. Use it on
  an ordinary native host.
- `sandbox-bridge`: the IDE client sends one hash-bound reviewer request to an
  owner-started sidecar outside the IDE sandbox. The sidecar creates the same
  platform capsule and returns a signed, result-bound attestation.

There is no fallback between these transports. Do not enable WorkBuddy Full
Access, `dangerouslyDisableSandbox`, `dangerously-skip-permissions`,
`excludedCommands`, unsandboxed-command escape hatches, or any equivalent
bypass. Keep WorkBuddy in Default Permission mode.

## Configure the producer and transport

In the generated `setup.yaml`, keep the generated closed schema and set only
the reviewed values. A WorkBuddy example uses:

```yaml
agentRoutes:
  producerAgent: workbuddy
  reviewerAgent: claude
  producerModel: <actual-workbuddy-model-id>
  reviewerModel: <actual-claude-model-id>
  producerPricing: <reviewed-price-object>
  reviewerPricing: <reviewed-price-object>
reviewerExecution:
  transport: sandbox-bridge
  isolationProvider: platform-capsule
```

For CodeBuddy use `producerAgent: codebuddy`. The reviewer must remain `codex`
or `claude`; never label WorkBuddy/CodeBuddy output as Codex or Claude. Use the
exact model that the native host actually reports.

Apply setup normally. A bridge workspace remains blocked until its sidecar is
running and status/doctor succeeds. This is intentional; setup does not start a
privileged background process from inside the IDE.

## Start the owner-controlled sidecar

From a separate native terminal outside the IDE sandbox, resolve the exact CLI
through the installed canonical Skill and choose an explicit private state
directory outside the research workspace:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/workspace/.agents/skills/tiangong-auto-research/scripts/research_cli.mjs

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research reviewer serve \
  --workspace /absolute/path/to/workspace \
  --state-dir /absolute/private/sidecar-state --json
```

The state directory must be a regular non-symlink directory. It holds the
owner-only Ed25519 private key and atomic nonce claims and must not be placed in
the workspace, synchronized evidence tree, Skill directory, or version control.
The workspace receives only an owner-only ephemeral client binding. Neither
path, token, nor private key is printed or journaled.

The sidecar refuses READY until real negative probes prove that its capsule
cannot read the workspace credential store or an unapproved file and cannot
write outside the private capsule to the host. Its protocol exposes only
`execute`, `fingerprint`, and `status`; it has no arbitrary-command action. The
reviewer invocation disables general shell, browser, web, undeclared MCP and
Skills. Compatible review packets allow only `research_list_artifacts` and
`research_read_artifact` against the exact hash-bound packet directory; doctor
and formatting repair stay tool-free. The signed result binds this exact tool
policy and directory. Network access is for the configured model service and,
when granted, the local packet read channel, not a new evidence-acquisition route.

## Verify from the IDE

Inside WorkBuddy/CodeBuddy, keep Default Permission and run:

```bash
WORKBUDDY_AUTO_RESEARCH=/absolute/path/to/workspace/.agents/skills/tiangong-auto-research-workbuddy/scripts/workbuddy_research_cli.sh

"$WORKBUDDY_AUTO_RESEARCH" --workspace /absolute/path/to/workspace -- \
  research reviewer status --workspace /absolute/path/to/workspace --json

"$WORKBUDDY_AUTO_RESEARCH" --workspace /absolute/path/to/workspace -- \
  research reviewer doctor --confirm-agent-smoke-cost \
  --workspace /absolute/path/to/workspace --json
```

The adapter rejects WorkBuddy's ambient Node 22 instead of continuing after
`EBADENGINE`. It selects an explicit Node.js 24 executable and the adjacent
`npx`, including a fixed NVM/FNM/Volta Node 24 location when the IDE hides
system `/usr/local` paths, then the canonical resolver enforces the workspace's
exact CLI version. `AUTO_RESEARCH_NODE` remains the explicit absolute override.

Status is zero-model-cost and verifies the signed workspace/version/key binding
plus negative probes. Doctor runs the real tool-free reviewer smoke and may use
provider quota. Production remains blocked until the normal setup/workspace
doctor attestation is current.

Every bridge review binds the exact CLI version, runtime lock, workspace
configuration, capsule tree, request, result, reviewer model/runtime, platform
sandbox policy, tool policy, nonce, and signing-key fingerprint. Version,
model, policy, packet, result, signature, or session drift fails closed.

## Run native stages honestly

When the CLI reports `native-stage-required`, WorkBuddy uses
`--host-agent workbuddy`; CodeBuddy uses `--host-agent codebuddy`. Follow the
returned packet exactly and then submit through the canonical Skill resolver.
Do not invoke a nested reasoning CLI for producer work.

If a bridge error occurs, stop on its structured code. Restart the exact-version
sidecar for `UNAVAILABLE`; repair the reviewed configuration for version/model
drift; create a fresh request for replay; and treat attestation, result, or
sandbox-policy failures as security blockers. Never switch to `native-direct`
silently after a bridge failure or switch to the bridge silently after a native
failure.

The actionable fail-closed codes are:

- `RESEARCH_NESTED_SANDBOX_UNSUPPORTED`: the explicitly selected
  `native-direct` capsule cannot start inside the current outer sandbox.
- `RESEARCH_REVIEW_BRIDGE_UNAVAILABLE`: start or restore the exact-version
  owner-controlled sidecar, then rerun status.
- `RESEARCH_REVIEW_BRIDGE_VERSION_MISMATCH`: align the active CLI, runtime lock,
  client binding, and sidecar exact version.
- `RESEARCH_REVIEW_BRIDGE_ATTESTATION_INVALID`: stop; the workspace, request,
  signer, hash, or signature binding is invalid.
- `RESEARCH_REVIEW_BRIDGE_SANDBOX_POLICY_INVALID`: stop; the platform capsule,
  negative probes, declared tool-free policy or packet-read policy, or network
  policy is not the reviewed one.
- `RESEARCH_REVIEW_BRIDGE_MODEL_MISMATCH`: stop; use the exact configured
  reviewer family/model and rerun doctor.
- `RESEARCH_REVIEW_BRIDGE_NONCE_REPLAY`: create one fresh request; never retry
  or copy the rejected protocol bytes.
- `RESEARCH_REVIEW_BRIDGE_RESULT_BINDING_INVALID`: stop; the capsule, response,
  or exact result no longer matches the signed request.

Do not hide any of these codes behind a generic retry loop.

## Host approval and task pauses

An individual operation's approval rejection and an asynchronous task pause are
different controls. For an operation rejection, report the rejected action and
stated reason, then resolve that specific recipient/material or permission gap
using existing authorization where applicable. A later task pause does not undo
the evidence that the earlier gap was resolved, or reveal its own hidden cause.

For a Codex task pause, inspect the notice and available findings. Continue only
through the host's supported resume flow after reviewing whether the task can
safely proceed; an ended task or a surface without resume cannot be resumed
there. Preserve the packet, receipts and uncertainty about past requests. The
CLI can correct its routing/attestation layer; repeated calls, larger budgets,
provider switches or a fresh task are not documented cures for host monitoring.
Do not infer a provider ban or disable controls. If findings are insufficient,
report the specific observable limitation with sanitized context for upstream
diagnosis. See [OpenAI's safety-monitoring guidance](https://learn.chatgpt.com/docs/agent-approvals-security#safety-monitoring-and-paused-tasks).
