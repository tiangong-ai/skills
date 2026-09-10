# Bounded native investigation

Use this workflow when an existing computational requirement needs several
related diagnostic trials, such as a Canary that stopped before the solver or
returned unusable numerical results. A single ordinary calculation still uses
[execution assurance](execution-assurance.md#observe-an-actual-native-calculation).
The current native host proposes hypotheses, interprets diagnostics and chooses
whether to continue. The CLI observes exact runs and maintains their authority,
bounds and immutable relationships; it never launches another producer.

These recipes require a locked CLI exposing the investigation commands. Inspect
its command/schema surface first. An older runtime follows the reviewed
[upgrade workflow](setup.md); do not switch silently to `latest` or recreate the
lifecycle in shell scripts. Read only the schema needed for the current action.

## Approve one finite investigation

Keep the requirement/version, question, Policy, design and canonical acquisition
inputs fixed. Prepare a finite plan of already-existing Node/Python interpreters,
reviewed program/environment files, permitted numeric or enumerated options,
required solver metrics/statuses, run count, per-run/total wall time, reserved
cost upper bounds and output byte limits. Include only the programs and options
needed to test concrete hypotheses. The isolation boundary forbids network,
installation, credentials, holdout and workspace writes; do not broaden it after
an execution denial. Environment locks are declarations, not hermetic attestation.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show investigation --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation plan PROJECT --input /absolute/path/investigation.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation approve PROJECT --input /absolute/path/investigation.json --confirm PLAN_SHA256 --authorization-source /absolute/path/investigation-approval.txt --workspace /absolute/path/to/workspace --json
```

Present the concrete returned plan for approval. Preserve the actual approval
source and exact plan hash; planning is read-only. Existing authorization for
that exact envelope covers its permitted low-risk trials without another user
confirmation for every run. General intent to solve a problem is not approval
for a new envelope, extra resources, changed scientific scope or promotion.

## Test a hypothesis and inspect the result

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show investigation-attempt --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation attempt PROJECT --input /absolute/path/attempt.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation status PROJECT --investigation INVESTIGATION_ID --workspace /absolute/path/to/workspace --json
```

Each attempt names a falsifiable hypothesis, the parent/baseline and only the
approved configuration changes relevant to it. Use the returned immutable record,
configuration difference, logs, telemetry and artifact bindings to explain what
was supported or ruled out and why another trial is useful. Declared diagnostic
files must carry the required solver metrics and statuses; an exit code or a
program's claim of feasibility is insufficient scientific validation.

Distinguish an admission/authorization rejection from an observed harness or
runtime failure, solver not reached, missing required diagnostics, numerical
failure and feasible candidate. Missing run/model status is diagnostic-incomplete,
not evidence of a nonoptimal solution. Trials remain exploratory: neither a
failure before the solver nor a feasible trial is a certification Canary result.
Do not turn unknown actual cost into zero; report observed time/output and the
CLI's reserved/accounted upper bounds accurately. Output guards bound captured
streams and sampled declared files, not every transient scratch write.

Inspect status at recovery or before making a continuation decision. Use
`authorizationValid`, `allowedNextAction`, remaining quotas and project wall
reservations rather than reconstructing them from chat. A completed attempt ID
replays its record. An unresolved start retains its reservation and prohibits
blind retry under a new ID. Inspect its observer information: a matching process
may still run, while a missing/nonmatching process does not establish a result
or recover unknown usage. Do not edit routing files, release reservations or
invent a completion record. The one-shot supervisor bounds admitted execution;
it does not make interrupted result commits recoverable evidence.

Stop when the hypothesis is answered, no useful authorized trial remains, any
limit is exhausted or violated, or status requires inspection/reauthorization.
Changed inputs, code outside the approved program set, scientific scope,
permissions or budgets need the appropriate explicit new approval. Close a
settled investigation with a concrete reason; closure does not discard its
history or turn an unsolved requirement into a negative scientific finding.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show investigation-close --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation close PROJECT --input /absolute/path/investigation-close.json --workspace /absolute/path/to/workspace --json
```

## Select, promote, freeze, then certify

Select an exact committed informative or successful attempt and explain why its
recipe merits a fresh certification. Selection alone is non-evidence; it cannot
answer the requirement, unlock inference or admit holdout/publication evidence.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show investigation-candidate --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation select PROJECT --input /absolute/path/candidate.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show investigation-promotion --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation promotion plan PROJECT --input /absolute/path/promotion.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project investigation promotion approve PROJECT --input /absolute/path/promotion.json --confirm PROMOTION_PLAN_SHA256 --authorization-source /absolute/path/promotion-approval.txt --workspace /absolute/path/to/workspace --json
```

Promotion needs its own exact approval and fresh certification allocation.
Inspect the returned route: unchanged predeclared pending model/environment
slots use public scientific-object registration and the planned
[fulfillment](execution-assurance.md#fulfill-only-predeclared-scientific-slots).
An already-frozen identical recipe can remain frozen. Changed frozen assumptions
or scientific design require the existing reviewed authoritative successor;
promotion does not create that successor or authorize its scientific changes.
Use the exact source candidate in the actual successor with the same canonical
inputs, then plan promotion there. Never patch a stored design to fit a candidate.

Approval alone does not freeze scientific objects. Complete the returned
fulfillment and inspect status until certification is the allowed next action.
Run the existing observed calculation with `investigationPromotionSha256` set
to the exact returned promotion record, using its frozen recipe, current inputs,
required telemetry and limits. Read `task-native-run` from the CLI and use the
[observe/inspect recipes](execution-assurance.md#observe-an-actual-native-calculation).
Do not recycle an exploratory output or ordinary run as this fresh Canary.

A failed certification remains failed and needs a new explicit promotion
approval before another certification. A passed certification still proceeds
through the existing independent scientific review and task-acceptance gates;
no extra fixed reviewer round is introduced. Bind acceptance to its exact
`nativeRunSha256`. Report separately the candidate, certification, independent
review, current requirement and original-task completion. Include failed and
incomplete trials and all selection/promotion links in the portable audit.
