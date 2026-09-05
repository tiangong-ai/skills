#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmod, cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const skillsRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const skillNames = [
  "tiangong-auto-research",
  "tiangong-auto-research-workbuddy",
  "tiangong-kb-sci-search",
  "tiangong-kb-report-search",
  "tiangong-kb-patent-search",
];

async function description(skillName) {
  const text = await readFile(join(skillsRoot, skillName, "SKILL.md"), "utf8");
  const match = text.match(/^description:\s*(.+)$/m);
  assert.ok(match, `${skillName} must have a one-line routing description`);
  return match[1].toLowerCase();
}

const descriptions = Object.fromEntries(
  await Promise.all(skillNames.map(async (name) => [name, await description(name)])),
);

const autoResearchSkill = await readFile(
  join(skillsRoot, "tiangong-auto-research", "SKILL.md"),
  "utf8",
);
const setupReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "setup.md"),
  "utf8",
);
const environmentReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "env.md"),
  "utf8",
);
const evidencePipelineReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "evidence-pipeline.md"),
  "utf8",
);
const publicationReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "publication-policy.md"),
  "utf8",
);
const sandboxedIdeReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "sandboxed-ide.md"),
  "utf8",
);
const scientificDesignReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "scientific-design.md"),
  "utf8",
);
const nativeExecutionReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "native-execution.md"),
  "utf8",
);

// Test the installed command recipes and their ordering, not a second runtime
// implementation. Behavioral decisions are evaluated independently before release.
function researchRecipes(reference) {
  return [...reference.matchAll(/```bash\n([\s\S]*?)```/g)].flatMap((match) =>
    match[1].replace(/\\\n\s*/g, " ").split("\n")
      .filter((line) => line.startsWith('node "$AUTO_RESEARCH_CLI"'))
      .map((line) => line.slice(line.indexOf(" -- ") + 4).trim()),
  );
}
const evidenceRecipes = researchRecipes(evidencePipelineReference);
const forecastRecipe = evidenceRecipes.findIndex((line) => line.startsWith("research project evidence content forecast "));
const freezeRecipe = evidenceRecipes.findIndex((line) => line.startsWith("research project evidence content freeze "));
assert.ok(forecastRecipe >= 0 && forecastRecipe < freezeRecipe,
  "The installed acquisition recipe must forecast before the immutable typed-content boundary");
for (const prefix of [
  "research project evidence artifact preflight ",
  "research project evidence decomposition batch ",
  "research project evidence atom batch ",
]) assert.ok(evidenceRecipes.some((line) => line.startsWith(prefix)), `Missing public efficient recipe: ${prefix}`);
const scientificRecipes = researchRecipes(scientificDesignReference);
assert.ok(scientificRecipes.some((line) => line.startsWith("research project scientific review execute ") && line.includes("--confirm-review-cost")),
  "The installed scientific-review recipe must use explicit isolated execution with cost consent");
assert.ok(scientificRecipes.some((line) => line.startsWith("research project fork ") && line.includes("--resume-through discover")),
  "Acquisition recovery must preserve discovery through the supported fork boundary");
assert.match(nativeExecutionReference, /scientific-stopped/u,
  "Native routing must distinguish stopped scientific work from a producer action");

const questionGateHeading = "## Gate the research question before acting";
const questionGateIndex = autoResearchSkill.indexOf(questionGateHeading);
const firstManagedCommandIndex = autoResearchSkill.indexOf(
  "For an existing managed directory",
);
assert.ok(questionGateIndex > 0, "Auto Research must define the research-question gate");
assert.ok(
  questionGateIndex < firstManagedCommandIndex,
  "The research-question gate must run before setup, resolver, or other tool instructions",
);
const normalizedQuestionGate = autoResearchSkill
  .slice(questionGateIndex, firstManagedCommandIndex)
  .toLowerCase()
  .replace(/\s+/g, " ");
for (const marker of [
  "before any cli, browser, search, database, or file operation",
  "do not call tools or begin setup",
  "one testable rewrite",
  "explicit confirmation",
  "directional hypothesis",
  "null results",
  "alternative explanations",
  "counterevidence",
  "fabricate, conceal, or misrepresent evidence",
]) {
  assert.ok(
    normalizedQuestionGate.includes(marker),
    `Research-question gate must preserve the observable behavior: ${marker}`,
  );
}

for (const marker of [
  ".tiangong-research/setup.yaml",
  "research setup init",
  "never scans parent directories",
  "overallReadiness",
]) {
  assert.ok(autoResearchSkill.includes(marker), `Auto Research entry must explain ${marker}`);
}

for (const marker of [
  "runDataCapability",
  "standalone `data run`",
  "dynamic data catalog",
]) {
  assert.ok(
    autoResearchSkill.includes(marker),
    `Auto Research entry must explain native data evidence marker ${marker}`,
  );
}

const managedDataGuidance = `${autoResearchSkill}\n${evidencePipelineReference}`;
assert.doesNotMatch(autoResearchSkill, /later stages are tool-free/u,
  "Acquisition must retain packet-governed download/parsing operations after discovery");
assert.match(
  managedDataGuidance,
  /node "\$AUTO_RESEARCH_CLI"[\s\S]*?--[\s\\\n]+data describe <capability-id> --json/,
  "Managed Auto Research must inspect data capabilities through the locked resolver",
);
assert.doesNotMatch(
  managedDataGuidance,
  /`tiangong-ai data describe|^tiangong-ai data describe/m,
  "Managed Auto Research must not expose a bare data describe command",
);

for (const marker of [
  "native-direct",
  "sandbox-bridge",
  "Default Permission",
  "research reviewer serve",
  "research reviewer status",
  "research reviewer doctor",
  "--host-agent workbuddy",
  "no arbitrary-command action",
  "Never switch",
  "RESEARCH_REVIEW_BRIDGE_UNAVAILABLE",
  "RESEARCH_REVIEW_BRIDGE_VERSION_MISMATCH",
  "RESEARCH_REVIEW_BRIDGE_ATTESTATION_INVALID",
  "RESEARCH_REVIEW_BRIDGE_SANDBOX_POLICY_INVALID",
  "RESEARCH_REVIEW_BRIDGE_MODEL_MISMATCH",
  "RESEARCH_REVIEW_BRIDGE_NONCE_REPLAY",
  "RESEARCH_REVIEW_BRIDGE_RESULT_BINDING_INVALID",
]) {
  assert.ok(
    sandboxedIdeReference.includes(marker),
    `Sandboxed IDE reference must explain ${marker}`,
  );
}

for (const marker of [
  "## Declarative clean-directory setup",
  "schemaVersion: 2",
  ".tiangong-research/setup.env.example",
  "selection.skills",
  "all current catalog Skills",
  "requirement",
  "enabled: false",
  "replaceExistingPlan: true",
  "does not fall back to the Wizard",
  "overallReadiness=READY",
]) {
  assert.ok(setupReference.includes(marker), `Setup reference must explain ${marker}`);
}


for (const marker of [
  "workbuddy",
  "codebuddy",
  "default permission",
  "sandbox-bridge",
  "thin router",
]) {
  assert.ok(
    descriptions["tiangong-auto-research-workbuddy"].includes(marker),
    `WorkBuddy adapter routing description must include ${marker}`,
  );
}

for (const marker of [
  ".tiangong-research/setup.env",
  "chmod 600",
  "literal `NAME=value`",
  "all catalog credentials",
  "disabled credential",
  "must not differ",
  "owner-only logical stores",
]) {
  assert.ok(environmentReference.includes(marker), `Environment reference must explain ${marker}`);
}

for (const marker of [
  "evidence decomposition record",
  "evidence atom register",
  "evidence content freeze",
  "inference-snapshot.json",
  "claim-evidence-graph.json",
  "evidencePipeline",
  "structured data capabilities",
  "research project evidence data run",
  "core receipt digest",
  "data-runtime receipt",
]) {
  assert.ok(
    evidencePipelineReference.includes(marker),
    `Evidence pipeline reference must explain ${marker}`,
  );
}

const structuredDataReviewGuidance =
  `${autoResearchSkill}\n${evidencePipelineReference}\n${nativeExecutionReference}`;
for (const marker of [
  "providerCoverage",
  "limitCoverage",
  "contextView.nextCursor",
  "runDataCapability.readArgv",
  "workspace-cli-relative-argv",
  "prefix its resolver-relative",
  "suspended capabilities are not projected",
  "nextCursor is null",
  "presented fraction",
  "does not consume another provider call",
]) {
  assert.ok(
    structuredDataReviewGuidance.includes(marker),
    `Managed structured-data review must explain ${marker}`,
  );
}

for (const marker of [
  "--submission",
  "reporting-checklist",
  "source-data",
  "Claim-Evidence Graph",
  "submissionPackageSha256",
]) {
  assert.ok(
    publicationReference.includes(marker),
    `Publication reference must explain ${marker}`,
  );
}

for (const marker of [
  "open-ended",
  "multi-source",
  "current native",
  "independent review",
  ".tiangong-research",
  "takes precedence",
  "研究一下",
  "朝这个方向做一做",
  "结合已有成果继续研究",
  "查资料并形成结论",
  "系统梳理证据",
]) {
  assert.ok(
    descriptions["tiangong-auto-research"].includes(marker),
    `Auto Research routing description must include ${marker}`,
  );
}

for (const source of ["sci", "report", "patent"]) {
  const skill = `tiangong-kb-${source}-search`;
  for (const marker of [
    "one isolated",
    ".tiangong-research",
    "route to `tiangong-auto-research`",
    "execution_mode=standalone",
  ]) {
    assert.ok(descriptions[skill].includes(marker), `${skill} must include ${marker}`);
  }
}

function expectedRoute(prompt, managedWorkspace) {
  const normalized = prompt.toLowerCase();
  const source = normalized.includes("sci")
    ? "sci"
    : normalized.includes("报告") || normalized.includes("report")
      ? "report"
      : normalized.includes("专利") || normalized.includes("patent")
        ? "patent"
        : null;
  const explicitlyIsolated =
    source !== null &&
    /(只|仅|one isolated|only).*(sci|报告|report|专利|patent)|(sci|报告|report|专利|patent).*(只|仅|only)/i.test(
      normalized,
    );
  const systematic =
    /(研究一下|朝这个方向做一做|结合.*已有成果|查资料.*结论|系统梳理|open-ended|multi-source|investigate|form a conclusion|reviewed research artifact)/i.test(
      normalized,
    );
  if (explicitlyIsolated) {
    return `tiangong-kb-${source}-search`;
  }
  if (managedWorkspace || systematic) {
    return "tiangong-auto-research";
  }
  return source ? `tiangong-kb-${source}-search` : "unrelated";
}

const fixtures = [
  ["研究一下新能源汽车变重是否增加道路损伤，并形成结论", true, "tiangong-auto-research"],
  ["朝这个方向做一做", false, "tiangong-auto-research"],
  ["结合这个目录中的已有成果继续研究", true, "tiangong-auto-research"],
  ["查资料并形成结论/报告", false, "tiangong-auto-research"],
  ["系统梳理证据和应对措施", false, "tiangong-auto-research"],
  ["Investigate this as a multi-source reviewed research artifact", false, "tiangong-auto-research"],
  ["只在 SCI 库查询标题 X，返回前 5 条", true, "tiangong-kb-sci-search"],
  ["Only search the report database for title X", true, "tiangong-kb-report-search"],
  ["仅查询专利库中的申请号 X", true, "tiangong-kb-patent-search"],
];
for (const [prompt, managed, expected] of fixtures) {
  assert.equal(expectedRoute(prompt, managed), expected, prompt);
}

// Execute the installed recipes against a capture-only Node stand-in. This
// validates portable argv and consent binding, not model reasoning or CLI state.
const recipeFixture = await mkdtemp(join(tmpdir(), "research-task-recipes-"));
try {
  const installed = join(recipeFixture, "installed-skill");
  await cp(join(skillsRoot, "tiangong-auto-research"), installed, { recursive: true });
  const referencePath = join(installed, "references", "execution-assurance.md");
  const reference = await readFile(referencePath, "utf8");
  for (const marker of ["requestProvenance", "verbatim", "interpreted", "reconstructed", "unrecorded", "nativeRunSha256", "unverified-execution", "on-demand", "no total context-length"]) {
    assert.ok(reference.includes(marker), `Installed task assurance must explain ${marker}`);
  }
  assert.match(reference, /For CLI or Skill implementation changes only[\s\S]*?ordinary research work does not require these development tests/u,
    "Development TDD must not become a prerequisite for ordinary research actions");
  assert.doesNotMatch(scientificDesignReference, /Freezing previously pending[\s\S]*?is a material design change/u,
    "Predeclared pending slots must not automatically require a successor");
  assert.doesNotMatch(nativeExecutionReference, /Freeze replacements\s+through a new authoritative generation/u,
    "Native execution must expose the supported same-project fulfillment path");
  const installedEntry = await readFile(join(installed, "SKILL.md"), "utf8");
  assert.ok(installedEntry.includes("references/execution-assurance.md"),
    "The execution-assurance workflow must be discoverable from the installed Skill");
  for (const [, href] of reference.matchAll(/\[[^\]]+\]\(([^)#]+\.md)(?:#[^)]*)?\)/g)) {
    const target = resolve(dirname(referencePath), href);
    assert.ok(target.startsWith(installed + sep), "Reference must not escape the installed Skill");
    await readFile(target);
  }
  const bin = join(recipeFixture, "bin");
  await mkdir(bin);
  const trace = join(recipeFixture, "argv.jsonl");
  const standIn = join(bin, "node");
  await writeFile(standIn, `#!${process.execPath}\nconst fs = require("node:fs");\nfs.appendFileSync(process.env.TASK_RECIPE_TRACE, JSON.stringify(process.argv.slice(2)) + "\\n");\n`);
  await chmod(standIn, 0o700);
  const resolver = join(installed, "scripts", "research_cli.mjs");
  const recipes = [...reference.matchAll(/```bash\n([\s\S]*?)```/g)].flatMap((match) =>
    match[1].replace(/\\\n\s*/g, " ").split("\n").map((line) => line.trim()).filter((line) => line && !line.startsWith("#")));
  assert.ok(recipes.length > 0, "The workflow needs executable command examples");
  for (const line of recipes) {
    assert.ok(line.startsWith('node "$AUTO_RESEARCH_CLI" '), "Recipes must use the installed locked resolver");
    assert.ok(!/[`;|&<>]/.test(line) && !line.includes("$("), "Recipe must be a single bounded argv invocation");
    const run = spawnSync("sh", ["-eu", "-c", line], {encoding: "utf8", cwd: recipeFixture,
      env: {...process.env, PATH: bin + delimiter + process.env.PATH, AUTO_RESEARCH_CLI: resolver, TASK_RECIPE_TRACE: trace}});
    assert.equal(run.status, 0, run.stderr);
  }
  const invocations = (await readFile(trace, "utf8")).trim().split("\n").map(JSON.parse);
  for (const argv of invocations) {
    assert.equal(argv[0], resolver);
    assert.equal(argv[argv.indexOf("--workspace") + 1], argv[argv.lastIndexOf("--workspace") + 1]);
  }
  const commands = invocations.map((argv) => argv.slice(argv.indexOf("--") + 1));
  for (const prefix of [
    ["research", "project", "task", "define"],
    ["research", "project", "task", "status"],
    ["research", "project", "task", "scope", "propose"],
    ["research", "project", "task", "scope", "approve"],
    ["research", "project", "task", "acceptance", "record"],
    ["research", "project", "evidence", "acquisition", "revise"],
    ["research", "scientific", "fulfillment", "record"],
    ["research", "scientific", "fulfillment", "status"],
    ["research", "project", "task", "run", "observe"],
    ["research", "project", "task", "run", "inspect"],
    ["research", "project", "stage", "artifacts"],
    ["research", "project", "stage", "read"],
  ]) assert.ok(commands.some((argv) => prefix.every((part, index) => argv[index] === part)), `Missing executable recipe: ${prefix.join(" ")}`);
  const approval = commands.find((argv) => argv.includes("scope") && argv.includes("approve"));
  assert.equal(approval[approval.indexOf("--proposal") + 1], approval[approval.indexOf("--confirm-change") + 1]);
  const observation = commands.find(argv => argv.includes("run") && argv.includes("observe"));
  assert.ok(observation.includes("--confirm-execution"), "Ordinary calculation observation requires exact execution consent");
  const completeRead = commands.find(argv => argv.includes("stage") && argv.includes("read"));
  assert.equal(completeRead[completeRead.indexOf("--length") + 1], "all", "Whole-object reads must remain an explicit supported choice");
} finally {
  await rm(recipeFixture, { recursive: true, force: true });
}

process.stdout.write("Auto Research routing contract tests passed\n");
