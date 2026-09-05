#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import {
  existsSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REQUIREMENT_SCHEMA_VERSION_V1 =
  "tiangong.data.skill-capability-requirement.v1";
const REQUIREMENT_SCHEMA_VERSION_V2 =
  "tiangong.data.skill-capability-requirement.v2";
const PROVENANCE_SCHEMA_VERSION =
  "tiangong.data.skill-migration-provenance.v1";
const DESCRIBE_SCHEMA_VERSION = "tiangong.data.describe.v1";
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const SEMVER_PATTERN = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const CONTRACT_VERSION_PATTERN = /^(0|[1-9]\d*)$/;
const REQUIREMENT_FIELDS = new Set([
  "schemaVersion",
  "skillName",
  "capabilityId",
  "capabilityContractVersion",
  "operations",
]);
const REQUIREMENT_OPERATION_FIELDS_V1 = new Set(["contractVersion"]);
const REQUIREMENT_OPERATION_FIELDS_V2 = new Set([
  "contractVersion",
  "requiredFeatures",
]);
const PROVENANCE_FIELDS = new Set([
  "schemaVersion",
  "generatedWithCliVersion",
  "skills",
]);
const PROVENANCE_SKILL_FIELDS = new Set([
  "skillName",
  "capabilityId",
  "capabilityVersion",
  "minimumCliVersion",
  "manifestDigest",
  "operations",
]);
const PROVENANCE_OPERATION_FIELDS = new Set([
  "operationId",
  "operationVersion",
  "inputSchemaId",
  "inputSchemaDigest",
  "outputSchemaId",
  "outputSchemaDigest",
]);

function fail(message) {
  throw new Error(message);
}

function assertPlainObject(value, label) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    fail(`${label} must be an object.`);
  }
  return value;
}

function assertClosedObject(value, allowedFields, label) {
  const object = assertPlainObject(value, label);
  for (const key of Object.keys(object)) {
    if (!allowedFields.has(key)) {
      fail(`Unexpected ${label} field: ${key}`);
    }
  }
  return object;
}

function requireString(value, label) {
  if (typeof value !== "string" || value.length === 0) {
    fail(`${label} must be a non-empty string.`);
  }
  return value;
}

function requireDigest(value, label) {
  const digest = requireString(value, label);
  if (!DIGEST_PATTERN.test(digest)) {
    fail(`${label} must be a lowercase SHA-256 digest.`);
  }
  return digest;
}

function parseSemver(value, label) {
  const version = requireString(value, label);
  const match = SEMVER_PATTERN.exec(version);
  if (!match) {
    fail(`${label} must be an exact x.y.z version.`);
  }
  return match.slice(1).map(Number);
}

function contractVersion(value, label) {
  const version = requireString(value, label);
  if (!CONTRACT_VERSION_PATTERN.test(version)) {
    fail(`${label} must be a non-negative major version.`);
  }
  return version;
}

function featureList(value, label) {
  if (!Array.isArray(value) || value.length === 0) {
    fail(`${label} must be a non-empty feature array.`);
  }
  const features = value.map((feature, index) =>
    requireString(feature, `${label}[${index}]`),
  );
  if (
    features.some(
      (feature) => !/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/.test(feature),
    ) ||
    new Set(features).size !== features.length ||
    [...features].sort().some((feature, index) => feature !== features[index])
  ) {
    fail(`${label} must contain sorted unique feature IDs.`);
  }
  return features;
}

function semverContractVersion(value, label) {
  return String(parseSemver(value, label)[0]);
}

function manifestFromDescribe(describe) {
  const envelope = assertPlainObject(describe, "describe envelope");
  if (envelope.schemaVersion !== DESCRIBE_SCHEMA_VERSION) {
    fail(
      `describe schemaVersion must be ${DESCRIBE_SCHEMA_VERSION}, got ${String(envelope.schemaVersion)}.`,
    );
  }
  const manifest = assertPlainObject(envelope.manifest, "describe manifest");
  if (!Array.isArray(manifest.operations)) {
    fail("describe manifest.operations must be an array.");
  }
  return manifest;
}

function operationSnapshot(operation) {
  const item = assertPlainObject(operation, "manifest operation");
  const inputSchema = assertPlainObject(
    item.inputSchema,
    "manifest operation inputSchema",
  );
  const outputSchema = assertPlainObject(
    item.outputSchema,
    "manifest operation outputSchema",
  );
  return {
    operationId: requireString(item.operationId, "operationId"),
    operationVersion: requireString(
      item.operationVersion,
      "operationVersion",
    ),
    inputSchemaId: requireString(inputSchema.schemaId, "inputSchemaId"),
    inputSchemaDigest: requireDigest(
      inputSchema.digest,
      "inputSchemaDigest",
    ),
    outputSchemaId: requireString(outputSchema.schemaId, "outputSchemaId"),
    outputSchemaDigest: requireDigest(
      outputSchema.digest,
      "outputSchemaDigest",
    ),
  };
}

function selectedOperations(manifest, operationIds) {
  const requested = [...new Set(operationIds)].sort();
  if (requested.length === 0) {
    fail("At least one operation ID is required.");
  }
  return requested.map((operationId) => {
    const operation = manifest.operations.find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!operation) {
      fail(`Capability does not publish operationId ${operationId}.`);
    }
    return operation;
  });
}

export function validateDataSkillRequirement(requirement) {
  const value = assertClosedObject(
    requirement,
    REQUIREMENT_FIELDS,
    "requirement",
  );
  if (
    value.schemaVersion !== REQUIREMENT_SCHEMA_VERSION_V1 &&
    value.schemaVersion !== REQUIREMENT_SCHEMA_VERSION_V2
  ) {
    fail(
      `requirement schemaVersion must be ${REQUIREMENT_SCHEMA_VERSION_V1} or ${REQUIREMENT_SCHEMA_VERSION_V2}, got ${String(value.schemaVersion)}.`,
    );
  }
  requireString(value.skillName, "skillName");
  requireString(value.capabilityId, "capabilityId");
  contractVersion(
    value.capabilityContractVersion,
    "capabilityContractVersion",
  );
  const operations = assertPlainObject(value.operations, "requirement operations");
  if (Object.keys(operations).length === 0) {
    fail("requirement operations must not be empty.");
  }
  let hasRequiredFeatures = false;
  for (const [operationId, operation] of Object.entries(operations)) {
    requireString(operationId, "operationId");
    const item = assertClosedObject(
      operation,
      value.schemaVersion === REQUIREMENT_SCHEMA_VERSION_V2
        ? REQUIREMENT_OPERATION_FIELDS_V2
        : REQUIREMENT_OPERATION_FIELDS_V1,
      `requirement operation ${operationId}`,
    );
    contractVersion(item.contractVersion, `${operationId}.contractVersion`);
    if (item.requiredFeatures !== undefined) {
      featureList(item.requiredFeatures, `${operationId}.requiredFeatures`);
      hasRequiredFeatures = true;
    }
  }
  if (value.schemaVersion === REQUIREMENT_SCHEMA_VERSION_V2 && !hasRequiredFeatures) {
    fail("A v2 requirement must declare at least one required feature.");
  }
  return value;
}

export function buildDataSkillRequirement({
  skillName,
  describe,
  operationIds,
  requiredFeatures = {},
}) {
  requireString(skillName, "skillName");
  const manifest = manifestFromDescribe(describe);
  const selected = selectedOperations(manifest, operationIds);
  const selectedIds = new Set(selected.map((operation) => operation.operationId));
  for (const operationId of Object.keys(requiredFeatures)) {
    if (!selectedIds.has(operationId)) {
      fail(`Required features reference unselected operationId ${operationId}.`);
    }
  }
  let usesFeatures = false;
  const operations = Object.fromEntries(
    selected.map((operation) => {
      const features = requiredFeatures[operation.operationId];
      const normalizedFeatures =
        features === undefined
          ? []
          : [...featureList([...features].sort(), `${operation.operationId}.requiredFeatures`)];
      const publishedFeatures = new Set(operation.features ?? []);
      for (const feature of normalizedFeatures) {
        if (!publishedFeatures.has(feature)) {
          fail(
            `${operation.operationId} does not publish required feature ${feature}.`,
          );
        }
      }
      usesFeatures ||= normalizedFeatures.length > 0;
      return [
        operation.operationId,
        {
          contractVersion: semverContractVersion(
            operation.operationVersion,
            `${operation.operationId}.operationVersion`,
          ),
          ...(normalizedFeatures.length === 0
            ? {}
            : { requiredFeatures: normalizedFeatures }),
        },
      ];
    }),
  );
  return {
    schemaVersion: usesFeatures
      ? REQUIREMENT_SCHEMA_VERSION_V2
      : REQUIREMENT_SCHEMA_VERSION_V1,
    skillName,
    capabilityId: requireString(manifest.capabilityId, "capabilityId"),
    capabilityContractVersion: semverContractVersion(
      manifest.capabilityVersion,
      "capabilityVersion",
    ),
    operations,
  };
}

export function verifyDataSkillRequirement({ requirement, describe }) {
  const expected = validateDataSkillRequirement(requirement);
  const manifest = manifestFromDescribe(describe);
  if (manifest.capabilityId !== expected.capabilityId) {
    fail(
      `capabilityId drift: expected ${expected.capabilityId}, got ${String(manifest.capabilityId)}.`,
    );
  }
  const actualCapabilityContract = semverContractVersion(
    manifest.capabilityVersion,
    "capabilityVersion",
  );
  if (actualCapabilityContract !== expected.capabilityContractVersion) {
    fail(
      `capability contract drift: expected major ${expected.capabilityContractVersion}, got ${actualCapabilityContract}.`,
    );
  }
  for (const [operationId, operationRequirement] of Object.entries(
    expected.operations,
  )) {
    const operation = manifest.operations.find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!operation) {
      fail(`operationId drift: missing ${operationId}.`);
    }
    const actualOperationContract = semverContractVersion(
      operation.operationVersion,
      `${operationId}.operationVersion`,
    );
    if (actualOperationContract !== operationRequirement.contractVersion) {
      fail(
        `${operationId} contract drift: expected major ${operationRequirement.contractVersion}, got ${actualOperationContract}.`,
      );
    }
    const publishedFeatures = new Set(operation.features ?? []);
    const missingFeatures = (operationRequirement.requiredFeatures ?? []).filter(
      (feature) => !publishedFeatures.has(feature),
    );
    if (missingFeatures.length > 0) {
      fail(
        `${operationId} feature drift: missing ${missingFeatures.join(", ")}.`,
      );
    }
  }
}

export function buildMigrationProvenanceEntry({
  skillName,
  describe,
  operationIds,
}) {
  const manifest = manifestFromDescribe(describe);
  return {
    skillName: requireString(skillName, "skillName"),
    capabilityId: requireString(manifest.capabilityId, "capabilityId"),
    capabilityVersion: requireString(
      manifest.capabilityVersion,
      "capabilityVersion",
    ),
    minimumCliVersion: requireString(
      manifest.minimumCliVersion,
      "minimumCliVersion",
    ),
    manifestDigest: requireDigest(manifest.manifestDigest, "manifestDigest"),
    operations: selectedOperations(manifest, operationIds).map(operationSnapshot),
  };
}

function validateProvenanceEntry(entry) {
  const value = assertClosedObject(
    entry,
    PROVENANCE_SKILL_FIELDS,
    "provenance skill",
  );
  requireString(value.skillName, "skillName");
  requireString(value.capabilityId, "capabilityId");
  parseSemver(value.capabilityVersion, "capabilityVersion");
  parseSemver(value.minimumCliVersion, "minimumCliVersion");
  requireDigest(value.manifestDigest, "manifestDigest");
  if (!Array.isArray(value.operations) || value.operations.length === 0) {
    fail("provenance operations must be a non-empty array.");
  }
  const operationIds = new Set();
  for (const operation of value.operations) {
    const item = assertClosedObject(
      operation,
      PROVENANCE_OPERATION_FIELDS,
      "provenance operation",
    );
    const normalized = operationSnapshot({
      operationId: item.operationId,
      operationVersion: item.operationVersion,
      inputSchema: {
        schemaId: item.inputSchemaId,
        digest: item.inputSchemaDigest,
      },
      outputSchema: {
        schemaId: item.outputSchemaId,
        digest: item.outputSchemaDigest,
      },
    });
    if (operationIds.has(normalized.operationId)) {
      fail(`Duplicate provenance operationId: ${normalized.operationId}.`);
    }
    operationIds.add(normalized.operationId);
  }
  return value;
}

export function validateMigrationProvenance(provenance) {
  const value = assertClosedObject(
    provenance,
    PROVENANCE_FIELDS,
    "provenance",
  );
  if (value.schemaVersion !== PROVENANCE_SCHEMA_VERSION) {
    fail(
      `provenance schemaVersion must be ${PROVENANCE_SCHEMA_VERSION}, got ${String(value.schemaVersion)}.`,
    );
  }
  parseSemver(value.generatedWithCliVersion, "generatedWithCliVersion");
  if (!Array.isArray(value.skills) || value.skills.length === 0) {
    fail("provenance skills must be a non-empty array.");
  }
  const skillNames = new Set();
  for (const entry of value.skills) {
    const item = validateProvenanceEntry(entry);
    if (skillNames.has(item.skillName)) {
      fail(`Duplicate provenance skillName: ${item.skillName}.`);
    }
    skillNames.add(item.skillName);
  }
  return value;
}

export function buildMigrationProvenance({ cliVersion, skills }) {
  parseSemver(cliVersion, "CLI version");
  return validateMigrationProvenance({
    schemaVersion: PROVENANCE_SCHEMA_VERSION,
    generatedWithCliVersion: cliVersion,
    skills: [...skills].sort((left, right) =>
      left.skillName.localeCompare(right.skillName),
    ),
  });
}

function compareField(actual, expected, label) {
  if (actual !== expected) {
    fail(`${label} drift: expected ${expected}, got ${String(actual)}.`);
  }
}

export function verifyMigrationProvenanceEntry({ entry, describe }) {
  const expected = validateProvenanceEntry(entry);
  const actual = buildMigrationProvenanceEntry({
    skillName: expected.skillName,
    describe,
    operationIds: expected.operations.map((operation) => operation.operationId),
  });
  for (const field of [
    "skillName",
    "capabilityId",
    "capabilityVersion",
    "minimumCliVersion",
    "manifestDigest",
  ]) {
    compareField(actual[field], expected[field], field);
  }
  for (let index = 0; index < expected.operations.length; index += 1) {
    for (const field of PROVENANCE_OPERATION_FIELDS) {
      compareField(
        actual.operations[index]?.[field],
        expected.operations[index][field],
        `${expected.skillName}.${expected.operations[index].operationId}.${field}`,
      );
    }
  }
}

function parseArguments(argv) {
  const [command, ...tokens] = argv;
  const commands = new Set([
    "generate",
    "verify",
    "generate-provenance",
    "verify-provenance",
  ]);
  if (!command || !commands.has(command)) {
    fail(
      "Expected command generate, verify, generate-provenance, or verify-provenance.",
    );
  }
  const options = {};
  for (let index = 0; index < tokens.length; index += 2) {
    const flag = tokens[index];
    const value = tokens[index + 1];
    if (!flag?.startsWith("--") || value === undefined) {
      fail(`Invalid argument near ${String(flag)}.`);
    }
    options[flag.slice(2)] = value;
  }
  return { command, options };
}

function requireOption(options, name) {
  return requireString(options[name], `--${name}`);
}

function runExactCli(cliVersion, packageSpecifier, args) {
  const result = spawnSync(
    "npx",
    [
      "--yes",
      "--package",
      packageSpecifier,
      "--",
      "tiangong-ai",
      ...args,
    ],
    {
      encoding: "utf8",
      env: { ...process.env, NO_COLOR: "1" },
      maxBuffer: 32 * 1024 * 1024,
    },
  );
  if (result.error) {
    fail(`Unable to run exact CLI ${cliVersion}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const detail = (result.stderr || result.stdout || "unknown error").trim();
    fail(`Exact CLI ${cliVersion} failed: ${detail}`);
  }
  return result.stdout.trim();
}

function loadExactDescribe(options, capabilityId) {
  const cliVersion = requireOption(options, "cli-version");
  parseSemver(cliVersion, "--cli-version");
  const packageSpecifier =
    options.package ?? `@tiangong-ai/cli@${cliVersion}`;
  const actualVersion = runExactCli(cliVersion, packageSpecifier, ["--version"]);
  if (actualVersion !== cliVersion) {
    fail(
      `Exact CLI package reported ${actualVersion}; expected ${cliVersion}.`,
    );
  }
  const output = runExactCli(cliVersion, packageSpecifier, [
    "data",
    "describe",
    capabilityId,
    "--json",
  ]);
  try {
    return { cliVersion, describe: JSON.parse(output) };
  } catch (error) {
    fail(`Exact CLI describe output is not JSON: ${error.message}`);
  }
}

function readSkillName(skillPath) {
  const skillFile = resolve(skillPath, "SKILL.md");
  const text = readFileSync(skillFile, "utf8");
  const frontmatter = /^---\n([\s\S]*?)\n---\n/.exec(text)?.[1];
  const name = frontmatter && /^name:\s*([^\s]+)\s*$/m.exec(frontmatter)?.[1];
  return requireString(name, `${skillFile} frontmatter name`);
}

function writeJsonAtomically(path, value) {
  const outputPath = resolve(path);
  const temporaryPath = resolve(
    dirname(outputPath),
    `.${basename(outputPath)}.tmp-${process.pid}`,
  );
  writeFileSync(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  renameSync(temporaryPath, outputPath);
}

function generateCommand(options) {
  const skillPath = resolve(requireOption(options, "skill"));
  const capabilityId = requireOption(options, "capability");
  const operationIds = requireOption(options, "operations")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const { describe } = loadExactDescribe(options, capabilityId);
  const requirement = buildDataSkillRequirement({
    skillName: readSkillName(skillPath),
    describe,
    operationIds,
    requiredFeatures: parseRequiredFeatures(options["required-features"]),
  });
  const outputPath =
    options.output ??
    resolve(skillPath, "references", "tiangong-data-requirement.json");
  writeJsonAtomically(outputPath, requirement);
  process.stdout.write(`${resolve(outputPath)}\n`);
}

function parseRequiredFeatures(value) {
  if (value === undefined) return {};
  const result = {};
  for (const entry of requireString(value, "--required-features").split(";")) {
    const separator = entry.indexOf("=");
    if (separator <= 0 || separator === entry.length - 1) {
      fail(
        "--required-features must use operation=feature[,feature][;operation=feature].",
      );
    }
    const operationId = entry.slice(0, separator).trim();
    if (result[operationId]) {
      fail(`Duplicate required feature operationId ${operationId}.`);
    }
    result[operationId] = entry
      .slice(separator + 1)
      .split(",")
      .map((feature) => feature.trim())
      .filter(Boolean);
  }
  return result;
}

function verifyCommand(options) {
  const requirementPath = resolve(requireOption(options, "requirement"));
  const requirement = JSON.parse(readFileSync(requirementPath, "utf8"));
  const { describe } = loadExactDescribe(
    options,
    requireString(requirement.capabilityId, "requirement capabilityId"),
  );
  verifyDataSkillRequirement({ requirement, describe });
  process.stdout.write(`${requirementPath}: compatible\n`);
}

function readRepositoryRequirements(root) {
  const requirements = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const path = resolve(
      root,
      entry.name,
      "references",
      "tiangong-data-requirement.json",
    );
    if (!existsSync(path)) continue;
    requirements.push({
      path,
      value: validateDataSkillRequirement(
        JSON.parse(readFileSync(path, "utf8")),
      ),
    });
  }
  return requirements.sort((left, right) =>
    left.value.skillName.localeCompare(right.value.skillName),
  );
}

function generateProvenanceCommand(options) {
  const root = resolve(options.root ?? ".");
  const requirements = readRepositoryRequirements(root);
  const skills = requirements.map(({ value }) => {
    const { describe } = loadExactDescribe(options, value.capabilityId);
    return buildMigrationProvenanceEntry({
      skillName: value.skillName,
      describe,
      operationIds: Object.keys(value.operations),
    });
  });
  const provenance = buildMigrationProvenance({
    cliVersion: requireOption(options, "cli-version"),
    skills,
  });
  const outputPath =
    options.output ?? resolve(root, "scripts", "data-skill-migration-provenance.json");
  writeJsonAtomically(outputPath, provenance);
  process.stdout.write(`${resolve(outputPath)}\n`);
}

function verifyProvenanceCommand(options) {
  const root = resolve(options.root ?? ".");
  const provenancePath = resolve(
    options.provenance ??
      resolve(root, "scripts", "data-skill-migration-provenance.json"),
  );
  const provenance = validateMigrationProvenance(
    JSON.parse(readFileSync(provenancePath, "utf8")),
  );
  const cliVersion = requireOption(options, "cli-version");
  if (cliVersion !== provenance.generatedWithCliVersion) {
    fail(
      `CLI version ${cliVersion} does not match migration provenance ${provenance.generatedWithCliVersion}.`,
    );
  }
  const requirements = new Map(
    readRepositoryRequirements(root).map(({ value }) => [value.skillName, value]),
  );
  if (requirements.size !== provenance.skills.length) {
    fail(
      `Migration provenance contains ${provenance.skills.length} skills but the repository contains ${requirements.size} data skill requirements.`,
    );
  }
  for (const entry of provenance.skills) {
    const requirement = requirements.get(entry.skillName);
    if (!requirement) {
      fail(`Missing requirement for provenance skill ${entry.skillName}.`);
    }
    const { describe } = loadExactDescribe(options, entry.capabilityId);
    verifyDataSkillRequirement({ requirement, describe });
    verifyMigrationProvenanceEntry({ entry, describe });
  }
  process.stdout.write(`${provenancePath}: exact migration provenance valid\n`);
}

function main() {
  const { command, options } = parseArguments(process.argv.slice(2));
  if (command === "generate") generateCommand(options);
  else if (command === "verify") verifyCommand(options);
  else if (command === "generate-provenance") {
    generateProvenanceCommand(options);
  } else {
    verifyProvenanceCommand(options);
  }
}

const entryPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (entryPath === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
