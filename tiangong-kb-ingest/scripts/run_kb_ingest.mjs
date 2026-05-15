#!/usr/bin/env node
import { accessSync, constants, existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const skillDir = resolve(scriptDir, "..");
const workspaceCli = resolve(skillDir, "..", "..", "tiangong-ai-cli", "bin", "tiangong-ai.js");
const VALUE_FLAGS = new Set([
  "--api-key",
  "--api-base-url",
  "--api-path-prefix",
  "--timeout",
  "--collection-name",
  "--collection-key",
  "--collection-path",
  "--collection-id",
  "--state",
  "--metadata-map",
  "--metadata-map-output",
  "--schema-file",
  "--window-size",
  "--top-up-max",
  "--upload-concurrency",
  "--retries",
  "--poll-interval",
  "--max-polls",
  "--scan-budget",
  "--min-samples-per-pattern",
  "--max-patterns",
]);
const SELECTOR_AND_CONFIG_FLAGS = new Set([
  "--api-key",
  "--api-base-url",
  "--api-path-prefix",
  "--timeout",
  "--collection-name",
  "--collection-key",
  "--collection-path",
  "--collection-id",
  "--schema-file",
]);
const SKILL_ONLY_FLAGS = new Set([
  "--metadata-map-output",
  "--no-metadata-map-autogen",
  "--metadata-map-autogen",
]);

function isExecutable(path) {
  try {
    accessSync(path, constants.R_OK);
    return true;
  } catch {
    return false;
  }
}

function cliInvocation() {
  const explicitCli = process.env.TIANGONG_AI_CLI_BIN?.trim();
  if (explicitCli) {
    return explicitCli.endsWith(".js")
      ? { command: process.execPath, prefix: [explicitCli] }
      : { command: explicitCli, prefix: [] };
  }
  if (isExecutable(workspaceCli)) {
    return { command: process.execPath, prefix: [workspaceCli] };
  }
  return { command: "tiangong-ai", prefix: [] };
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    env: process.env,
    shell: false,
    encoding: options.capture ? "utf8" : undefined,
  });

  if (result.error) {
    if (options.capture) {
      throw result.error;
    }
    return { ok: false, status: 127 };
  }
  return {
    ok: (result.status ?? 1) === 0,
    status: result.status ?? 1,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
}

function runCli(args, options = {}) {
  const invocation = cliInvocation();
  return run(invocation.command, [...invocation.prefix, ...args], options);
}

function execCliJson(args) {
  const result = runCli(args, { capture: true });
  if (!result.ok) {
    process.stderr.write(result.stderr || `Command failed: tiangong-ai ${args.join(" ")}\n`);
    process.exit(result.status);
  }
  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    process.stderr.write(
      [
        `Expected JSON output from: tiangong-ai ${args.join(" ")}`,
        error instanceof Error ? error.message : String(error),
        result.stdout.slice(0, 1000),
        "",
      ].join("\n"),
    );
    process.exit(1);
  }
}

function exitWithCli(args) {
  const result = runCli(args);
  if (!result.ok && result.status === 127) {
    process.stderr.write(
      [
        "Unable to execute the Tiangong AI CLI.",
        "Install @tiangong-ai/cli, add tiangong-ai to PATH, or set TIANGONG_AI_CLI_BIN.",
        "",
      ].join("\n"),
    );
  }
  process.exit(result.status);
}

function parseItems(items) {
  const flags = new Map();
  const positionals = [];

  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item) continue;
    if (!item.startsWith("--")) {
      positionals.push(item);
      continue;
    }
    const equalsIndex = item.indexOf("=");
    const [name, inlineValue] =
      equalsIndex >= 0
        ? [item.slice(0, equalsIndex), item.slice(equalsIndex + 1)]
        : [item, undefined];
    if (inlineValue !== undefined) {
      flags.set(name, inlineValue);
      continue;
    }
    if (VALUE_FLAGS.has(name) && items[index + 1] && !items[index + 1].startsWith("--")) {
      flags.set(name, items[index + 1]);
      index += 1;
      continue;
    }
    flags.set(name, true);
  }

  return { flags, positionals };
}

function stripSkillOnlyFlags(items) {
  const output = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    const name = item?.startsWith("--") ? item.split("=", 1)[0] : "";
    if (SKILL_ONLY_FLAGS.has(name)) {
      if (!item.includes("=") && VALUE_FLAGS.has(name) && items[index + 1]) index += 1;
      continue;
    }
    output.push(item);
  }
  return output;
}

function flagArgs(items, allowed) {
  const output = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item?.startsWith("--")) continue;
    const name = item.split("=", 1)[0];
    if (!allowed.has(name)) {
      if (!item.includes("=") && VALUE_FLAGS.has(name) && items[index + 1]) index += 1;
      continue;
    }
    output.push(item);
    if (!item.includes("=") && VALUE_FLAGS.has(name) && items[index + 1]) {
      output.push(items[index + 1]);
      index += 1;
    }
  }
  return output;
}

function hasFlag(items, name) {
  return items.some((item) => item === name || item.startsWith(`${name}=`));
}

function getFlagValue(items, name) {
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (item === name) return items[index + 1];
    if (item?.startsWith(`${name}=`)) return item.slice(name.length + 1);
  }
  return undefined;
}

function bulkRunContext(items) {
  const first = items[0] ?? "";
  if (["scan", "preflight", "dry-run", "resume", "export", "metadata-dry-run"].includes(first)) {
    return undefined;
  }
  const parsed = parseItems(items);
  const positionals = parsed.positionals[0] === "run" ? parsed.positionals.slice(1) : parsed.positionals;
  const rootPath = positionals[0];
  if (!rootPath) return undefined;
  return { rootPath, parsed };
}

function responseData(payload) {
  return isObject(payload?.data) ? payload.data : payload;
}

function isObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function metadataSchemaFields(schemaSnapshot) {
  const data = responseData(schemaSnapshot);
  const collection = isObject(data) ? data.collection : undefined;
  const schema =
    (isObject(collection) ? collection.metadataSchema : undefined) ??
    (isObject(data) ? data.metadataSchema : undefined) ??
    (isObject(schemaSnapshot) ? schemaSnapshot.metadataSchema : undefined) ??
    schemaSnapshot;
  return isObject(schema) && Array.isArray(schema.fields) ? schema.fields.filter(isObject) : [];
}

function collectionName(schemaSnapshot) {
  const data = responseData(schemaSnapshot);
  const collection = isObject(data) ? data.collection : undefined;
  return typeof collection?.name === "string" && collection.name ? collection.name : undefined;
}

function enumValues(field) {
  return Array.isArray(field.values) ? field.values.filter((value) => typeof value === "string") : [];
}

function requiredDefault(field) {
  if (field.required !== true) return undefined;
  const values = enumValues(field);
  if (values.includes("other")) return "other";
  if (values.length > 0) return values[0];
  if (field.type === "number") return 0;
  if (field.type === "boolean") return false;
  if (field.type === "date") return "1970-01-01";
  return "unknown";
}

function buildMetadataMap(schemaSnapshot, scanSummary) {
  const fields = metadataSchemaFields(schemaSnapshot);
  const defaults = { source: "local_bulk_upload" };
  for (const field of fields) {
    const value = requiredDefault(field);
    if (value !== undefined && typeof field.key === "string") defaults[field.key] = value;
  }

  const layers = [
    {
      name: "base",
      merge: "all",
      rules: [
        {
          name: "filesystem",
          match: { glob: "**/*" },
          fields: {
            raw_relative_path: { source: "relative_path" },
          },
        },
      ],
    },
  ];

  return { version: 1, rule_mode: "layered", defaults, layers };
}

function yamlScalar(value) {
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (value === null) return "null";
  return JSON.stringify(String(value));
}

function toYaml(value, indent = 0) {
  const space = " ".repeat(indent);
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (isObject(item)) {
          const entries = Object.entries(item);
          if (entries.length === 0) return `${space}- {}`;
          const [[firstKey, firstValue], ...rest] = entries;
          const firstLine =
            isObject(firstValue) || Array.isArray(firstValue)
              ? `${space}- ${firstKey}:\n${toYaml(firstValue, indent + 4)}`
              : `${space}- ${firstKey}: ${yamlScalar(firstValue)}`;
          const restLines = rest
            .map(([key, restValue]) => {
              if (isObject(restValue) || Array.isArray(restValue)) {
                return `${" ".repeat(indent + 2)}${key}:\n${toYaml(restValue, indent + 4)}`;
              }
              return `${" ".repeat(indent + 2)}${key}: ${yamlScalar(restValue)}`;
            })
            .join("\n");
          return restLines ? `${firstLine}\n${restLines}` : firstLine;
        }
        if (isObject(item) || Array.isArray(item)) {
          const rendered = toYaml(item, indent + 2);
          return `${space}-\n${rendered}`;
        }
        return `${space}- ${yamlScalar(item)}`;
      })
      .join("\n");
  }
  if (isObject(value)) {
    return Object.entries(value)
      .map(([key, item]) => {
        if (isObject(item) || Array.isArray(item)) {
          return `${space}${key}:\n${toYaml(item, indent + 2)}`;
        }
        return `${space}${key}: ${yamlScalar(item)}`;
      })
      .join("\n");
  }
  return `${space}${yamlScalar(value)}`;
}

function dryRunProblems(summary) {
  const missing = Object.values(summary?.requiredMissing ?? {}).reduce(
    (sum, value) => sum + value,
    0,
  );
  const typeErrors = Object.values(summary?.typeErrors ?? {}).reduce(
    (sum, value) => sum + value,
    0,
  );
  const unknown = Object.values(summary?.unknownRequired ?? {}).reduce(
    (sum, value) => sum + value,
    0,
  );
  return missing + typeErrors + unknown;
}

function writeMetadataMap(path, metadataMap) {
  writeFileSync(
    path,
    [
      "# Generated by tiangong-kb-ingest skill.",
      "# Review and edit when collection-specific semantics require higher precision.",
      toYaml(metadataMap),
      "",
    ].join("\n"),
  );
}

function repairMetadataMap(metadataMap, summary, schemaSnapshot) {
  const fieldsByKey = new Map(
    metadataSchemaFields(schemaSnapshot)
      .filter((field) => typeof field.key === "string")
      .map((field) => [field.key, field]),
  );
  let changed = false;
  for (const key of Object.keys(summary?.requiredMissing ?? {})) {
    if (metadataMap.defaults?.[key] !== undefined) continue;
    const fallback = requiredDefault(fieldsByKey.get(key) ?? { key, required: true });
    metadataMap.defaults = { ...(metadataMap.defaults ?? {}), [key]: fallback };
    changed = true;
  }
  for (const key of Object.keys(summary?.unknownRequired ?? {})) {
    if (metadataMap.defaults?.[key] !== undefined) continue;
    metadataMap.defaults = { ...(metadataMap.defaults ?? {}), [key]: "unknown" };
    changed = true;
  }
  return changed;
}

function metadataMapPath(items) {
  return getFlagValue(items, "--metadata-map-output") ?? "metadata-map.yaml";
}

function ensureMetadataMapForBulk(items) {
  if (hasFlag(items, "--metadata-map") || hasFlag(items, "--no-metadata-map-autogen")) {
    return items;
  }
  const context = bulkRunContext(items);
  if (!context) return items;

  const outputPath = resolve(metadataMapPath(items));
  const cleanItems = stripSkillOnlyFlags(items);
  if (existsSync(outputPath) && !hasFlag(items, "--metadata-map-autogen")) {
    process.stderr.write(`Using existing metadata map: ${outputPath}\n`);
    return [...cleanItems, "--metadata-map", outputPath];
  }

  const rootPath = context.rootPath;
  const selectorArgs = flagArgs(items, SELECTOR_AND_CONFIG_FLAGS);
  const schemaFile = getFlagValue(items, "--schema-file");
  const schemaSnapshot = schemaFile
    ? JSON.parse(readFileSync(resolve(schemaFile), "utf8"))
    : execCliJson(["kb", "collections", "schema", ...selectorArgs, "--json"]);
  const scanSummary = execCliJson(["kb", "ingest", "bulk", "scan", rootPath, "--json"]);
  const metadataMap = buildMetadataMap(schemaSnapshot, scanSummary);

  writeMetadataMap(outputPath, metadataMap);

  let lastSummary;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    lastSummary = execCliJson([
      "kb",
      "ingest",
      "metadata",
      "dry-run",
      rootPath,
      ...selectorArgs,
      "--metadata-map",
      outputPath,
      "--json",
    ]);
    if (dryRunProblems(lastSummary) === 0) break;
    if (attempt < 3 && repairMetadataMap(metadataMap, lastSummary, schemaSnapshot)) {
      writeMetadataMap(outputPath, metadataMap);
      continue;
    }
    break;
  }

  process.stderr.write(
    [
      `Generated metadata map: ${outputPath}`,
      `Metadata dry-run validRate: ${lastSummary?.validRate ?? "unknown"}`,
      `Metadata dry-run fallbackRate: ${lastSummary?.fallbackRate ?? "unknown"}`,
      "",
    ].join("\n"),
  );
  return [...cleanItems, "--metadata-map", outputPath];
}

const args = process.argv.slice(2);
const subcommand = args[0] ?? "";
const nested = args[1] ?? "";
let normalizedArgs;

if (subcommand === "upload") {
  normalizedArgs = ["kb", "ingest", "bulk", ...ensureMetadataMapForBulk(args.slice(1))];
} else if (subcommand === "bulk") {
  normalizedArgs = ["kb", "ingest", "bulk", ...ensureMetadataMapForBulk(args.slice(1))];
} else if (subcommand === "status") {
  normalizedArgs = ["kb", "ingest", "status", ...args.slice(1)];
} else if (subcommand === "collections" && nested === "list") {
  normalizedArgs = ["kb", "collections", "list", ...args.slice(2)];
} else if (subcommand === "collections" && nested === "schema") {
  normalizedArgs = ["kb", "collections", "schema", ...args.slice(2)];
} else if (subcommand === "collections") {
  normalizedArgs = ["kb", "collections", "list", ...args.slice(1)];
} else {
  normalizedArgs = args;
}

exitWithCli(normalizedArgs);
