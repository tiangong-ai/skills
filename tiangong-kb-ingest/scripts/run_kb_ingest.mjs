#!/usr/bin/env node
import { accessSync, constants } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const skillDir = resolve(scriptDir, "..");
const workspaceCli = resolve(skillDir, "..", "..", "tiangong-ai-cli", "bin", "tiangong-ai.js");

function isExecutable(path) {
  try {
    accessSync(path, constants.R_OK);
    return true;
  } catch {
    return false;
  }
}

function run(command, args) {
  const result = spawnSync(command, args, {
    stdio: "inherit",
    env: process.env,
    shell: false,
  });

  if (result.error) {
    return false;
  }
  process.exit(result.status ?? 1);
}

const explicitCli = process.env.TIANGONG_AI_CLI_BIN?.trim();
const args = process.argv.slice(2);
const subcommand = args[0] ?? "";
const nested = args[1] ?? "";
let normalizedArgs;

function hasFlag(items, name) {
  return items.some((item) => item === name || item.startsWith(`${name}=`));
}

function withDefaultMaxPolls(items) {
  if (hasFlag(items, "--max-polls")) return items;
  return [...items, "--max-polls", process.env.TIANGONG_KB_BULK_MAX_POLLS?.trim() || "120"];
}

function withDefaultBulkRunMaxPolls(items) {
  if (["scan", "preflight", "dry-run", "resume", "export"].includes(items[0] ?? "")) {
    return items;
  }
  return withDefaultMaxPolls(items);
}

if (subcommand === "upload") {
  normalizedArgs = ["kb", "ingest", "bulk", ...withDefaultBulkRunMaxPolls(args.slice(1))];
} else if (subcommand === "bulk") {
  normalizedArgs = ["kb", "ingest", "bulk", ...withDefaultBulkRunMaxPolls(args.slice(1))];
} else if (subcommand === "status") {
  normalizedArgs = ["kb", "ingest", "status", ...args.slice(1)];
} else if (subcommand === "collections" && nested === "list") {
  normalizedArgs = ["kb", "collections", "list", ...args.slice(2)];
} else if (subcommand === "collections") {
  normalizedArgs = ["kb", "collections", "list", ...args.slice(1)];
} else {
  normalizedArgs = args;
}

if (explicitCli) {
  const commandArgs = explicitCli.endsWith(".js")
    ? [explicitCli, ...normalizedArgs]
    : normalizedArgs;
  run(explicitCli.endsWith(".js") ? process.execPath : explicitCli, commandArgs);
}

if (isExecutable(workspaceCli)) {
  run(process.execPath, [workspaceCli, ...normalizedArgs]);
}

run("tiangong-ai", normalizedArgs);

process.stderr.write(
  [
    "Unable to execute the Tiangong AI CLI.",
    "Install @tiangong-ai/cli, add tiangong-ai to PATH, or set TIANGONG_AI_CLI_BIN.",
    "",
  ].join("\n"),
);
process.exit(127);
