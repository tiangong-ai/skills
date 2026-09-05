import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { verifyDataSkillRequirement } from "../data-skill-binding.mjs";

const REPOSITORY_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const CLI_PACKAGE = process.env.TIANGONG_DATA_CLI_PACKAGE;
const CLI_VERSION = process.env.TIANGONG_DATA_CLI_VERSION;
const RUN_INSTALL_SMOKE =
  process.env.TIANGONG_DATA_SKILLS_RUN_INSTALL_SMOKE === "1";
const PILOTS = [
  {
    skill: "airnow-hourly-obs-fetch",
    capability: "airnow.hourly-observations",
    operations: ["fetch-hourly"],
  },
  {
    skill: "bluesky-cascade-fetch",
    capability: "bluesky.public-posts",
    operations: ["fetch-cascades"],
  },
  {
    skill: "epa-eis-records-fetch",
    capability: "epa.eis-records",
    operations: ["search"],
  },
  {
    skill: "federal-register-doc-fetch",
    capability: "federal-register.documents",
    operations: ["search"],
  },
  {
    skill: "gdelt-doc-search",
    capability: "gdelt.doc-search",
    operations: ["search"],
  },
  {
    skill: "gdelt-events-fetch",
    capability: "gdelt.events",
    operations: ["fetch"],
  },
  {
    skill: "gdelt-gkg-fetch",
    capability: "gdelt.gkg",
    operations: ["fetch"],
  },
  {
    skill: "gdelt-mentions-fetch",
    capability: "gdelt.mentions",
    operations: ["fetch"],
  },
  {
    skill: "nasa-firms-fire-fetch",
    capability: "nasa-firms.active-fire",
    operations: ["fetch-area"],
    requiredCredential: true,
  },
  {
    skill: "open-meteo-air-quality-fetch",
    capability: "open-meteo.air-quality",
    operations: ["fetch-hourly"],
  },
  {
    skill: "open-meteo-flood-fetch",
    capability: "open-meteo.flood",
    operations: ["fetch-daily"],
  },
  {
    skill: "open-meteo-historical-fetch",
    capability: "open-meteo.historical-weather",
    operations: ["fetch"],
  },
  {
    skill: "openaq-data-fetch",
    capability: "openaq.air-quality",
    operations: ["fetch-sensor-measurements", "search-locations"],
    requiredCredential: true,
  },
  {
    skill: "regulationsgov-attachments-fetch",
    capability: "regulations-gov.attachments",
    operations: ["download"],
    requiredCredential: true,
    suspended: true,
  },
  {
    skill: "regulationsgov-comment-detail-fetch",
    capability: "regulations-gov.comments",
    operations: ["fetch-details"],
    requiredCredential: true,
    suspended: true,
  },
  {
    skill: "regulationsgov-comments-fetch",
    capability: "regulations-gov.comments",
    operations: ["search"],
    requiredCredential: true,
    suspended: true,
  },
  {
    skill: "usbr-project-records-fetch",
    capability: "usbr.project-records",
    operations: ["fetch"],
  },
  {
    skill: "usbr-rise-fetch",
    capability: "usbr.rise",
    operations: ["discover-items", "fetch-results"],
  },
  {
    skill: "usgs-water-iv-fetch",
    capability: "usgs.water-instantaneous-values",
    operations: ["fetch"],
  },
  {
    skill: "youtube-comments-fetch",
    capability: "youtube.public-content",
    operations: ["fetch-comments"],
    requiredCredential: true,
  },
  {
    skill: "youtube-video-search",
    capability: "youtube.public-content",
    operations: ["search-videos"],
    requiredCredential: true,
  },
];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
    env: options.env,
    input: options.input,
    maxBuffer: 32 * 1024 * 1024,
    timeout: 240_000,
  });
  if (result.error) {
    throw result.error;
  }
  return result;
}

function readInstalledRequirement(consumer, pilot) {
  return JSON.parse(
    readFileSync(
      resolve(
        consumer,
        ".agents/skills",
        pilot.skill,
        "references/tiangong-data-requirement.json",
      ),
      "utf8",
    ),
  );
}

test(
  "copy and symlink installs verify requirements against the exact smoke CLI",
  { skip: !RUN_INSTALL_SMOKE },
  () => {
    assert.match(CLI_VERSION ?? "", /^\d+\.\d+\.\d+$/);
    assert.ok(CLI_PACKAGE, "TIANGONG_DATA_CLI_PACKAGE is required");

    const temporaryRoot = mkdtempSync(
      resolve(tmpdir(), "tiangong-data-skills-install-"),
    );
    try {
      const npmCache = resolve(temporaryRoot, "npm-cache");
      for (const installMode of ["copy", "symlink"]) {
        const consumer = resolve(temporaryRoot, installMode);
        const home = resolve(consumer, "home");
        const environment = {
          ...process.env,
          CI: "1",
          HOME: home,
          NO_COLOR: "1",
          npm_config_cache: npmCache,
        };
        for (const name of Object.keys(environment)) {
          if (
            /AIRNOW|BLUESKY|FEDERAL_REGISTER|GDELT|NASA|FIRMS|OPENAQ|OPEN_METEO|REGGOV|USGS|YOUTUBE|TIANGONG.*KEY/i.test(
              name,
            )
          ) {
            delete environment[name];
          }
        }
        assert.equal(run("mkdir", ["-p", home]).status, 0);
        assert.equal(run("git", ["init", "-q"], { cwd: consumer }).status, 0);

        for (const pilot of PILOTS) {
          const installArguments = [
            "--yes",
            "skills@1.5.22",
            "add",
            REPOSITORY_ROOT,
            "--skill",
            pilot.skill,
            "--agent",
            "codex",
            "--yes",
          ];
          if (installMode === "copy") {
            installArguments.push("--copy");
          }
          const install = run("npx", installArguments, {
            cwd: consumer,
            env: environment,
          });
          assert.equal(install.status, 0, install.stderr || install.stdout);

          const installed = resolve(consumer, ".agents/skills", pilot.skill);
          for (const relative of [
            "SKILL.md",
            "agents/openai.yaml",
            "references/tiangong-data-requirement.json",
          ]) {
            assert.equal(
              existsSync(resolve(installed, relative)),
              true,
              relative,
            );
          }
          assert.equal(existsSync(resolve(installed, "scripts")), false);
          assert.equal(existsSync(resolve(installed, "assets")), false);
          const requirement = readInstalledRequirement(consumer, pilot);
          assert.equal(requirement.capabilityId, pilot.capability);
          assert.deepEqual(Object.keys(requirement.operations), pilot.operations);
          assert.equal("generatedWithCliVersion" in requirement, false);
        }

        const cli = ["--yes", "--package", CLI_PACKAGE, "--", "tiangong-ai"];
        const version = run("npx", [...cli, "--version"], {
          cwd: consumer,
          env: environment,
        });
        assert.equal(version.status, 0, version.stderr);
        assert.equal(version.stdout.trim(), CLI_VERSION);

        const catalog = run("npx", [...cli, "data", "catalog", "--json"], {
          cwd: consumer,
          env: environment,
        });
        assert.equal(catalog.status, 0, catalog.stderr);
        assert.equal(JSON.parse(catalog.stdout).capabilities.length >= 19, true);

        for (const pilot of PILOTS) {
          const describe = run(
            "npx",
            [...cli, "data", "describe", pilot.capability, "--json"],
            { cwd: consumer, env: environment },
          );
          assert.equal(describe.status, 0, describe.stderr);
          const describeEnvelope = JSON.parse(describe.stdout);
          const manifest = describeEnvelope.manifest;
          assert.equal(manifest.capabilityId, pilot.capability);
          verifyDataSkillRequirement({
            requirement: readInstalledRequirement(consumer, pilot),
            describe: describeEnvelope,
          });

          const doctor = run(
            "npx",
            [...cli, "data", "doctor", pilot.capability, "--json"],
            { cwd: consumer, env: environment },
          );
          assert.equal(
            doctor.status,
            pilot.requiredCredential ? 3 : 0,
            doctor.stderr,
          );
          assert.equal(
            JSON.parse(doctor.stdout).status,
            pilot.requiredCredential ? "blocked" : "ready",
          );
          if (pilot.suspended) {
            assert.equal(manifest.availability?.status, "suspended");
          }

          for (const operationId of pilot.operations) {
            const operation = manifest.operations.find(
              (candidate) => candidate.operationId === operationId,
            );
            assert.ok(operation, operationId);
            const blocked = run(
              "npx",
              [
                ...cli,
                "data",
                "run",
                pilot.capability,
                operationId,
                "--input",
                "-",
                "--json",
              ],
              {
                cwd: consumer,
                env: environment,
                input: `${JSON.stringify({
                  schemaVersion: "tiangong.data.run-request.v1",
                  capabilityId: pilot.capability,
                  capabilityVersion: manifest.capabilityVersion,
                  operationId,
                  operationVersion: operation.operationVersion,
                  input: { __offlineSmoke: true },
                })}\n`,
              },
            );
            assert.notEqual(blocked.status, 0);
            const blockedResult = JSON.parse(blocked.stdout);
            assert.equal(blockedResult.status, "blocked");
            assert.equal(
              blockedResult.errors[0]?.code,
              pilot.suspended ? "capability-unavailable" : "invalid-request",
            );
          }
        }
      }
    } finally {
      rmSync(temporaryRoot, { recursive: true, force: true });
    }
  },
);
