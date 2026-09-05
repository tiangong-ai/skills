import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  buildDataSkillRequirement,
  buildMigrationProvenance,
  buildMigrationProvenanceEntry,
  validateDataSkillRequirement,
  validateMigrationProvenance,
  verifyDataSkillRequirement,
  verifyMigrationProvenanceEntry,
} from "../data-skill-binding.mjs";

const CLI_VERSION = "0.0.55";
const REPOSITORY_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const PILOT_SKILLS = [
  {
    name: "airnow-hourly-obs-fetch",
    capabilityId: "airnow.hourly-observations",
    operations: [
      {
        operationId: "fetch-hourly",
        inputKeys: [
          "boundingBox",
          "endDateTimeUtc",
          "parameters",
          "startDateTimeUtc",
        ],
      },
    ],
  },
  {
    name: "bluesky-cascade-fetch",
    capabilityId: "bluesky.public-posts",
    operations: [
      {
        operationId: "fetch-cascades",
        inputKeys: [
          "endDateTime",
          "expandThreads",
          "maxThreads",
          "pageSize",
          "source",
          "startDateTime",
          "threadDepth",
          "threadParentHeight",
        ],
      },
    ],
  },
  {
    name: "epa-eis-records-fetch",
    capabilityId: "epa.eis-records",
    operations: [
      {
        operationId: "search",
        inputKeys: ["commonSearches", "searchUrls"],
      },
    ],
  },
  {
    name: "federal-register-doc-fetch",
    capabilityId: "federal-register.documents",
    operations: [
      {
        operationId: "search",
        inputKeys: [
          "agencies",
          "documentTypes",
          "order",
          "pageSize",
          "publicationDate",
          "term",
        ],
        usesExecutionLimits: true,
      },
    ],
  },
  {
    name: "gdelt-doc-search",
    capabilityId: "gdelt.doc-search",
    operations: [
      {
        operationId: "search",
        inputKeys: ["absoluteWindow", "maxRecords", "mode", "query", "sort"],
      },
    ],
  },
  {
    name: "gdelt-events-fetch",
    capabilityId: "gdelt.events",
    operations: [
      {
        operationId: "fetch",
        inputKeys: ["endDateTime", "maxFiles", "mode", "startDateTime"],
      },
    ],
  },
  {
    name: "gdelt-gkg-fetch",
    capabilityId: "gdelt.gkg",
    operations: [
      {
        operationId: "fetch",
        inputKeys: ["endDateTime", "maxFiles", "mode", "startDateTime"],
      },
    ],
  },
  {
    name: "gdelt-mentions-fetch",
    capabilityId: "gdelt.mentions",
    operations: [
      {
        operationId: "fetch",
        inputKeys: ["endDateTime", "maxFiles", "mode", "startDateTime"],
      },
    ],
  },
  {
    name: "nasa-firms-fire-fetch",
    capabilityId: "nasa-firms.active-fire",
    operations: [
      {
        operationId: "fetch-area",
        inputKeys: [
          "boundingBox",
          "checkAvailability",
          "endDate",
          "source",
          "startDate",
        ],
      },
    ],
  },
  {
    name: "open-meteo-air-quality-fetch",
    capabilityId: "open-meteo.air-quality",
    operations: [
      {
        operationId: "fetch-hourly",
        requiredFeatures: ["open-meteo.series-all-null"],
        inputKeys: [
          "cellSelection",
          "domain",
          "endDate",
          "hourlyVariables",
          "locations",
          "startDate",
        ],
      },
    ],
  },
  {
    name: "open-meteo-flood-fetch",
    capabilityId: "open-meteo.flood",
    operations: [
      {
        operationId: "fetch-daily",
        requiredFeatures: ["open-meteo.series-all-null"],
        inputKeys: [
          "cellSelection",
          "dailyVariables",
          "endDate",
          "includeEnsembleMembers",
          "locations",
          "startDate",
        ],
      },
    ],
  },
  {
    name: "open-meteo-historical-fetch",
    capabilityId: "open-meteo.historical-weather",
    operations: [
      {
        operationId: "fetch",
        requiredFeatures: ["open-meteo.series-all-null"],
        inputKeys: [
          "cellSelection",
          "dailyVariables",
          "endDate",
          "hourlyVariables",
          "locations",
          "model",
          "startDate",
        ],
      },
    ],
  },
  {
    name: "openaq-data-fetch",
    capabilityId: "openaq.air-quality",
    operations: [
      {
        operationId: "fetch-sensor-measurements",
        inputKeys: [
          "endDateTime",
          "granularity",
          "pageSize",
          "sensorId",
          "startDateTime",
        ],
      },
      {
        operationId: "search-locations",
        inputKeys: ["countryCode", "pageSize", "parameterIds", "sortOrder"],
      },
    ],
  },
  {
    name: "regulationsgov-attachments-fetch",
    capabilityId: "regulations-gov.attachments",
    operations: [
      {
        operationId: "download",
        inputKeys: [
          "attachmentIds",
          "commentIds",
          "maxFiles",
          "maxTotalBytes",
        ],
      },
    ],
  },
  {
    name: "regulationsgov-comment-detail-fetch",
    capabilityId: "regulations-gov.comments",
    operations: [
      {
        operationId: "fetch-details",
        inputKeys: ["commentIds", "includeAttachments"],
      },
    ],
  },
  {
    name: "regulationsgov-comments-fetch",
    capabilityId: "regulations-gov.comments",
    operations: [
      {
        operationId: "search",
        inputKeys: [
          "agencyId",
          "pageSize",
          "postedDate",
          "searchTerm",
          "sortOrder",
        ],
      },
    ],
  },
  {
    name: "usbr-project-records-fetch",
    capabilityId: "usbr.project-records",
    operations: [
      {
        operationId: "fetch",
        inputKeys: ["maxLinkedRecordsPerPage", "urls"],
      },
    ],
  },
  {
    name: "usbr-rise-fetch",
    capabilityId: "usbr.rise",
    operations: [
      {
        operationId: "discover-items",
        inputKeys: ["locationNameContains", "pageSize", "queryTerms"],
      },
      {
        operationId: "fetch-results",
        inputKeys: [
          "afterUtc",
          "beforeUtc",
          "includeItemMetadata",
          "itemIds",
          "orderDateTime",
          "pageSize",
        ],
      },
    ],
  },
  {
    name: "usgs-water-iv-fetch",
    capabilityId: "usgs.water-instantaneous-values",
    operations: [
      {
        operationId: "fetch",
        inputKeys: [
          "boundingBox",
          "parameterCodes",
          "period",
          "siteStatus",
          "siteType",
        ],
      },
    ],
  },
  {
    name: "youtube-comments-fetch",
    capabilityId: "youtube.public-content",
    operations: [
      {
        operationId: "fetch-comments",
        requiredFeatures: ["youtube.reply-strategy"],
        inputKeys: [
          "endDateTime",
          "maxReplyPagesPerThread",
          "maxThreadPagesPerVideo",
          "order",
          "pageSize",
          "replyStrategy",
          "startDateTime",
          "timeField",
          "videoIds",
        ],
      },
    ],
  },
  {
    name: "youtube-video-search",
    capabilityId: "youtube.public-content",
    operations: [
      {
        operationId: "search-videos",
        inputKeys: [
          "maxSearchPages",
          "minimumCommentCount",
          "minimumViewCount",
          "order",
          "pageSize",
          "publishedAfter",
          "publishedBefore",
          "query",
          "regionCode",
          "relevanceLanguage",
          "requirePublicComments",
          "safeSearch",
          "videoDuration",
        ],
      },
    ],
  },
];

function listFiles(root, current = root) {
  return readdirSync(current, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(current, entry.name);
    return entry.isDirectory() ? listFiles(root, path) : [relative(root, path)];
  });
}

function describeFixture() {
  return {
    schemaVersion: "tiangong.data.describe.v1",
    manifest: {
      schemaVersion: "tiangong.data.manifest.v1",
      capabilityId: "example.records",
      capabilityVersion: "1.2.3",
      minimumCliVersion: "0.0.54",
      providerId: "example",
      endpoints: [],
      credentials: [],
      limits: {},
      diagnostics: { static: true, live: false },
      operations: [
        {
          operationId: "search",
          operationVersion: "2.0.0",
          inputSchema: {
            schemaId: "https://schemas.tiangong.ai/data/example/input.v1.json",
            digest: "a".repeat(64),
          },
          outputSchema: {
            schemaId: "https://schemas.tiangong.ai/data/example/output.v1.json",
            digest: "b".repeat(64),
          },
          limits: {},
          features: ["example.stable-search"],
        },
      ],
      manifestDigest: "c".repeat(64),
    },
    discovery: {
      schemaVersion: "tiangong.data.discovery.v1",
      capabilityId: "example.records",
      capabilityVersion: "1.2.3",
      summary: "Example discovery text.",
      discoveryDigest: "d".repeat(64),
    },
    schemas: {},
  };
}

test("builds a stable capability requirement without a package build", () => {
  const requirement = buildDataSkillRequirement({
    skillName: "example-record-search",
    describe: describeFixture(),
    operationIds: ["search"],
  });

  assert.deepEqual(requirement, {
    schemaVersion: "tiangong.data.skill-capability-requirement.v1",
    skillName: "example-record-search",
    capabilityId: "example.records",
    capabilityContractVersion: "1",
    operations: {
      search: { contractVersion: "2" },
    },
  });
  assert.equal("generatedWithCliVersion" in requirement, false);
  assert.equal("manifestDigest" in requirement, false);
});

test("accepts package, discovery, and compatible execution changes", () => {
  const describe = describeFixture();
  const requirement = buildDataSkillRequirement({
    skillName: "example-record-search",
    describe,
    operationIds: ["search"],
  });
  describe.discovery.summary = "Updated discovery wording.";
  describe.discovery.discoveryDigest = "e".repeat(64);
  describe.manifest.capabilityVersion = "1.9.0";
  describe.manifest.operations[0].operationVersion = "2.4.1";
  describe.manifest.manifestDigest = "f".repeat(64);
  describe.manifest.operations[0].inputSchema.digest = "0".repeat(64);

  assert.doesNotThrow(() =>
    verifyDataSkillRequirement({ requirement, describe }),
  );
});

test("binds optional operation features and rejects a same-major CLI that lacks them", () => {
  const requirement = buildDataSkillRequirement({
    skillName: "example-record-search",
    describe: describeFixture(),
    operationIds: ["search"],
    requiredFeatures: {
      search: ["example.stable-search"],
    },
  });

  assert.deepEqual(requirement, {
    schemaVersion: "tiangong.data.skill-capability-requirement.v2",
    skillName: "example-record-search",
    capabilityId: "example.records",
    capabilityContractVersion: "1",
    operations: {
      search: {
        contractVersion: "2",
        requiredFeatures: ["example.stable-search"],
      },
    },
  });
  assert.doesNotThrow(() =>
    verifyDataSkillRequirement({ requirement, describe: describeFixture() }),
  );

  const sameMajorWithoutFeature = describeFixture();
  sameMajorWithoutFeature.manifest.operations[0].features = [];
  assert.throws(
    () =>
      verifyDataSkillRequirement({
        requirement,
        describe: sameMajorWithoutFeature,
      }),
    /search feature drift.*example\.stable-search/,
  );
});

test("accepts a newer CLI package when the capability contract is unchanged", () => {
  const requirement = buildDataSkillRequirement({
    skillName: "example-record-search",
    describe: describeFixture(),
    operationIds: ["search"],
  });

  assert.equal("generatedWithCliVersion" in requirement, false);
  assert.doesNotThrow(() =>
    verifyDataSkillRequirement({
      requirement,
      describe: describeFixture(),
    }),
  );
});

test("rejects missing capabilities, operations, and contract-major drift", () => {
  const requirement = buildDataSkillRequirement({
    skillName: "example-record-search",
    describe: describeFixture(),
    operationIds: ["search"],
  });
  const wrongCapability = describeFixture();
  wrongCapability.manifest.capabilityId = "other.records";
  assert.throws(
    () => verifyDataSkillRequirement({ requirement, describe: wrongCapability }),
    /capabilityId/,
  );

  const missingOperation = describeFixture();
  missingOperation.manifest.operations = [];
  assert.throws(
    () => verifyDataSkillRequirement({ requirement, describe: missingOperation }),
    /operationId drift/,
  );

  const capabilityMajor = describeFixture();
  capabilityMajor.manifest.capabilityVersion = "2.0.0";
  assert.throws(
    () => verifyDataSkillRequirement({ requirement, describe: capabilityMajor }),
    /capability contract drift/,
  );

  const operationMajor = describeFixture();
  operationMajor.manifest.operations[0].operationVersion = "3.0.0";
  assert.throws(
    () => verifyDataSkillRequirement({ requirement, describe: operationMajor }),
    /search contract drift/,
  );
});

test("keeps exact digests in one migration provenance artifact", () => {
  const describe = describeFixture();
  const entry = buildMigrationProvenanceEntry({
    skillName: "example-record-search",
    describe,
    operationIds: ["search"],
  });
  const provenance = buildMigrationProvenance({
    cliVersion: CLI_VERSION,
    skills: [entry],
  });
  validateMigrationProvenance(provenance);
  assert.equal(provenance.generatedWithCliVersion, CLI_VERSION);
  assert.equal(provenance.skills[0].manifestDigest, "c".repeat(64));
  assert.doesNotThrow(() =>
    verifyMigrationProvenanceEntry({ entry, describe }),
  );

  describe.manifest.operations[0].outputSchema.digest = "0".repeat(64);
  assert.throws(
    () => verifyMigrationProvenanceEntry({ entry, describe }),
    /outputSchemaDigest/,
  );
});

test("rejects undeclared requirement fields", () => {
  const requirement = {
    ...buildDataSkillRequirement({
      skillName: "example-record-search",
      describe: describeFixture(),
      operationIds: ["search"],
    }),
    generatedWithCliVersion: CLI_VERSION,
  };
  assert.throws(
    () => validateDataSkillRequirement(requirement),
    /Unexpected requirement field/,
  );
});

test("pilot data skills are thin, package-independent semantic entrypoints", () => {
  for (const pilot of PILOT_SKILLS) {
    const root = resolve(REPOSITORY_ROOT, pilot.name);
    const requirementPath = resolve(
      root,
      "references/tiangong-data-requirement.json",
    );
    assert.equal(existsSync(resolve(root, "scripts")), false, pilot.name);
    assert.equal(existsSync(resolve(root, "assets")), false, pilot.name);
    assert.deepEqual(
      listFiles(root).sort(),
      [
        "SKILL.md",
        "agents/openai.yaml",
        "references/tiangong-data-requirement.json",
      ],
      pilot.name,
    );
    assert.deepEqual(
      readdirSync(resolve(root, "references")).sort(),
      ["tiangong-data-requirement.json"],
      pilot.name,
    );

    const requirement = JSON.parse(readFileSync(requirementPath, "utf8"));
    validateDataSkillRequirement(requirement);
    assert.equal(requirement.skillName, pilot.name);
    assert.equal(requirement.capabilityId, pilot.capabilityId);
    assert.deepEqual(
      Object.keys(requirement.operations),
      pilot.operations.map((operation) => operation.operationId).sort(),
    );

    const skill = readFileSync(resolve(root, "SKILL.md"), "utf8");
    assert.match(skill, /references\/tiangong-data-requirement\.json/);
    assert.match(skill, new RegExp(`data describe ${pilot.capabilityId}`));
    for (const operation of pilot.operations) {
      assert.match(
        skill,
        new RegExp(`data run ${pilot.capabilityId} ${operation.operationId}`),
      );
    }
    assert.match(skill, /tiangong\.data\.run-request\.v1/);
    assert.match(skill, /"input": \{/);
    const exampleTexts = [...skill.matchAll(/```json\n([\s\S]*?)\n```/g)].map(
      (match) => match[1],
    );
    assert.equal(
      exampleTexts.length,
      pilot.operations.length,
      `${pilot.name} must include one JSON request example per operation`,
    );
    const examples = exampleTexts.map((exampleText) => {
      let normalized = exampleText.replaceAll(
        "<describe.manifest.capabilityVersion>",
        `${requirement.capabilityContractVersion}.0.0`,
      );
      Object.entries(requirement.operations).forEach(
        ([, operation], index) => {
          normalized = normalized.replaceAll(
            `<describe.manifest.operations[${index}].operationVersion>`,
            `${operation.contractVersion}.0.0`,
          );
        },
      );
      return JSON.parse(normalized);
    });
    for (const expectedOperation of pilot.operations) {
      const requirementOperation =
        requirement.operations[expectedOperation.operationId];
      const example = examples.find(
        (candidate) => candidate.operationId === expectedOperation.operationId,
      );
      assert.ok(requirementOperation, expectedOperation.operationId);
      assert.ok(example, expectedOperation.operationId);
      assert.equal(example.schemaVersion, "tiangong.data.run-request.v1");
      assert.equal(example.capabilityId, requirement.capabilityId);
      assert.equal(
        example.capabilityVersion,
        `${requirement.capabilityContractVersion}.0.0`,
      );
      assert.equal(
        example.operationVersion,
        `${requirementOperation.contractVersion}.0.0`,
      );
      assert.deepEqual(
        requirementOperation.requiredFeatures ?? [],
        expectedOperation.requiredFeatures ?? [],
      );
      for (const feature of expectedOperation.requiredFeatures ?? []) {
        assert.match(skill, new RegExp(feature.replaceAll(".", "\\.")));
      }
      assert.deepEqual(
        Object.keys(example.input).sort(),
        expectedOperation.inputKeys,
      );
      if (expectedOperation.usesExecutionLimits) {
        assert.deepEqual(example.limits, { maxPages: 2, maxRecords: 100 });
      }
    }
    assert.doesNotMatch(
      skill,
      /python3|OpenClaw|eco-council|check-config|--dry-run|--output|config\.example\.env/,
    );
    assert.doesNotMatch(skill, /@tiangong-ai\/cli@\d+\.\d+\.\d+/);
    assert.doesNotMatch(skill, /generatedWithCliVersion|manifestDigest/);

    const agent = readFileSync(resolve(root, "agents/openai.yaml"), "utf8");
    assert.match(agent, new RegExp(`\\$${pilot.name}`));
    assert.doesNotMatch(agent, /raw artifact|OpenClaw|eco-council/);
  }

  const provenance = JSON.parse(
    readFileSync(
      resolve(REPOSITORY_ROOT, "scripts/data-skill-migration-provenance.json"),
      "utf8",
    ),
  );
  validateMigrationProvenance(provenance);
  assert.equal(provenance.skills.length, PILOT_SKILLS.length);
  assert.deepEqual(
    provenance.skills.map((entry) => entry.skillName),
    PILOT_SKILLS.map((pilot) => pilot.name).sort(),
  );
});
