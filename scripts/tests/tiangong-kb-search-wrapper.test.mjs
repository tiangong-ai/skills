import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const CASES = [
  { skill: "tiangong-kb-esg-search", wrapper: "esg_search.sh", command: "research", source: "esg", credential: "TIANGONG_ESG_APIKEY" },
  { skill: "tiangong-kb-course-search", wrapper: "course_search.sh", command: "education", source: "course", credential: "TIANGONG_COURSE_APIKEY" },
  { skill: "tiangong-kb-edu-search", wrapper: "edu_search.sh", command: "education", source: "edu", credential: "TIANGONG_EDU_APIKEY" },
  { skill: "tiangong-kb-textbook-search", wrapper: "textbook_search.sh", command: "education", source: "textbook", credential: "TIANGONG_TEXTBOOK_APIKEY" },
];

for (const entry of CASES) {
  test(`${entry.skill} keeps credentials out of input and loads only owner env`, async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), `${entry.skill}.`));
    try {
      const wrapper = path.join(REPO_ROOT, entry.skill, "scripts", entry.wrapper);
      const fakeCli = path.join(root, "fake-cli");
      const marker = path.join(root, "invoked");
      const argsFile = path.join(root, "args.txt");
      const envFile = path.join(root, "owner.env");
      const secret = `owner-${entry.source}-secret`;
      await writeFile(
        fakeCli,
        [
          "#!/bin/bash",
          "set -euo pipefail",
          ': > "$FAKE_MARKER"',
          'printf "%s\\n" "$@" > "$FAKE_ARGS"',
          `printf '%s\\n' "\${${entry.credential}:-}" "\${EVIL_SETTING:-}" > "$FAKE_ENV"`,
          'printf \'{"ok":true}\\n\'',
        ].join("\n"),
        { mode: 0o700 },
      );
      await writeFile(envFile, `${entry.credential}=${secret}\nEVIL_SETTING=ignored\n`, { mode: 0o600 });
      const baseEnv = {
        ...process.env,
        FAKE_MARKER: marker,
        FAKE_ARGS: argsFile,
        FAKE_ENV: path.join(root, "env.txt"),
        TIANGONG_AI_CLI_BIN: fakeCli,
      };

      const success = spawnSync(
        wrapper,
        [JSON.stringify({ query: "deterministic query", dry_run: true, env_file: envFile })],
        { encoding: "utf8", env: baseEnv },
      );
      assert.equal(success.status, 0, success.stderr);
      const args = await readFile(argsFile, "utf8");
      assert.match(args, new RegExp(`^${entry.command}$`, "m"));
      assert.match(args, new RegExp(`^${entry.source}$`, "m"));
      assert.equal(await readFile(baseEnv.FAKE_ENV, "utf8"), `${secret}\n\n`);
      assert.doesNotMatch(args + success.stdout + success.stderr, new RegExp(secret));

      await rm(marker);
      const inline = spawnSync(
        wrapper,
        [JSON.stringify({ query: "blocked", api_key: secret })],
        { encoding: "utf8", env: baseEnv },
      );
      assert.notEqual(inline.status, 0);
      assert.doesNotMatch(inline.stdout + inline.stderr, new RegExp(secret));
      await assert.rejects(readFile(marker));

      await chmod(envFile, 0o644);
      const permissive = spawnSync(
        wrapper,
        [JSON.stringify({ query: "blocked", env_file: envFile })],
        { encoding: "utf8", env: baseEnv },
      );
      assert.notEqual(permissive.status, 0);

      const request = path.join(root, "request.json");
      const requestLink = path.join(root, "request-link.json");
      await writeFile(request, '{"query":"safe"}\n');
      await symlink(request, requestLink);
      const linked = spawnSync(
        wrapper,
        [JSON.stringify({ request_file: requestLink })],
        { encoding: "utf8", env: baseEnv },
      );
      assert.notEqual(linked.status, 0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
}

test("tiangong-kb-esg-search preserves the dedicated base URL environment override", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tiangong-kb-esg-search-endpoint."));
  try {
    const wrapper = path.join(REPO_ROOT, "tiangong-kb-esg-search", "scripts", "esg_search.sh");
    const fakeCli = path.join(root, "fake-cli");
    const argsFile = path.join(root, "args.txt");
    const envFile = path.join(root, "owner.env");
    await writeFile(fakeCli, "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' \"$@\" > \"$FAKE_ARGS\"\nprintf '{\"ok\":true}\\n'\n", { mode: 0o700 });
    await writeFile(envFile, "TIANGONG_ESG_API_BASE_URL=https://example.invalid/api\n", { mode: 0o600 });

    const result = spawnSync(
      wrapper,
      [JSON.stringify({ query: "endpoint compatibility", env_file: envFile })],
      {
        encoding: "utf8",
        env: { ...process.env, FAKE_ARGS: argsFile, TIANGONG_AI_CLI_BIN: fakeCli },
      },
    );
    assert.equal(result.status, 0, result.stderr);
    const args = await readFile(argsFile, "utf8");
    assert.match(args, /^--api-base-url$/m);
    assert.match(args, /^https:\/\/example\.invalid\/api$/m);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
