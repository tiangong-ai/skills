import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const CLI_VERSION = "0.0.62";
const SKILLS = [
  ["tiangong-kb-sci-search", "sci_search.sh"],
  ["tiangong-kb-patent-search", "patent_search.sh"],
  ["tiangong-kb-report-search", "report_search.sh"],
  ["tiangong-kb-esg-search", "esg_search.sh"],
  ["tiangong-kb-course-search", "course_search.sh"],
  ["tiangong-kb-edu-search", "edu_search.sh"],
  ["tiangong-kb-textbook-search", "textbook_search.sh"],
];

test("Tiangong KB search skills share one exact standalone CLI pin", async () => {
  for (const [skill, wrapperName] of SKILLS) {
    const skillText = await readFile(path.join(REPO_ROOT, skill, "SKILL.md"), "utf8");
    const wrapperText = await readFile(
      path.join(REPO_ROOT, skill, "scripts", wrapperName),
      "utf8",
    );

    assert.match(skillText, new RegExp(`@tiangong-ai/cli@${CLI_VERSION.replaceAll(".", "\\.")}`), skill);
    assert.doesNotMatch(skillText, /@tiangong-ai\/cli@0\.0\.(?:19|30)/, skill);
    assert.match(
      wrapperText,
      new RegExp(`STANDALONE_TESTED_CLI_VERSION="${CLI_VERSION.replaceAll(".", "\\.")}"`),
      skill,
    );
    assert.match(wrapperText, /npx --yes --package "@tiangong-ai\/cli@\$STANDALONE_TESTED_CLI_VERSION" -- tiangong-ai/, skill);
    assert.doesNotMatch(wrapperText, /@tiangong-ai\/cli@0\.0\.(?:19|30)/, skill);
  }
});
