from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[2]
SKILLS_REPOSITORY = SKILL_ROOT.parent
RUN_INSTALL_SMOKE = os.environ.get("ACADEMIC_PAPER_DOWNLOAD_RUN_INSTALL_SMOKE") == "1"


@unittest.skipUnless(
    RUN_INSTALL_SMOKE, "set ACADEMIC_PAPER_DOWNLOAD_RUN_INSTALL_SMOKE=1"
)
class InstalledSkillSmokeTests(unittest.TestCase):
    def test_npx_copy_and_symlink_installs_bootstrap_and_smoke_offline(self) -> None:
        if not shutil.which("npx"):
            self.skipTest("npx is not installed")
        with tempfile.TemporaryDirectory() as cache:
            for install_mode in ("copy", "symlink"):
                with (
                    self.subTest(install_mode=install_mode),
                    tempfile.TemporaryDirectory() as consumer,
                ):
                    subprocess.run(["git", "init", "-q"], cwd=consumer, check=True)
                    environment = {
                        **os.environ,
                        "ACADEMIC_PAPER_DOWNLOAD_CACHE_DIR": cache,
                        "CI": "1",
                        "NO_COLOR": "1",
                    }
                    command = [
                        "npx",
                        "--yes",
                        "skills@1.5.22",
                        "add",
                        str(SKILLS_REPOSITORY),
                        "--skill",
                        "academic-paper-download",
                        "--agent",
                        "codex",
                        "--yes",
                    ]
                    if install_mode == "copy":
                        command.append("--copy")
                    subprocess.run(
                        command,
                        cwd=consumer,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    installed = (
                        Path(consumer)
                        / ".agents"
                        / "skills"
                        / "academic-paper-download"
                    )
                    for relative in (
                        "SKILL.md",
                        "pyproject.toml",
                        "requirements.lock",
                        "scripts/runtime.py",
                    ):
                        self.assertTrue((installed / relative).is_file(), relative)

                    bootstrap = subprocess.run(
                        [
                            sys.executable,
                            str(installed / "scripts" / "runtime.py"),
                            "bootstrap",
                            "--locked",
                            "--json",
                        ],
                        cwd=consumer,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(
                        json.loads(bootstrap.stdout)["data"]["pypdf"], "6.14.2"
                    )

                    smoke = subprocess.run(
                        [
                            sys.executable,
                            str(installed / "scripts" / "runtime.py"),
                            "smoke",
                            "--offline",
                            "--json",
                        ],
                        cwd=consumer,
                        env=environment,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    payload = json.loads(smoke.stdout)
                    self.assertTrue(payload["ok"])
                    self.assertFalse(payload["data"]["network_used"])
                    self.assertEqual(payload["data"]["pdf_pages"], 1)
                    self.assertEqual(payload["data"]["identity_status"], "matched")


if __name__ == "__main__":
    unittest.main()
