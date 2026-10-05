import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("semver-level.py")
SPEC = importlib.util.spec_from_file_location("semver_level", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def commit(subject, body=""):
    return f"{subject}\x1f{body}\x1e\n"


class SemverLevelTest(unittest.TestCase):
    def test_breaking_subjects(self):
        for subject in ("feat!: change API", "fix(scope)!: change API", "perf!: change API"):
            with self.subTest(subject=subject):
                self.assertEqual(MODULE.semver_level(commit(subject)), "major")

    def test_breaking_body(self):
        for marker in ("BREAKING CHANGE:", "BREAKING-CHANGE:"):
            with self.subTest(marker=marker):
                body = f"Details\twith tabs\n\n{marker} change API\nMore details\n"
                self.assertEqual(MODULE.semver_level(commit("fix: update", body)), "major")

    def test_marker_must_start_a_line(self):
        for body in ("Mention BREAKING CHANGE: in text", " BREAKING-CHANGE: indented"):
            with self.subTest(body=body):
                self.assertEqual(MODULE.semver_level(commit("fix: update", body)), "patch")

    def test_minor_types(self):
        for subject in ("feat: add API", "feat(scope): add API", "revert: undo change"):
            with self.subTest(subject=subject):
                self.assertEqual(MODULE.semver_level(commit(subject)), "minor")

    def test_patch_types(self):
        for kind in ("perf", "fix", "build", "ci", "docs", "style", "refactor", "chore", "test", "custom"):
            with self.subTest(kind=kind):
                self.assertEqual(MODULE.semver_level(commit(f"{kind}: update")), "patch")

    def test_highest_level_wins_in_either_order(self):
        patch = commit("perf: optimize", "Details\twith tabs\nand newlines\n")
        minor = commit("feat: add API")
        major = commit("fix: update", "BREAKING CHANGE: change API\n")
        for records, expected in (
            ([patch, minor], "minor"),
            ([patch, minor, major], "major"),
        ):
            with self.subTest(expected=expected):
                self.assertEqual(MODULE.semver_level("".join(records)), expected)
                self.assertEqual(MODULE.semver_level("".join(reversed(records))), expected)

    def test_empty_or_unmatched_log(self):
        for log in ("", "\n", commit("Merge branch main")):
            with self.subTest(log=log):
                self.assertIsNone(MODULE.semver_level(log))

    def test_cli(self):
        for log, code, output in (("", 1, ""), (commit("perf: optimize"), 0, "patch\n")):
            with self.subTest(log=log):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT)], input=log, text=True, capture_output=True
                )
                self.assertEqual(result.returncode, code)
                self.assertEqual(result.stdout, output)
                self.assertEqual(result.stderr, "")

    def test_real_git_log_with_multiline_body_and_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args], text=True)

            git("init", "--quiet")
            for subject, body in (
                ("feat!: change API", ""),
                ("fix(scope)!: change scoped API", ""),
                ("perf: optimize", "Details\twith tabs\nand newlines"),
                ("fix: update", "Details\n\nBREAKING-CHANGE: change API\nMore details"),
            ):
                git(
                    "-c", "user.name=Test", "-c", "user.email=test@example.com",
                    "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty",
                    "-m", subject, "-m", body,
                )
            log = git(
                "log", "--pretty=format:%s%x1f%b%x1e", "--no-merges", "-P",
                r"--grep=^(BREAKING CHANGE|BREAKING-CHANGE|build|ci|feat|fix|docs|style|refactor|perf|test|revert|chore)(\(.*\))?!?:",
            )
            self.assertEqual(log.count("\x1e"), 4)
            self.assertEqual(MODULE.semver_level(log), "major")
            self.assertEqual(MODULE.semver_level(git("log", "-1", "--pretty=format:%s%x1f%b%x1e")), "major")


if __name__ == "__main__":
    unittest.main()
