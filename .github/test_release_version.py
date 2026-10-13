import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("release-version.py")
SPEC = importlib.util.spec_from_file_location("release_version", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

TAGS = ["v1.0.9", "v1.0.10", "v1.0.197"]


class ReleaseVersionTest(unittest.TestCase):
    def test_accepts_greater_versions(self):
        for version in ("5.0.0", "1.0.198", "1.1.0", "2.0.0"):
            with self.subTest(version=version):
                self.assertEqual(MODULE.validate_version(version, TAGS), version)

    def test_compares_numerically(self):
        self.assertEqual(MODULE.validate_version("1.0.11", ["v1.0.9", "v1.0.10"]), "1.0.11")
        with self.assertRaises(ValueError):
            MODULE.validate_version("1.0.9", ["v1.0.2", "v1.0.10"])

    def test_rejects_bad_format(self):
        for version in (
            "", "v5.0.0", "5.0", "5", "5.0.0.1", "5.0.0-rc.1", "5.0.0+build",
            " 5.0.0", "5.0.0 ", "5.0.0\n", "05.0.0", "５.0.0", "5.0.x", "-5.0.0",
        ):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    MODULE.validate_version(version, TAGS)

    def test_rejects_not_greater(self):
        for version in ("1.0.197", "1.0.196", "0.9.9", "1.0.10"):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    MODULE.validate_version(version, TAGS)

    def test_rejects_existing_tag(self):
        with self.assertRaises(ValueError):
            MODULE.validate_version("1.0.10", ["v1.0.10", "v1.0.9", "v1.0.197"])

    def test_rejects_no_tags_or_unparsable_latest(self):
        for tags in ([], [""], ["v1.0.1", "vnext"]):
            with self.subTest(tags=tags):
                with self.assertRaises(ValueError):
                    MODULE.validate_version("5.0.0", tags)

    def test_cli(self):
        stdin = "\n".join(TAGS) + "\n"
        for version, code, output in (("5.0.0", 0, "5.0.0\n"), ("1.0.197", 1, ""), ("v5.0.0", 1, "")):
            with self.subTest(version=version):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), version], input=stdin, text=True, capture_output=True
                )
                self.assertEqual(result.returncode, code)
                self.assertEqual(result.stdout, output)
                self.assertEqual(result.stderr == "", code == 0)

    def test_cli_without_argument_fails(self):
        result = subprocess.run([sys.executable, str(SCRIPT)], input="", text=True, capture_output=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")

    def test_real_git_tags_sorted_by_sort_v(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args], text=True)

            git("init", "--quiet")
            git(
                "-c", "user.name=Test", "-c", "user.email=test@example.com",
                "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "init",
            )
            for tag in TAGS:
                git("tag", tag)
            listed = subprocess.run(
                "git tag --list 'v*' | sort -V", shell=True, cwd=directory, text=True, capture_output=True, check=True
            ).stdout
            self.assertEqual(listed.splitlines(), ["v1.0.9", "v1.0.10", "v1.0.197"])
            for version, code in (("5.0.0", 0), ("1.0.198", 0), ("1.0.197", 1), ("1.0.11", 1)):
                with self.subTest(version=version):
                    result = subprocess.run(
                        [sys.executable, str(SCRIPT), version], input=listed, text=True, capture_output=True
                    )
                    self.assertEqual(result.returncode, code)


if __name__ == "__main__":
    unittest.main()
