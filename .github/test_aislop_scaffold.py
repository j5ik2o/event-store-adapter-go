import hashlib
import json
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPOSITORY_ROOT / "scripts"

BOOTSTRAP_SHA256 = "04f58b167f7b94066f7afe294cd0289f160efa5a768818ef1aa343ab26ab54e5"
CHECKER_SHA256 = "c53e1b91dd45ead4bfdfd3dc4bbec794e2fd81b96857a90f7c5aaea414f8e30c"
CHECKER_TEST_SHA256 = "95dda876c46cea1ca3be454d82be31080ecf9ff1f387d0a06ea014e5bc1c9130"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


class AislopScaffoldTest(unittest.TestCase):
    def test_package_manifest_pins_aislop_and_declares_gate_scripts(self):
        package = read_json(REPOSITORY_ROOT / "package.json")
        self.assertEqual(package["devDependencies"]["aislop"], "0.20.1")
        self.assertEqual(
            package["scripts"]["check:aislop"],
            "node scripts/aislop-check-bootstrap.mjs check",
        )
        self.assertEqual(
            package["scripts"]["test:aislop"],
            "node scripts/aislop-check-bootstrap.mjs test",
        )

    def test_lockfile_pins_the_same_aislop_version(self):
        lock = read_json(REPOSITORY_ROOT / "package-lock.json")
        self.assertEqual(lock["packages"]["node_modules/aislop"]["version"], "0.20.1")

    def test_pin_records_the_fixed_reference_and_hashes(self):
        pin = read_json(SCRIPTS_DIR / "aislop-check-pin.json")
        self.assertEqual(set(pin.keys()), {"ref", "files"})
        self.assertEqual(pin["ref"], "aislop-check-v3")
        self.assertEqual(
            pin["files"],
            {
                "check-aislop.mjs": CHECKER_SHA256,
                "check-aislop.test.mjs": CHECKER_TEST_SHA256,
            },
        )

    def test_bootstrap_is_the_unmodified_upstream_copy(self):
        digest = hashlib.sha256(
            (SCRIPTS_DIR / "aislop-check-bootstrap.mjs").read_bytes()
        ).hexdigest()
        self.assertEqual(digest, BOOTSTRAP_SHA256)
