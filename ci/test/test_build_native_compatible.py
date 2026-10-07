# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import signal
import tempfile
import unittest
from pathlib import Path

import toml

from ci.test.build_native_compatible import PACKAGES, compatible_manifests, terminate


class CompatibleManifestsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        # Copy the real inputs into a disposable fixture, never mutate the checkout.
        checkout = Path(__file__).resolve().parents[2]
        paths = [Path(f"src/{name}/Cargo.toml") for name in PACKAGES]
        paths.append(Path("Cargo.lock"))
        self.originals = {path: (checkout / path).read_bytes() for path in paths}
        for path, contents in self.originals.items():
            (self.root / path).parent.mkdir(parents=True, exist_ok=True)
            (self.root / path).write_bytes(contents)

    def assert_restored(self) -> None:
        for path, contents in self.originals.items():
            self.assertEqual((self.root / path).read_bytes(), contents)

    def test_only_local_package_identities_change(self) -> None:
        with compatible_manifests(self.root) as artifact:
            baseline = artifact["old_version"]
            major, minor, _ = baseline.split(".", 2)
            self.assertEqual(
                artifact["new_version"], f"{major}.{int(minor) + 1}.0-dev.0"
            )
            for path, original in self.originals.items():
                expected = toml.loads(original.decode())
                if path.name == "Cargo.lock":
                    for package in expected["package"]:
                        if package["name"] in {f"mz-{name}" for name in PACKAGES}:
                            package["version"] = artifact["new_version"]
                else:
                    expected["package"]["version"] = artifact["new_version"]
                self.assertEqual(toml.loads((self.root / path).read_text()), expected)
                self.assertIn(f"--- a/{path}\n", artifact["manifest_diff"])
        self.assert_restored()

    def test_restores_on_build_failure_and_cancellation(self) -> None:
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                with self.assertRaises(SystemExit if cancel else RuntimeError):
                    with compatible_manifests(self.root):
                        if cancel:
                            terminate(signal.SIGTERM, None)
                        raise RuntimeError("image build failed")
                self.assert_restored()

    def test_rejects_mismatched_lock_before_writing(self) -> None:
        path = Path("Cargo.lock")
        contents = self.originals[path].replace(
            b'name = "mz-persist-client"', b'name = "not-the-local-package"'
        )
        (self.root / path).write_bytes(contents)
        self.originals[path] = contents
        with self.assertRaisesRegex(ValueError, "local lock entry"):
            with compatible_manifests(self.root):
                self.fail("must not build with an inconsistent lock")
        self.assert_restored()

    def test_rejects_build_time_lock_changes_and_restores(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unexpectedly changed Cargo.lock"):
            with compatible_manifests(self.root):
                with (self.root / "Cargo.lock").open("ab") as lock:
                    lock.write(b"\n# unexpected build mutation\n")
        self.assert_restored()


if __name__ == "__main__":
    unittest.main()
