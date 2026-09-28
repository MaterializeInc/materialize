# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Produce one real next-minor image in a dedicated CI checkout."""

import difflib
import json
import os
import re
import signal
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import FrameType

import toml

PACKAGES = ("materialized", "environmentd", "clusterd", "persist-client")
ARTIFACT = "native-compatible-image.json"


@contextmanager
def compatible_manifests(root: Path) -> Iterator[dict[str, str]]:
    """Temporarily edit only package identities, restoring exact original bytes."""
    paths = [Path(f"src/{name}/Cargo.toml") for name in PACKAGES]
    paths.append(Path("Cargo.lock"))
    originals = {path: (root / path).read_bytes() for path in paths}
    baseline = toml.loads(originals[paths[0]].decode())["package"]["version"]
    match = re.fullmatch(r"(\d+)\.(\d+)\.\d+(?:-[0-9A-Za-z.-]+)?", baseline)
    if match is None:
        raise ValueError(f"Unsupported package version: {baseline}")
    version = f"{match[1]}.{int(match[2]) + 1}.0-dev.0"
    replacements = {}
    for name, path in zip(PACKAGES, paths):
        text = originals[path].decode()
        package = toml.loads(text)["package"]
        if package["name"] != f"mz-{name}" or package["version"] != baseline:
            raise ValueError(f"Unexpected package identity in {path}")
        text, count = re.subn(
            rf'^version = "{re.escape(baseline)}"$',
            f'version = "{version}"',
            text,
            count=1,
            flags=re.MULTILINE,
        )
        if count != 1:
            raise ValueError(f"Missing package version in {path}")
        replacements[path] = text.encode()

    lock_path = Path("Cargo.lock")
    lock = originals[lock_path].decode()
    packages = toml.loads(lock)["package"]
    for name in PACKAGES:
        entries = [p for p in packages if p["name"] == f"mz-{name}"]
        if (
            len(entries) != 1
            or entries[0].get("source")
            or entries[0]["version"] != baseline
        ):
            raise ValueError(f"Expected one local lock entry for mz-{name}")
        lock, count = re.subn(
            rf'(\[\[package\]\]\nname = "mz-{name}"\nversion = "){re.escape(baseline)}(")',
            rf"\g<1>{version}\2",
            lock,
        )
        if count != 1:
            raise ValueError(f"Missing lock version for mz-{name}")
    replacements[lock_path] = lock.encode()
    diff = "".join(
        "".join(
            difflib.unified_diff(
                originals[path].decode().splitlines(keepends=True),
                replacements[path].decode().splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )
        for path in paths
    )
    # Validate everything before writing. Keep the writes inside the finally
    # boundary so partial writes and failed image builds also restore the files.
    try:
        for path, contents in replacements.items():
            (root / path).write_bytes(contents)
        yield {"old_version": baseline, "new_version": version, "manifest_diff": diff}
        for path, contents in replacements.items():
            if (root / path).read_bytes() != contents:
                raise RuntimeError(f"Image build unexpectedly changed {path}")
    finally:
        for path, contents in originals.items():
            (root / path).write_bytes(contents)


def terminate(signum: int, frame: FrameType | None) -> None:
    # Buildkite cancellation uses SIGTERM. Unwind the restoration boundary.
    raise SystemExit(128 + signum)


def main() -> None:
    from materialize import buildkite, ui

    if not ui.env_is_truthy("BUILDKITE"):
        raise RuntimeError("Run only in a dedicated Buildkite producer checkout")
    if (
        ui.env_is_truthy("CI_COVERAGE_ENABLED")
        or os.getenv("CI_SANITIZER", "none") != "none"
    ):
        raise RuntimeError("Compatible-version producer requires an ordinary CI build")
    signal.signal(signal.SIGTERM, terminate)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    # Fresh mzimage processes see the finalized lockfile before fingerprinting.
    # Their defaults match the normal builder's native arch and CI_LTO policy.
    with compatible_manifests(Path(".")) as artifact:
        options = ["--image-registry", "materialize", "materialized"]
        subprocess.run(["bin/mzimage", "ensure", *options], check=True)
        artifact["image"] = subprocess.check_output(
            ["bin/mzimage", "spec", *options], text=True
        ).strip()
        artifact["source_revision"] = revision
    Path(ARTIFACT).write_text(json.dumps(artifact, indent=2) + "\n")
    buildkite.upload_artifact(ARTIFACT)


if __name__ == "__main__":
    main()
