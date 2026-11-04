# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Resolve Renovate's indirect crate updates with Cargo."""

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path


def packages(contents: bytes) -> set[tuple[str, str]]:
    return {
        (package["name"], package["version"])
        for package in tomllib.loads(contents.decode())["package"]
        if package.get("source")
        == "registry+https://github.com/rust-lang/crates.io-index"
    }


def update_lockfile(
    root: Path, original: bytes, upgrades: list[dict[str, str]]
) -> None:
    """Apply resolvable crate updates, deferring versions blocked by parent pins."""
    lockfile = root / "Cargo.lock"
    selected = sorted({(u["name"], u["from"], u["to"]) for u in upgrades})
    # JSONata has edited version strings without updating dependency references
    # or checksums. Cargo must start from the valid pre-update lockfile.
    lockfile.write_bytes(original)
    try:
        original_packages = packages(original)
        for name, old, _ in selected:
            if (name, old) not in original_packages:
                raise ValueError(
                    f"Crate {name}@{old} is absent from the original lockfile"
                )
        pending = selected
        while pending:
            failures = []
            errors = []
            for name, old, new in pending:
                checkpoint = lockfile.read_bytes()
                current = packages(checkpoint)
                if (name, old) not in current and (
                    (name, new) in current or not any(n == name for n, _ in current)
                ):
                    continue
                result = subprocess.run(
                    [
                        "cargo",
                        "update",
                        "--config",
                        "net.git-fetch-with-cli=true",
                        "--manifest-path",
                        "Cargo.toml",
                        "--package",
                        f"{name}@{old}",
                        "--precise",
                        new,
                    ],
                    cwd=root,
                    capture_output=True,
                    text=True,
                )
                resolved = packages(lockfile.read_bytes())
                if (
                    result.returncode
                    and "failed to select a version" not in result.stderr
                ):
                    raise RuntimeError(result.stderr)
                # NOTE: Cargo can return success while retaining a parent's
                # exact pin. Such an update still needs a retry.
                if result.returncode or (
                    (name, new) not in resolved and any(n == name for n, _ in resolved)
                ):
                    lockfile.write_bytes(checkpoint)
                    failures.append((name, old, new))
                    errors.append(
                        f"Cargo did not resolve requested crate {name}@{new}:\n"
                        + result.stderr
                    )
                else:
                    print(result.stderr, end="")
            if len(failures) == len(pending):
                print(
                    "Deferring blocked indirect updates:\n" + "\n".join(errors),
                    file=sys.stderr,
                )
                break
            # A parent upgrade can release an exact pin that blocked another
            # member of the group. Retry only while the group makes progress.
            pending = failures
        current = packages(lockfile.read_bytes())
        for name, _, new in selected:
            if any(n == name and version == new for n, _, version in pending):
                continue
            if (name, new) not in current and any(n == name for n, _ in current):
                raise ValueError(f"Cargo did not resolve requested crate {name}@{new}")
    except Exception:
        lockfile.write_bytes(original)
        raise


def main() -> None:
    upgrades = json.loads(
        Path(os.environ["RENOVATE_POST_UPGRADE_COMMAND_DATA_FILE"]).read_text()
    )
    original = subprocess.check_output(["git", "show", "HEAD:Cargo.lock"])
    subprocess.run(["git", "add", "--", "Cargo.lock"], check=True)
    update_lockfile(Path.cwd(), original, upgrades)


if __name__ == "__main__":
    main()
