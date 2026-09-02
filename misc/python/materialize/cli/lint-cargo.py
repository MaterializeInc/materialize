# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check our set of Cargo.toml files for issues"""

import os
import sys
from pprint import pprint

import toml

from materialize import MZ_ROOT
from materialize.cargo import Workspace


def check_rust_versions(workspace: Workspace) -> bool:
    """Checks that every crate has a minimum specified rust version, and furthermore,
    that they are all the same."""

    rust_version_to_crate_name: dict[str | None, list[str]] = {}
    for name, crate in workspace.crates.items():
        rust_version_to_crate_name.setdefault(crate.rust_version, []).append(name)
    success = (
        len(rust_version_to_crate_name) == 1 and None not in rust_version_to_crate_name
    )
    if not success:
        print(
            "Not all crates have the same rust-version value. Rust versions found:",
            file=sys.stderr,
        )
        pprint(rust_version_to_crate_name, stream=sys.stderr)
    return success


def check_default_members(workspace: Workspace) -> bool:
    """Checks that the default members for the workspace includes all crates
    except those that are intentionally excluded."""

    EXCLUDED = {
        # This crate force enables jemalloc on Linux. We want it to be excluded
        # by default, and only pulled in by feature flags for other crates. This
        # ensures that `--no-default-features` properly disables jemalloc.
        "src/alloc-default",
        "src/fivetran-destination",
    }

    crates = set(str(c.path.relative_to(c.root)) for c in workspace.crates.values())
    default_members = set(workspace.default_members)

    missing = crates - EXCLUDED - default_members
    success = len(missing) == 0
    if not success:
        print(
            "Crates missing from workspace.default-members in root Cargo.toml:",
            file=sys.stderr,
        )
        for crate in sorted(missing):
            print(f'    "{crate}",', file=sys.stderr)
    return success


def check_workspace_dependencies(workspace: Workspace) -> bool:
    """Checks that crates use workspace dependencies instead of specifying
    versions inline when a workspace dependency is available."""

    success = True
    for name, crate in sorted(workspace.crates.items()):
        for dep, dep_types in crate.non_workspace_deps.items():
            if dep in workspace.workspace_dependencies:
                print(
                    f"{name}: {dep} should use `workspace = true` "
                    f"(found in {', '.join(dep_types)})",
                    file=sys.stderr,
                )
                success = False
    if not success:
        print(
            '\nhint: replace `dep = "version"` with `dep.workspace = true` '
            "or `dep = { workspace = true, ... }` for the above dependencies",
            file=sys.stderr,
        )
    return success


def check_fuzz_versions_mirror_root(workspace: Workspace) -> bool:
    """Checks that the cargo-fuzz crates pin the same dependency versions as the
    root workspace.

    The fuzz crates live in their own workspace (test/cargo-fuzz) because
    libFuzzer requires nightly Rust, so they cannot use `workspace = true` to
    inherit the root `[workspace.dependencies]`. They inline versions instead,
    which silently drift from the root and make a fuzz target exercise a
    different dependency than production. Enforce that any inlined version whose
    crate is also a root workspace dependency matches the root version exactly.
    Features are intentionally not compared. Fuzz crates deliberately pull a
    minimal feature set."""

    def version_req(spec: object) -> str | None:
        # None for path/workspace/git-only specs that pin no registry version.
        if isinstance(spec, str):
            return spec
        if isinstance(spec, dict) and "path" not in spec and not spec.get("workspace"):
            version = spec.get("version")
            return version if isinstance(version, str) else None
        return None

    root_versions = {
        name: version_req(spec)
        for name, spec in workspace.workspace_dependencies.items()
    }

    fuzz_workspace = MZ_ROOT / "test" / "cargo-fuzz"
    with open(fuzz_workspace / "Cargo.toml") as f:
        members = toml.load(f)["workspace"]["members"]

    success = True
    for member in members:
        with open(fuzz_workspace / member / "Cargo.toml") as f:
            config = toml.load(f)
        rel = os.path.normpath(os.path.join("test/cargo-fuzz", member))
        for section in ("dependencies", "build-dependencies", "dev-dependencies"):
            for name, spec in config.get(section, {}).items():
                version = version_req(spec)
                root_version = root_versions.get(name)
                if version is None or root_version is None:
                    continue
                if version != root_version:
                    print(
                        f'{rel}/Cargo.toml: {name} = "{version}" must match the '
                        f'root workspace version "{root_version}"',
                        file=sys.stderr,
                    )
                    success = False
    if not success:
        print(
            "\nhint: fuzz crates can't use `workspace = true`, so pin the exact "
            "root version. Update each value above to match the root Cargo.toml.",
            file=sys.stderr,
        )
    return success


def main() -> None:
    workspace = Workspace(MZ_ROOT)
    lints = [
        check_rust_versions,
        check_default_members,
        check_workspace_dependencies,
        check_fuzz_versions_mirror_root,
    ]
    # Run every lint, then combine. `success and lint(...)` would short-circuit
    # and skip the remaining lints after the first failure, under-reporting.
    success = all([lint(workspace) for lint in lints])
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
