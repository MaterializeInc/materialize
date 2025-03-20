# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check our set of Cargo.toml files for issues"""

import sys
from pprint import pprint

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
        # This crate depends on the `fivetran-sdk` submodule. We don't want
        # users building the main binaries to need to set up this submodule.
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


def main() -> None:
    workspace = Workspace(MZ_ROOT)
    lints = [check_rust_versions, check_default_members]
    success = True
    for lint in lints:
        success = success and lint(workspace)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
