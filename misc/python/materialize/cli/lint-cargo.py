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
from typing import Dict, List, Optional

from materialize import ROOT
from materialize.cargo import Workspace


def check_rust_versions(workspace: Workspace) -> bool:
    """Checks that every crate has a minimum specified rust version, and furthermore,
    that they are all the same."""

    rust_version_to_crate_name: Dict[Optional[str], List[str]] = {}
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


def check_namespaced_features(workspace: Workspace) -> bool:
    """Bans usage of Rust 1.60 namespace features.

    As of April 25 2022, namespace features are not compatible with Dependabot.
    """
    success = True
    for crate_name, crate in workspace.crates.items():
        for feature, deps in crate.features.items():
            for dep in deps:
                if dep.startswith("dep:"):
                    print(
                        f"Feature {crate_name}/{feature} uses banned namespace feature: {dep}"
                    )
                    success = False
    return success


def main() -> None:
    workspace = Workspace(ROOT)
    lints = [check_rust_versions, check_namespaced_features]
    success = True
    for lint in lints:
        success = success and lint(workspace)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
