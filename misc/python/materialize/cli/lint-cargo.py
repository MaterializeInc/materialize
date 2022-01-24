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
from pathlib import Path
from pprint import pprint
from typing import Dict, List, Optional

from materialize import ROOT
from materialize.cargo import Workspace


def check_rust_versions() -> bool:
    """Checks that every crate has a minimum specified rust version, and furthermore,
    that they are all the same."""

    rust_version_to_crate_name: Dict[Optional[str], List[str]] = {}
    for name, crate in Workspace(ROOT).crates.items():
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


def main() -> None:
    lints = [check_rust_versions]
    success = True
    for lint in lints:
        success = success and lint()
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
