# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import spawn
from pathlib import Path
from . import deploy_util


def main() -> None:
    print("--- Building materialized release binary")
    spawn.runv(["cargo", "build", "--bin", "materialized", "--release"])

    print("--- Uploading binary tarball")
    deploy_util.deploy_tarball(
        "x86_64-apple-darwin", Path("target") / "release" / "materialized"
    )


if __name__ == "__main__":
    main()
