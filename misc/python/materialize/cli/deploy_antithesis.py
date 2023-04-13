# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import sys
from pathlib import Path

from materialize import mzbuild, spawn

IMAGES = [
    "antithesis-cp-combined",
    "antithesis-materialized",
    "antithesis-testdrive",
    "antithesis-driver",
]

REGISTRY = "us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository"


def main() -> None:
    root = Path(os.environ["MZ_ROOT"])
    coverage = bool(os.getenv("CI_COVERAGE_ENABLED", False))
    repo = mzbuild.Repository(root, coverage=coverage)
    deps = repo.resolve_dependencies([repo.images[name] for name in IMAGES])
    deps.acquire()

    tag = sys.argv[1] if len(sys.argv) > 1 else "latest"

    for mzimage in IMAGES:
        spawn.runv(
            ["docker", "tag", deps[mzimage].spec(), f"{REGISTRY}/{mzimage}:{tag}"]
        )
        spawn.runv(["docker", "push", f"{REGISTRY}/{mzimage}:{tag}"])


if __name__ == "__main__":
    main()
