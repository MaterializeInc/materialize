# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import itertools
import os
from pathlib import Path

from materialize import mzbuild
from materialize import spawn

IMAGES = [
    "antithesis-cp-combined",
    "antithesis-materialized",
    "antithesis-testdrive",
    "antithesis-driver",
]

REGISTRY = "us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository"


def main() -> None:
    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)
    deps = repo.resolve_dependencies([repo.images[name] for name in IMAGES])
    deps.acquire()
    for mzimage in IMAGES:
        spawn.runv(["docker", "tag", deps[mzimage].spec(), f"{REGISTRY}/{mzimage}"])
        spawn.runv(["docker", "push", f"{REGISTRY}/{mzimage}"])


if __name__ == "__main__":
    main()
