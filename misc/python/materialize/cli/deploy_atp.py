# Copyright Materialize, Inc. All rights reserved.
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

MZIMAGES = [
    "materialized",
    "atp-testdrive",
    "atp-driver",
]

IMAGES = [
    "confluentinc/cp-zookeeper:5.5.3",
    "confluentinc/cp-kafka:5.5.3",
    "confluentinc/cp-schema-registry:5.5.3",
]

REGISTRY = "REGISTRYHOST:5000"


def main() -> None:
    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)
    deps = repo.resolve_dependencies([repo.images[name] for name in MZIMAGES])
    deps.acquire()
    for image in IMAGES:
        spawn.runv(["docker", "pull", image])
    for mzimage in MZIMAGES:
        spawn.runv(["docker", "tag", deps[mzimage].spec(), f"{REGISTRY}/{mzimage}"])
    for image in IMAGES:
        spawn.runv(["docker", "tag", image, f"{REGISTRY}/{image}"])
    for image in itertools.chain(MZIMAGES, IMAGES):
        spawn.runv(["docker", "push", f"{REGISTRY}/{image}"])


if __name__ == "__main__":
    main()
