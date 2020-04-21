# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utility functions only useful in CI."""

from materialize import mzbuild
from materialize import spawn
from pathlib import Path


def acquire_materialized(repo: mzbuild.Repository, out: Path) -> None:
    """Acquire a Linux materialized binary from the materialized Docker image.

    This avoids an expensive rebuild if a Docker image is available from Docker
    Hub.
    """
    mz_image = repo.images["materialized"]
    repo.resolve_dependencies([mz_image]).acquire()
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        spawn.runv(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "cat",
                mz_image.spec(),
                "/usr/local/bin/materialized",
            ],
            stdout=f,
        )
    mzbuild.chmod_x(out)
