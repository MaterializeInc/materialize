# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Service

SERVICES = [
    Service(
        "maelstrom-persist",
        {"mzbuild": "maelstrom-persist", "volumes": ["./maelstrom:/store"]},
    ),
]


def workflow_default(c: Composition) -> None:
    """Run the nemesis for 5 seconds as a smoke test."""
    c.run(
        "maelstrom-persist",
        "--time-limit=5",
        "--node-count=1",
        "--concurrency=2",
        "--rate=100",
        "--",
        "maelstrom",
        "--blob-uri=mem://",
        "--consensus-uri=mem://",
    )
    # TODO: Reenable this when we un-break MaelstromConsensus
    # c.run("maelstrom-persist", "--time-limit=5", "--", "maelstrom")
