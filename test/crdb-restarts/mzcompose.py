# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from textwrap import dedent
from typing import Callable

from materialize.mzcompose import Composition, ServiceHealthcheck
from materialize.mzcompose.services import Cockroach, Materialized, Testdrive
from materialize.ui import UIError

CRDB_NODE_COUNT = 4
TESTDRIVE_TIMEOUT = (
    "40s"  # We expect any CRDB disruption to not disrupt Mz for more than this timeout
)

COCKROACH_HEALTHCHECK_DISABLED = ServiceHealthcheck(
    test="/bin/true",
    interval="1s",
    start_period="30s",
)

TESTDRIVE_SCRIPT = dedent(
    """
    # This source will persist throughout the CRDB rolling restart
    > CREATE SOURCE IF NOT EXISTS s_old FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s') WITH (SIZE = '4-4');

    > SELECT COUNT(*) > 1 FROM s_old;
    true

    # This source is recreated periodically
    > DROP SOURCE IF EXISTS s_new CASCADE;
    > CREATE SOURCE s_new FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s') WITH (SIZE ='4-4');

    > SELECT COUNT(*) > 1 FROM s_new;
    true
    """
)


ALL_COCKROACH_NODES = ",".join(
    [f"cockroach{id}:26257" for id in range(CRDB_NODE_COUNT)]
)

SERVICES = [
    Testdrive(default_timeout=TESTDRIVE_TIMEOUT, no_reset=True),
    Materialized(
        depends_on=[f"cockroach{id}" for id in range(CRDB_NODE_COUNT)],
        options=[
            "--adapter-stash-url=postgres://root@cockroach:26257?options=--search_path=adapter",
            "--storage-stash-url=postgres://root@cockroach:26257?options=--search_path=storage",
            "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
        ],
    ),
    *[
        Cockroach(
            setup_materialize=True,
            name=f"cockroach{id}",
            command=[
                "start",
                "--insecure",
                f"--store=cockroach{id}",
                "--listen-addr=0.0.0.0:26257",
                f"--advertise-addr=cockroach{id}:26257",
                "--http-addr=0.0.0.0:8080",
                f"--join={ALL_COCKROACH_NODES}",
            ],
            healthcheck=COCKROACH_HEALTHCHECK_DISABLED,
        )
        for id in range(CRDB_NODE_COUNT)
    ],
]


@dataclass
class CrdbDisruption:
    name: str
    disruption: Callable


DISRUPTIONS = [
    CrdbDisruption(
        name="sigkill",
        disruption=lambda c, id: c.kill(f"cockroach{id}"),
    ),
    CrdbDisruption(
        name="sigterm",
        disruption=lambda c, id: c.kill(f"cockroach{id}", signal="SIGTERM"),
    ),
    CrdbDisruption(
        name="drain",
        disruption=lambda c, id: c.exec(
            # Execute the 'drain' command on a different node from the one that we are draining
            #
            # Draining may sometimes time out, but we continue with the restart in case this happens,
            # as a real life CRDB upgrade procedure will most likely also ignore such a timeout.
            f"cockroach{(id % 2) + 1}",
            "cockroach",
            "node",
            "drain",
            str(id + 1),
            "--insecure",
            check=False,
        ),
    ),
]


def workflow_default(c: Composition) -> None:
    """Perform rolling restarts on a CRDB cluster with CRDB_NODE_COUNT nodes and
    confirm that Mz does not hang for longer than the expected."""
    for d in DISRUPTIONS:
        run_disruption(c, d)


def run_disruption(c: Composition, d: CrdbDisruption) -> None:
    print(f"--- Running Disruption {d.name} ...")
    c.down(destroy_volumes=True)

    for id in range(CRDB_NODE_COUNT):
        c.up(f"cockroach{id}")

    c.exec("cockroach0", "cockroach", "init", "--insecure", "--host=localhost:26257")

    for query in [
        "SET CLUSTER SETTING sql.stats.forecasts.enabled = false",
        "CREATE SCHEMA IF NOT EXISTS consensus",
        "CREATE SCHEMA IF NOT EXISTS storage",
        "CREATE SCHEMA IF NOT EXISTS adapter",
    ]:
        c.exec("cockroach0", "cockroach", "sql", "--insecure", "-e", query)

    c.up("materialized")
    c.up("testdrive", persistent=True)

    # Messing with cockroach node #0 borks the cluster permanently, so we start from node #1
    for id in range(1, CRDB_NODE_COUNT):
        d.disruption(c, id)

        # We expect the testdrive fragment to complete within Testdrive's default_timeout
        # This will indicate that Mz has not hung for a prolonged period of time
        # as a result of the disruption we just introduced
        c.testdrive(input=TESTDRIVE_SCRIPT)

        # Restart the node we just disrupted so that we can safely disrupt another node
        try:
            # Node may have died already, so we eat any docker-compose exceptions
            c.kill(f"cockroach{id}")
        except UIError:
            pass
        c.up(f"cockroach{id}")

        # Confirm things continue to work after CRDB is back to full complement
        c.testdrive(input=TESTDRIVE_SCRIPT)
