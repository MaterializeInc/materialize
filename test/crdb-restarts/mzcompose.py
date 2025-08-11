# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Disrupt Cockroach and verify that Materialize recovers from it.
"""

from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import ServiceHealthcheck
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.util import selected_by_name

CRDB_NODE_COUNT = 4
TESTDRIVE_TIMEOUT = (
    "80s"  # We expect any CRDB disruption to not disrupt Mz for more than this timeout
)

COCKROACH_HEALTHCHECK_DISABLED = ServiceHealthcheck(
    test="/bin/true",
    interval="1s",
    start_period="30s",
)

INIT_SCRIPT = dedent(
    """
    # This source will persist throughout the CRDB rolling restart
    > DROP CLUSTER IF EXISTS s_old_cluster CASCADE;
    > CREATE CLUSTER s_old_cluster SIZE = 'scale=4,workers=4';
    > CREATE SOURCE s_old IN CLUSTER s_old_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');

    > SELECT COUNT(*) > 1 FROM s_old;
    true

    # This source is recreated periodically
    > DROP CLUSTER IF EXISTS s_new_cluster CASCADE;
    > CREATE CLUSTER s_new_cluster SIZE = 'scale=4,workers=4';
    > CREATE SOURCE s_new IN CLUSTER s_new_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');

    > SELECT COUNT(*) > 1 FROM s_new;
    true
    """
)

VALIDATE_SCRIPT = dedent(
    """
    > SELECT COUNT(*) > 1 FROM s_old;
    true

    # This source is recreated periodically
    > DROP SOURCE s_new CASCADE;
    > CREATE SOURCE s_new IN CLUSTER s_new_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');

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
            "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
            "--timestamp-oracle-url=postgres://root@cockroach:26257?options=--search_path=tsoracle",
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
    # Unfortunately this disruption is too aggressive and causes CRDB to enter in a state
    # where it is no longer able to service queries, with either no error or errors about
    # 'lost quorum' or 'encountered poisoned latch'
    #
    # Most likely the test kills and restarts the nodes too fast for CRDB to handle, even though
    # the nodes are taken out in succession one by one and never in parallel.
    #
    # CrdbDisruption(
    #    name="sigkill",
    #    disruption=lambda c, id: c.kill(f"cockroach{id}"),
    # ),
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


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Perform rolling restarts on a CRDB cluster with CRDB_NODE_COUNT nodes and
    confirm that Mz does not hang for longer than the expected."""
    parser.add_argument("disruptions", nargs="*", default=[d.name for d in DISRUPTIONS])

    args = parser.parse_args()

    for d in selected_by_name(args.disruptions, DISRUPTIONS):
        run_disruption(c, d)


def run_disruption(c: Composition, d: CrdbDisruption) -> None:
    print(f"--- Running Disruption {d.name} ...")
    c.down(destroy_volumes=True, sanity_restart_mz=False)

    c.up(*[f"cockroach{id}" for id in range(CRDB_NODE_COUNT)])

    c.exec("cockroach0", "cockroach", "init", "--insecure", "--host=localhost:26257")

    for query in [
        "SET CLUSTER SETTING sql.stats.forecasts.enabled = false",
        "CREATE SCHEMA IF NOT EXISTS consensus",
        "CREATE SCHEMA IF NOT EXISTS storage",
        "CREATE SCHEMA IF NOT EXISTS adapter",
        "CREATE SCHEMA IF NOT EXISTS tsoracle",
    ]:
        c.exec("cockroach0", "cockroach", "sql", "--insecure", "-e", query)

    c.up("materialized", Service("testdrive", idle=True))

    # We expect the testdrive fragment to complete within Testdrive's default_timeout
    # This will indicate that Mz has not hung for a prolonged period of time
    # as a result of the disruption we just introduced
    c.testdrive(input=INIT_SCRIPT)

    # Messing with cockroach node #0 borks the cluster permanently, so we start from node #1
    for id in range(1, CRDB_NODE_COUNT):
        d.disruption(c, id)

        # Restart the node we just disrupted so that we can safely disrupt another node
        try:
            # Node may have died already, so we eat any docker-compose exceptions
            c.kill(f"cockroach{id}")
        except UIError:
            pass
        c.up(f"cockroach{id}")

        # Confirm things continue to work after CRDB is back to full complement
        c.testdrive(input=VALIDATE_SCRIPT)
