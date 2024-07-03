# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time
from textwrap import dedent

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(sanity_restart=False),
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition) -> None:
    for name in buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    ):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_basic(c: Composition) -> None:
    """Verify basic 0dt deployment flow."""
    c.down(destroy_volumes=True)
    c.up("materialized")
    c.up("testdrive", persistent=True)

    # Make sure cluster is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster CASCADE;
        CREATE CLUSTER cluster SIZE '2-1';
        GRANT ALL ON CLUSTER cluster TO materialize;
        ALTER SYSTEM SET cluster = cluster;
        ALTER SYSTEM SET enable_0dt_deployment = true;
    """,
        port=6877,
        user="mz_system",
    )

    # Inserts should be reflected when writes are allowed.
    c.testdrive(
        dedent(
            """
        > SET CLUSTER = cluster;
        > CREATE table t (a int, b int);
        > INSERT INTO t VALUES (1, 2);
        > CREATE INDEX t_idx ON t (a, b);
        > CREATE MATERIALIZED VIEW mv AS SELECT sum(a) FROM t;
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        1
        > SELECT max(b) FROM t;
        2
        """
        )
    )

    # Restart in a new deploy generation, which will cause Materialize to
    # boot in read-only mode.
    with c.override(Materialized(environment_extra=["MZ_DEPLOY_GENERATION=1"])):
        c.up("materialized")

        # Materialized views should not update in read only mode.
        #
        # TODO: it should not be possible to issue any mutating queries (e.g.,
        # DDL, inserts) in read only mode.
        c.testdrive(
            dedent(
                """
            ! SET CLUSTER = cluster;
            contains:cannot write in read-only mode
            > SELECT 1
            1
            ! INSERT INTO t VALUES (3, 4);
            contains:cannot write in read-only mode
            ! SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            contains:cannot write in read-only mode
            > BEGIN ISOLATION LEVEL SERIALIZABLE;
            # TODO: Hangs
            # > SELECT * FROM mv;
            # 1
            # > SELECT max(b) FROM t;
            # 4
            > COMMIT;
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            ! INSERT INTO t VALUES (5, 6);
            contains:cannot write in read-only mode
            > BEGIN ISOLATION LEVEL SERIALIZABLE;
            # TODO: Hangs
            # > SELECT * FROM mv;
            # 1
            # > SELECT max(b) FROM t;
            # 2
            > COMMIT;
            ! DROP INDEX t_idx
            contains:cannot write in read-only mode
            > BEGIN ISOLATION LEVEL SERIALIZABLE;
            # TODO: Hangs
            # > SELECT max(b) FROM t;
            # 2
            > COMMIT;
            ! CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            contains:cannot write in read-only mode

            $ set-regex match=(s\\d+|\\d{13}|[ ]{12}0|u\\d{1,3}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > BEGIN ISOLATION LEVEL SERIALIZABLE;
            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"
            > COMMIT;
            """
            )
        )

        c.up("materialized")
        # Wait up to 60s for the new deployment to become ready for promotion.
        for _ in range(1, 60):
            result = json.loads(
                c.exec(
                    "materialized",
                    "curl",
                    "localhost:6878/api/leader/status",
                    capture=True,
                ).stdout
            )
            if result["status"] == "ReadyToPromote":
                break
            assert result["status"] == "Initializing", f"Unexpected status {result}"
            print("Not ready yet, waiting 1s")
            time.sleep(1)
        result = json.loads(
            c.exec(
                "materialized",
                "curl",
                "-X",
                "POST",
                "localhost:6878/api/leader/promote",
                capture=True,
            ).stdout
        )
        assert result["result"] == "Success", f"Unexpected result {result}"

    # After promotion, the deployment should boot with writes allowed.
    with c.override(
        Materialized(
            healthcheck=[
                "CMD-SHELL",
                """[ "$(curl -f localhost:6878/api/leader/status)" = '{"status":"IsLeader"}' ]""",
            ],
            environment_extra=["MZ_DEPLOY_GENERATION=1"],
        )
    ):
        c.up("materialized")

        c.testdrive(
            dedent(
                """
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;
            > SELECT * FROM mv;
            1
            > SELECT * FROM mv2;
            1
            > SELECT max(b) FROM t;
            2
            > INSERT INTO t VALUES (7, 8);
            > SELECT * FROM mv;
            8
            > SELECT * FROM mv2;
            8
            > SELECT max(b) FROM t;
            8
            """
            )
        )
