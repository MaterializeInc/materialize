# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verify that data from Kafka is only ingested exactly once, there should be no
duplicates, even after restarting Materialize.
"""

import json
import time

import requests
from psycopg import sql

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Kafka(),
    SchemaRegistry(),
    Materialized(default_replication_factor=2),
    Testdrive(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--seed",
        help="an alternate seed to use to avoid clashing with existing topics",
        type=int,
        default=1,
    )
    args = parser.parse_args()

    c.up("kafka", "schema-registry", "materialized")
    c.run_testdrive_files(
        f"--seed={args.seed}",
        "--kafka-option=group.id=group1",
        "--no-reset",
        "before-restart.td",
    )

    def snapshot() -> dict:
        response = requests.get(
            f"http://localhost:{c.port('materialized', 6878)}/api/catalog/dump",
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def evidence(label: str, state: dict) -> None:
        physical = {}
        with c.sql_connection(
            port=6877, user="mz_system", startup_params={"statement_timeout": "5s"}
        ) as conn:
            objects = conn.execute("""
                SELECT name, id FROM mz_objects WHERE name IN ('input_tbl', 'output')
                ORDER BY name
            """).fetchall()
            for name, item_id in objects:
                row = conn.execute(
                    sql.SQL("INSPECT SHARD {}").format(sql.Literal(item_id))
                ).fetchone()
                assert row is not None
                physical[name] = {
                    key: row[0][key] for key in ("shard_id", "since", "upper")
                }
        print(
            json.dumps(
                {
                    "label": label,
                    "physical": physical,
                    **{
                        key: state[key]
                        for key in (
                            "client_incarnations",
                            "client_read_requirements",
                            "maintained_read_requirements",
                            "collection_compaction_bounds",
                        )
                    },
                }
            )
        )

    before = snapshot()
    predecessors = {
        incarnation
        for incarnation, value in before["client_incarnations"].items()
        if value["replica_id"] is not None
    }
    evidence("before-restart", before)
    c.kill("materialized")
    c.up("materialized")
    if predecessors:
        # All managed replica processes were killed with this container. Native
        # Kafka admission still respects their ordinary abandonment grace.
        deadline = time.monotonic() + 420
        next_report = 0.0
        while True:
            current = snapshot()
            remaining = predecessors.intersection(current["client_incarnations"])
            if time.monotonic() >= next_report or not remaining:
                evidence("awaiting-native-takeover", current)
                next_report = time.monotonic() + 30
            if not remaining:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"Native predecessors not reclaimed: {remaining}")
            time.sleep(1)
    c.run_testdrive_files(
        f"--seed={args.seed}",
        "--no-reset",
        "--kafka-option=group.id=group2",
        "after-restart.td",
    )
    evidence("after-restart-verified", snapshot())
