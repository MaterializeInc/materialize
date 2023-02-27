# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from enum import Enum

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Cockroach,
    Debezium,
    Materialized,
    Minio,
    Postgres,
    Redpanda,
    Testdrive,
)
from materialize.zippy.framework import Test
from materialize.zippy.scenarios import *  # noqa: F401 F403

SERVICES = [
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Postgres(),
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    # Those two are overriden below
    Materialized(),
    Clusterd(name="storaged"),
    Testdrive(),
]


class TransactionIsolation(Enum):
    SERIALIZABLE = "serializable"
    STRICT_SERIALIZABLE = "strict serializable"

    def __str__(self) -> str:
        return self.value


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """A general framework for longevity and stress testing"""

    c.silent = True

    parser.add_argument(
        "--scenario",
        metavar="SCENARIO",
        type=str,
        help="Scenario to run",
        required=True,
    )

    parser.add_argument("--seed", metavar="N", type=int, help="Random seed", default=1)

    parser.add_argument(
        "--actions",
        metavar="N",
        type=int,
        help="Number of actions to run",
        default=1000,
    )

    parser.add_argument(
        "--transaction-isolation",
        type=TransactionIsolation,
        choices=list(TransactionIsolation),
        default=TransactionIsolation.STRICT_SERIALIZABLE,
    )

    parser.add_argument(
        "--size",
        type=int,
        default=None,
        help="SIZE to use for sources, sinks, materialized views and clusters",
    )

    parser.add_argument(
        "--cockroach-tag",
        type=str,
        default=Cockroach.DEFAULT_COCKROACH_TAG,
        help="Cockroach DockerHub tag to use.",
    )

    args = parser.parse_args()
    scenario_class = globals()[args.scenario]

    c.up("redpanda")

    random.seed(args.seed)

    with c.override(
        Cockroach(image=f"cockroachdb/cockroach:{args.cockroach_tag}"),
        Testdrive(
            no_reset=True,
            seed=1,
            default_timeout="600s",
            materialize_params={
                "statement_timeout": "'900s'",
                "transaction_isolation": f"'{args.transaction_isolation}'",
            },
        ),
        Materialized(
            default_size=args.size or Materialized.Size.DEFAULT_SIZE,
            options=["--log-filter=warn"],
            external_minio=True,
            external_cockroach=True,
        ),
    ):
        c.up("materialized")
        c.sql(
            """
            CREATE CLUSTER storaged REPLICAS (r2 (
                STORAGECTL ADDRESSES ['storaged:2100'],
                STORAGE ADDRESSES ['storaged:2103'],
                COMPUTECTL ADDRESSES ['storaged:2101'],
                COMPUTE ADDRESSES ['storaged:2102'],
                WORKERS 4

            ))
        """
        )
        c.rm("materialized")

        c.up("testdrive", persistent=True)

        print("Generating test...")
        test = Test(scenario=scenario_class(), actions=args.actions)
        print("Running test...")
        test.run(c)
