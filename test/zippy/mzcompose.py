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
    Kafka,
    Materialized,
    Minio,
    MinioMc,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.zippy.framework import Test
from materialize.zippy.scenarios import *  # noqa: F401 F403

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Debezium(),
    Postgres(),
    Cockroach(),
    Minio(),
    MinioMc(),
    # Those two are overriden below
    Materialized(),
    Clusterd(name="storaged", storage_workers=4),
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

    args = parser.parse_args()
    scenario_class = globals()[args.scenario]

    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])

    random.seed(args.seed)

    environment_extra = ["MZ_LOG_FILTER=warn"]
    mz_options = [
        "--adapter-stash-url=postgres://root@cockroach:26257?options=--search_path=adapter",
        "--storage-stash-url=postgres://root@cockroach:26257?options=--search_path=storage",
        "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
    ]
    persist_blob_url = "s3://minioadmin:minioadmin@persist/persist?endpoint=http://minio:9000/&region=minio"

    with c.override(
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
            environment_extra=environment_extra,
            options=mz_options,
            persist_blob_url=persist_blob_url,
        )
        if args.size is None
        else Materialized(
            default_size=args.size,
            environment_extra=environment_extra,
            options=mz_options,
            persist_blob_url=persist_blob_url,
        ),
    ):
        c.up("testdrive", persistent=True)

        print("Generating test...")
        test = Test(scenario=scenario_class(), actions=args.actions)
        print("Running test...")
        test.run(c)
