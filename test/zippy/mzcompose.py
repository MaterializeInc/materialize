# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import re
from datetime import timedelta
from enum import Enum

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Cockroach,
    Debezium,
    Grafana,
    Kafka,
    Materialized,
    Minio,
    Postgres,
    Prometheus,
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
    Minio(setup_materialize=True),
    # Those two are overriden below
    Materialized(),
    Clusterd(name="storaged"),
    Testdrive(),
    Grafana(),
    Prometheus(),
]


class TransactionIsolation(Enum):
    SERIALIZABLE = "serializable"
    STRICT_SERIALIZABLE = "strict serializable"

    def __str__(self) -> str:
        return self.value


def parse_timedelta(arg: str) -> timedelta:
    p = re.compile(
        (r"((?P<days>-?\d+)d)?" r"((?P<hours>-?\d+)h)?" r"((?P<minutes>-?\d+)m)?"),
        re.IGNORECASE,
    )

    m = p.match(arg)
    assert m is not None

    parts = {k: int(v) for k, v in m.groupdict().items() if v}
    td = timedelta(**parts)

    assert td > timedelta(0), f"timedelta '{td}' from arg '{arg}' is not positive"
    return td


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
        "--max-execution-time", metavar="XhYmZs", type=parse_timedelta, default="1d"
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

    parser.add_argument(
        "--observability",
        action="store_true",
        help="Start Prometheus and Grafana",
    )

    args = parser.parse_args()
    scenario_class = globals()[args.scenario]

    c.up("zookeeper", "kafka", "schema-registry")

    if args.observability:
        c.up("prometheus", "grafana")

    random.seed(args.seed)

    with c.override(
        Cockroach(
            image=f"cockroachdb/cockroach:{args.cockroach_tag}",
            # Workaround for #19276
            restart="on-failure:5",
            setup_materialize=True,
        ),
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
        test = Test(
            scenario=scenario_class(),
            actions=args.actions,
            max_execution_time=args.max_execution_time,
        )
        print("Running test...")
        test.run(c)
