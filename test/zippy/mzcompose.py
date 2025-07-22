# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Zippy generates a pseudo-random sequence of DDLs, DMLs, failures, data
validation and other events and runs it sequentially. By keeping track of the
expected state it can verify results for correctness.
"""

import random
import re
import time
from datetime import timedelta
from enum import Enum

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azure import Azurite
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.grafana import Grafana
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.prometheus import Prometheus
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.ssh_bastion_host import (
    SshBastionHost,
    setup_default_ssh_test_connection,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.zippy.framework import Test
from materialize.zippy.mz_actions import Mz0dtDeploy
from materialize.zippy.scenarios import *  # noqa: F401 F403


def create_mzs(
    azurite: bool,
    transaction_isolation: bool,
    additional_system_parameter_defaults: dict[str, str] | None = None,
) -> list[Testdrive | Materialized]:
    return [
        Materialized(
            name=mz_name,
            external_blob_store=True,
            blob_store_is_azure=azurite,
            external_metadata_store=True,
            sanity_restart=False,
            metadata_store="cockroach",
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            default_replication_factor=2,
        )
        for mz_name in ["materialized", "materialized2"]
    ] + [
        Testdrive(
            materialize_url="postgres://materialize@balancerd:6875",
            no_reset=True,
            seed=1,
            # Timeout increased since Large Zippy occasionally runs into them
            default_timeout="1200s",
            materialize_params={
                "statement_timeout": "'1800s'",
                "transaction_isolation": f"'{transaction_isolation}'",
            },
            metadata_store="cockroach",
            external_blob_store=True,
            blob_store_is_azure=azurite,
        ),
    ]


SERVICES = [
    Zookeeper(),
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Postgres(),
    Cockroach(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Azurite(),
    Mc(),
    Balancerd(),
    *create_mzs(azurite=False, transaction_isolation=False),
    Clusterd(name="storaged", workers=4),
    Grafana(),
    Prometheus(),
    SshBastionHost(),
    Persistcli(),
    MySql(),
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

    parser.add_argument(
        "--seed", metavar="N", type=int, help="Random seed", default=int(time.time())
    )

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

    parser.add_argument(
        "--system-param",
        type=str,
        action="append",
        nargs="*",
        help="System parameters to set in Materialize, i.e. what you would set with `ALTER SYSTEM SET`",
    )

    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )

    args = parser.parse_args()
    scenario_class = globals()[args.scenario]

    c.up("zookeeper", "redpanda", "ssh-bastion-host")
    if args.azurite:
        c.up("azurite")
    else:
        del c.compose["services"]["azurite"]
    # Required for backups, even with azurite
    c.enable_minio_versioning()

    if args.observability:
        c.up("prometheus", "grafana")

    print(f"Using seed {args.seed}")
    random.seed(args.seed)

    additional_system_parameter_defaults = {}
    for val in args.system_param or []:
        x = val[0].split("=", maxsplit=1)
        assert len(x) == 2, f"--system-param '{val}' should be the format <key>=<val>"
        additional_system_parameter_defaults[x[0]] = x[1]

    with c.override(
        Cockroach(
            image=f"cockroachdb/cockroach:{args.cockroach_tag}",
            # Workaround for database-issues#5719
            restart="on-failure:5",
            setup_materialize=True,
        ),
        *create_mzs(
            args.azurite,
            args.transaction_isolation,
            additional_system_parameter_defaults,
        ),
    ):
        c.up("materialized")

        c.sql(
            "ALTER SYSTEM SET max_replicas_per_cluster = 10;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        scenario = scenario_class()

        # Separate clusterd not supported by Mz0dtDeploy yet
        if Mz0dtDeploy in scenario.actions_with_weight():
            c.sql(
                """
                CREATE CLUSTER storage (SIZE = '2-2');
            """
            )
        else:
            c.sql(
                """
                CREATE CLUSTER storage REPLICAS (r2 (
                    STORAGECTL ADDRESSES ['storaged:2100'],
                    STORAGE ADDRESSES ['storaged:2103'],
                    COMPUTECTL ADDRESSES ['storaged:2101'],
                    COMPUTE ADDRESSES ['storaged:2102'],
                    WORKERS 4
                ))
            """
            )

        setup_default_ssh_test_connection(c, "zippy_ssh")

        c.rm("materialized")

        c.up({"name": "testdrive", "persistent": True})

        print("Generating test...")
        test = Test(
            scenario=scenario,
            actions=args.actions,
            max_execution_time=args.max_execution_time,
        )
        print("Running test...")
        test.run(c)
