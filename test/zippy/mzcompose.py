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

import requests

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
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
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.zippy.framework import Test
from materialize.zippy.scenarios import *  # noqa: F401 F403
from materialize.zippy.scenarios import UserTables

SERVICES = [
    Zookeeper(),
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Postgres(),
    Cockroach(),
    Minio(setup_materialize=True),
    Mc(),
    Balancerd(),
    # Those two are overriden below
    Materialized(),
    Clusterd(name="storaged"),
    Testdrive(),
    Grafana(),
    Prometheus(),
    SshBastionHost(),
    Persistcli(),
    Toxiproxy(),
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

    c.up("zookeeper", "redpanda", "ssh-bastion-host")
    c.enable_minio_versioning()

    if args.observability:
        c.up("prometheus", "grafana")

    jitter = 100 if issubclass(scenario_class, UserTables) else 0

    random.seed(args.seed)

    with c.override(
        Cockroach(
            image=f"cockroachdb/cockroach:{args.cockroach_tag}",
            # Workaround for #19276
            restart="on-failure:5",
            setup_materialize=True,
        ),
        Testdrive(
            materialize_url="postgres://materialize@balancerd:6875",
            no_reset=True,
            seed=1,
            default_timeout="600s",
            materialize_params={
                "statement_timeout": "'900s'",
                "transaction_isolation": f"'{args.transaction_isolation}'",
            },
        ),
        Materialized(
            external_minio="toxiproxy",
            external_cockroach="toxiproxy",
            sanity_restart=False,
        ),
        # Override so seed gets respected
        Toxiproxy(seed=random.randrange(2**63)),
    ):
        toxiproxy_start(c, jitter)

        c.up("materialized")

        c.sql(
            "ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

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

        c.up("testdrive", persistent=True)

        print("Generating test...")
        test = Test(
            scenario=scenario_class(),
            actions=args.actions,
            max_execution_time=args.max_execution_time,
        )
        print("Running test...")
        test.run(c)


def toxiproxy_start(c: Composition, jitter: int) -> None:
    c.up("toxiproxy")

    port = c.default_port("toxiproxy")
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "cockroach",
            "listen": "0.0.0.0:26257",
            "upstream": "cockroach:26257",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "minio",
            "listen": "0.0.0.0:9000",
            "upstream": "minio:9000",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies/cockroach/toxics",
        json={
            "name": "cockroach",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
    r = requests.post(
        f"http://localhost:{port}/proxies/minio/toxics",
        json={
            "name": "minio",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
