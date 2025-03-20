# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import datetime
import itertools
import os
import stat
import tempfile
import xml.etree.ElementTree as ET
from textwrap import dedent

import pytz

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metabase import Metabase
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.util import all_subclasses

SERVICES = [
    Materialized(support_external_clusterd=True),
    Testdrive(no_reset=True),
    Postgres(),
    Metabase(),
    Service(
        name="benchbase",
        config={
            "mzbuild": "benchbase",
            "init": True,
            "volumes": ["./config:/config"],
        },
    ),
    Clusterd(name="clusterd_1", workers=4, cpu="4"),
]

service_names = [
    "materialized",
    "postgres",
]


def setup(c: Composition, workers: int) -> None:
    c.up(*service_names)

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        DROP CLUSTER IF EXISTS single_replica_cluster CASCADE;
        CREATE CLUSTER single_replica_cluster REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_1:2100'],
                STORAGE ADDRESSES ['clusterd_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_1:2101'],
                COMPUTE ADDRESSES ['clusterd_1:2102']
            )
        );
        GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO materialize;
    """,
        port=6877,
        user="mz_system",
    )


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_main(c: Composition, parser: WorkflowArgumentParser) -> None:

    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument(
        "--workers",
        type=int,
        metavar="N",
        default=2,
        help="set the default number of workers",
    )

    args = parser.parse_args()

    scenarios = buildkite.shard_list(
        (
            [globals()[args.scenario]]
            if args.scenario
            else list(all_subclasses(Benchbase))
        ),
        lambda s: s.__name__,
    )
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {scenarios}"
    )

    if not scenarios:
        return

    setup(c, args.workers)

    for scenario in scenarios:
        scenario.run(c)


class Benchbase:
    """Benchbase benchmark"""

    BENCH: str
    TEMPLATE: str
    PARAMS: dict[str, str]

    @classmethod
    def run(cls, c: Composition) -> None:

        c.up("clusterd_1")

        path = MZ_ROOT / "test" / "benchbase"

        tree = ET.parse(path / cls.TEMPLATE)
        for param, value in cls.PARAMS.items():
            node = tree.find(f".//{param}")
            if not node:
                raise ValueError(f"Could not find element .//{param} in {cls.TEMPLATE}")
            node.text = value

        os.makedirs(path / "config", exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path / "config", mode="w") as config:
            tree.write(config, encoding="unicode")
            config.flush()
            os.chmod(
                config.name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
            )

            relative_config = "/" + os.path.relpath(config.name, path)
            cls.run_inner(c, relative_config)

    @classmethod
    def benchbase(
        cls,
        c: Composition,
        config: str,
        create: bool = False,
        load: bool = False,
        execute: bool = False,
    ):
        print(config)
        c.run(
            "benchbase",
            "-b",
            cls.BENCH,
            "-c",
            config,
            f"--create={create}",
            f"--load={load}",
            f"--execute={execute}",
            rm=True,
        )

    @classmethod
    def run_inner(cls, c: Composition, config) -> None:
        c.testdrive(
            dedent(
                """
                $ postgres-connect name=mz_system url=postgres://mz_system@materialized:6877/materialize
                $ postgres-execute connection=mz_system
                ALTER SYSTEM SET wallclock_lag_recording_interval = '10s';
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public CASCADE;
                DROP PUBLICATION IF EXISTS mz_source;
                CREATE SCHEMA public;
                """
            ),
        )

        # Create tables.
        cls.benchbase(c, config, create=True)

        tables = c.sql_query(
            "SELECT tablename FROM pg_catalog.pg_tables where schemaname = 'public'",
            service="postgres",
            database="postgres",
            user="postgres",
            password="postgres",
        )

        c.testdrive(
            "$ postgres-execute connection=postgres://postgres:postgres@postgres\n"
            + "\n".join(
                f"ALTER TABLE {table[0]} REPLICA IDENTITY FULL;" for table in tables
            ),
        )
        c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE PUBLICATION mz_source FOR ALL TABLES;
                > CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
                > DROP CONNECTION IF EXISTS pg CASCADE;
                > CREATE CONNECTION pg TO POSTGRES (
                    HOST postgres,
                    DATABASE postgres,
                    USER postgres,
                    PASSWORD SECRET pgpass
                    );
                > CREATE SOURCE p
                    IN CLUSTER single_replica_cluster
                    FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
                    FOR ALL TABLES;
                """
            ),
        )

        # Generate initial data.
        cls.benchbase(c, config, load=True)

        # Wait for initial snapshot to complete.
        c.testdrive(
            "".join(
                dedent(
                    f"""
                    > SELECT count(*) > 0 FROM {table[0]};
                    true
                    """
                )
                for table in tables
            ),
        )

        execute_started = datetime.datetime.now(pytz.UTC)
        # Run update workload
        cls.benchbase(c, config, execute=True)
        execute_done = datetime.datetime.now(pytz.UTC)

        lag = c.sql_query(
            f"""
            SELECT
                obj.name,
                lag.lag,
                lag.occurred_at
            FROM
                mz_internal.mz_wallclock_lag_history lag,
                mz_catalog.mz_objects obj
            WHERE
                lag.object_id LIKE 'u%'
                AND lag.object_id = obj.id
                AND obj.name IN ({",".join(f"'{table[0]}'" for table in tables)})
            ORDER BY
                obj.name,
                lag.occurred_at ASC
            """
        )

        lag_by_table = itertools.groupby(lag, lambda row: row[0])

        for table, rows in lag_by_table:
            rows = list(rows)
            max_lag_load = datetime.timedelta(seconds=0)
            max_lag_execute = datetime.timedelta(seconds=0)
            print(f"Wall clock lag for {table}:")
            for row in rows[1:]:
                print(
                    f"  {row[2]}: {row[1]} {row[2] < execute_started} {row[2] < execute_done}"
                )
                if row[2] < execute_started:
                    max_lag_load = max(max_lag_load, row[1])
                elif row[2] < execute_done:
                    max_lag_execute = max(max_lag_execute, row[1])
            print(f"  Max lag during load: {max_lag_load}")
            print(f"  Max lag during execute: {max_lag_execute}")


class TpccSf10(Benchbase):
    BENCH = "tpcc"
    TEMPLATE = "./benchbase/tpcc.xml"
    PARAMS = {
        "scalefactor": "10",
        "terminals": "10",
        "time": "300",
        "rate": "10000",
    }
