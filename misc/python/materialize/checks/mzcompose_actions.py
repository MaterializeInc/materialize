# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services import Clusterd, Materialized


class MzcomposeAction(Action):
    pass


class StartMz(MzcomposeAction):
    def __init__(
        self, tag: Optional[str] = None, environment_extra: List[str] = []
    ) -> None:
        self.tag = tag
        self.environment_extra = environment_extra

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}")
        mz = Materialized(
            image=image,
            external_cockroach=True,
            environment_extra=self.environment_extra,
        )

        with c.override(mz):
            c.up("materialized")

        for config_param in [
            "max_tables",
            "max_sinks",
            "max_sources",
            "max_materialized_views",
            "max_secrets",
        ]:
            c.sql(
                f"ALTER SYSTEM SET {config_param} TO 1000",
                user="mz_system",
                port=6877,
            )


class KillMz(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.kill("materialized")


class UseClusterdCompute(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1
                STORAGECTL ADDRESSES ['clusterd_compute_1:2100'],
                STORAGE ADDRESSES ['clusterd_compute_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_compute_1:2101'],
                COMPUTE ADDRESSES ['clusterd_compute_1:2102'],
                WORKERS 1;
        """
        )


class KillClusterdCompute(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        with c.override(Clusterd(name="clusterd_compute_1")):
            c.kill("clusterd_compute_1")


class StartClusterdCompute(MzcomposeAction):
    def __init__(self, tag: Optional[str] = None) -> None:
        self.tag = tag

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        clusterd = Clusterd(name="clusterd_compute_1")
        if self.tag:
            clusterd = Clusterd(
                name="clusterd_compute_1",
                image=f"materialize/clusterd:{self.tag}",
            )
        print(f"Starting Compute using image {clusterd.config.get('image')}")

        with c.override(clusterd):
            c.up("clusterd_compute_1")


class RestartRedpandaDebezium(MzcomposeAction):
    """Restarts Redpanda and Debezium. Debezium is unable to survive Redpanda restarts so the two go together."""

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        for service in ["redpanda", "debezium"]:
            c.kill(service)
            c.up(service)


class RestartCockroach(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("cockroach")
        c.up("cockroach")


class RestartSourcePostgres(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("postgres")
        c.up("postgres")


class KillClusterdStorage(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd` || true")


class DropCreateDefaultReplica(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
           DROP CLUSTER REPLICA default.r1;
           CREATE CLUSTER REPLICA default.r1 SIZE '1';
        """
        )
