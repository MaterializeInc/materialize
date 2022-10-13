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

from typing import Optional

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services import Computed, Materialized


class MzcomposeAction(Action):
    pass


class StartMz(MzcomposeAction):
    DEFAULT_MZ_OPTIONS = " ".join(
        [
            "--persist-consensus-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=consensus",
            "--storage-stash-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=storage",
            "--adapter-stash-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=adapter",
        ]
    )

    def __init__(self, tag: Optional[str] = None) -> None:
        self.tag = tag

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}")
        mz = Materialized(image=image, options=StartMz.DEFAULT_MZ_OPTIONS)

        with c.override(mz):
            c.up("materialized")

        c.wait_for_materialized()

        for config_param in ["max_tables", "max_sources"]:
            c.sql(
                f"ALTER SYSTEM SET {config_param} TO 1000",
                user="mz_system",
                port=6877,
            )


class KillMz(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.kill("materialized")


class UseComputed(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1
                REMOTE ['computed_1:2100'],
                COMPUTE ['computed_1:2102'],
                WORKERS 1;
        """
        )


class KillComputed(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        with c.override(Computed(name="computed_1")):
            c.kill("computed_1")


class StartComputed(MzcomposeAction):
    def __init__(self, tag: Optional[str] = None) -> None:
        self.tag = tag

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/computed:{self.tag}" if self.tag is not None else None
        print(f"Starting Computed using image {image}")

        computed = Computed(
            name="computed_1",
            image=image,
        )

        with c.override(computed):
            c.up("computed_1")


class RestartRedpanda(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("redpanda")
        c.start_and_wait_for_tcp(services=["redpanda"])


class RestartPostgresBackend(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("postgres-backend")
        c.up("postgres-backend")
        c.wait_for_postgres(service="postgres-backend")


class RestartSourcePostgres(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("postgres-source")
        c.up("postgres-source")
        c.wait_for_postgres(service="postgres-source")


class KillStoraged(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        # Depending on the workload, storaged may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof storaged` || true")


class DropCreateDefaultReplica(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
           DROP CLUSTER REPLICA default.r1;
           CREATE CLUSTER REPLICA default.r1 SIZE '1';
        """
        )
