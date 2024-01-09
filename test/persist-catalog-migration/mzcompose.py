# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Minio(setup_materialize=True),
    Cockroach(setup_materialize=True),
    Materialized(catalog_store="stash"),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    workflow_test_migration_and_rollback(c)
    # for i, name in enumerate(c.workflows):
    #     with c.test_case(name):
    #         c.workflow(name)


def workflow_test_migration_and_rollback(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("minio", "cockroach")

    # Create some data in .
    with c.override(
            Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.testdrive(
            input=dedent(
                """
        > CREATE DATABASE db3
    """
            )
        )
        c.down("materialized")

    # Switch to persist catalog.
    # with c.override(
    #         Testdrive(no_reset=True),
    # ):
    #     c.up("testdrive", persistent=True)
    #     c.up("materialized")
    #     c.testdrive(
    #         input=dedent(
    #             """
    #     $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    #     ALTER SYSTEM SET catalog_kind TO 'persist'
    # """
    #         )
    #     )
    #     c.down("materialized")

    # Reboot and check that database still exists.
    with c.override(
            Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")
        c.testdrive(
            input=dedent(
                """
        > SELECT name FROM mz_databases WHERE id LIKE 'u%' AND NAME != 'materialize';
        db3
        
        > CREATE DATABASE db4
    """
            )
        )

    # # Rollback to stash.
    # with c.override(
    #         Testdrive(no_reset=True),
    # ):
    #     c.up("testdrive", persistent=True)
    #     c.testdrive(
    #         input=dedent(
    #             """
    #     $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    #     ALTER SYSTEM SET catalog_kind TO 'stash'
    # """
    #         )
    #     )
    #     c.down("materialized")
    #
    # # Reboot and check that database still exists.
    # with c.override(
    #         Testdrive(no_reset=True),
    # ):
    #     c.up("testdrive", persistent=True)
    #     c.up("materialized")
    #     c.testdrive(
    #         input=dedent(
    #             """
    #     > SELECT name FROM mz_databases WHERE id LIKE 'u%' AND NAME != 'materialize';
    #     db3
    #     db4
    # """
    #         )
    #     )
