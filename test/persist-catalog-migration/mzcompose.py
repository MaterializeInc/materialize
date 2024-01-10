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
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition) -> None:
    workflow_test_migration_and_rollback(c)
    workflow_test_epoch_migration(c)

    for i, name in enumerate(c.workflows):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_test_migration_and_rollback(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("minio", "cockroach")

    # Create some data in .
    c.up("testdrive", persistent=True)
    c.up("materialized")
    c.testdrive(
        input=dedent(
            """
    > CREATE DATABASE db3
"""
        )
    )
    c.kill("materialized")

    # Switch to persist catalog.
    # c.up("testdrive", persistent=True)
    # c.up("materialized")
    # c.testdrive(
    #     input=dedent(
    #         """
    # $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    # ALTER SYSTEM SET catalog_kind TO 'persist'
    #
    #     )
    # )
    # c.kill("materialized")

    # Reboot and check that database still exists.
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
    # c.up("testdrive", persistent=True)
    # c.testdrive(
    #     input=dedent(
    #         """
    # $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    # ALTER SYSTEM SET catalog_kind TO 'stash'
    #
    #     )
    # )
    # c.kill("materialized")
    #
    # # Reboot and check that database still exists.
    # c.up("testdrive", persistent=True)
    # c.up("materialized")
    # c.testdrive(
    #     input=dedent(
    #         """
    # > SELECT name FROM mz_databases WHERE id LIKE 'u%' AND NAME != 'materialize';
    # db3
    # db4
    # """
    #     )
    # )


def workflow_test_epoch_migration(c: Composition) -> None:
    pass
    # c.down(destroy_volumes=True)
    # c.up("minio", "cockroach")
    #
    # # Switch to emergency stash catalog so only the stash epoch is incremented.
    # c.up("testdrive", persistent=True)
    # c.up("materialized")
    # c.testdrive(
    #     input=dedent(
    #         """
    # $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    # ALTER SYSTEM SET catalog_kind TO 'emergency-stash'
    # """
    #     )
    # )
    # c.kill("materialized")
    #
    # # Start and stop Materialize with stash multiple times to increment the epoch.
    # for _ in range(0, 2):
    #     c.up("materialized")
    #     c.kill("materialized")
    #
    # # Switch to persist catalog.
    # c.up("testdrive", persistent=True)
    # c.up("materialized")
    # c.testdrive(
    #     input=dedent(
    #         """
    # $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    # ALTER SYSTEM SET catalog_kind TO 'persist'
    # """
    #     )
    # )
    #     c.kill("materialized")
    #
    # # Start and stop Materialize with persist multiple times to increment the epoch.
    # # for _ in range(0, 1):
    # c.up("materialized")
    # # c.kill("materialized")
    # #
    # # # Switch to stash catalog.
    # # c.up("testdrive", persistent=True)
    # # c.up("materialized")
    # # c.testdrive(
    # #     input=dedent(
    # #         """
    # # $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    # # ALTER SYSTEM SET catalog_kind TO 'stash'
    # # """
    # #     )
    # # )
    # # c.kill("materialized")
    # #
    # # c.up("materialized")
    # # c.kill("materialized")
