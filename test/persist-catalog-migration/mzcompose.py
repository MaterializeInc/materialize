# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.version_list import (
    get_minor_mz_versions_listed_in_docs,
)

mz_options: dict[MzVersion, str] = {}

SERVICES = [
    Materialized(catalog_store="stash"),
    Testdrive(no_reset=True),
]


def workflow_default(c: Composition) -> None:
    for i, name in enumerate(c.workflows):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_test_migration_and_rollback(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create some objects.
    c.up("materialized")
    create_objects(c, 0)

    # Switch to persist catalog.
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Reboot and check that objects still exists.
    c.up("materialized")
    check_objects(c, 0)
    # Create some more objects.
    create_objects(c, 1)

    # Rollback to stash.
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'stash'
    """
        )
    )
    c.kill("materialized")

    # Reboot and check that objects still exists.
    c.up("materialized")
    check_objects(c, 0)
    check_objects(c, 1)


# TODO(jkosh44) There's a class of tests that are missing that do the following:
#   1. Try to start up with catalog implementation A and panic during startup.
#   2. Switch to catalog implementation B while Materialize is down.
#   3. Start up successfully with catalog implementation B.
# In order to test this scenario, we need to manually alter the catalog while Materialize is down,
# which is not currently possible via mzcompose.


def workflow_test_migration_panic_after_stash_fence_then_migrate(c: Composition):
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create initial objects and update flag.
    c.up("materialized")
    create_objects(c, 0)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Panic during migration.
    try:
        with c.override(
            Materialized(
                catalog_store="stash",
                environment_extra=["FAILPOINTS=post_stash_fence=panic"],
            )
        ):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
    except UIError:
        pass

    # Complete migration and check objects.
    c.up("materialized")
    check_objects(c, 0)


def workflow_test_rollback_panic_after_stash_fence_then_rollback(c: Composition):
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create initial objects and update flag.
    c.up("materialized")
    create_objects(c, 0)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Perform migration, check objects, create more objects, and update flag.
    c.up("materialized")
    check_objects(c, 0)
    create_objects(c, 1)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'stash'
    """
        )
    )
    c.kill("materialized")

    # Panic during rollback.
    try:
        with c.override(
            Materialized(
                catalog_store="stash",
                environment_extra=["FAILPOINTS=post_stash_fence=panic"],
            )
        ):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
    except UIError:
        pass

    # Complete migration and check objects.
    c.up("materialized")
    check_objects(c, 0)
    check_objects(c, 1)


def workflow_test_migration_panic_after_fence_then_migrate(c: Composition):
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create initial objects and update flag.
    c.up("materialized")
    create_objects(c, 0)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Panic during migration.
    try:
        with c.override(
            Materialized(
                catalog_store="stash",
                environment_extra=["FAILPOINTS=post_persist_fence=panic"],
            )
        ):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
    except UIError:
        pass

    # Complete migration and check objects.
    c.up("materialized")
    check_objects(c, 0)


def workflow_test_rollback_panic_after_fence_then_rollback(c: Composition):
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create initial objects and update flag.
    c.up("materialized")
    create_objects(c, 0)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Perform migration, check objects, create more objects, and update flag.
    c.up("materialized")
    check_objects(c, 0)
    create_objects(c, 1)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'stash'
    """
        )
    )
    c.kill("materialized")

    # Panic during rollback.
    try:
        with c.override(
            Materialized(
                catalog_store="stash",
                environment_extra=["FAILPOINTS=post_persist_fence=panic"],
            )
        ):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
    except UIError:
        pass

    # Complete migration and check objects.
    c.up("materialized")
    check_objects(c, 0)
    check_objects(c, 1)


def workflow_test_migration_panic_after_write_then_migrate(c: Composition):
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Create initial objects and update flag.
    c.up("materialized")
    create_objects(c, 0)
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'persist'
    """
        )
    )
    c.kill("materialized")

    # Panic during migration.
    try:
        with c.override(
            Materialized(
                catalog_store="stash",
                environment_extra=["FAILPOINTS=migrate_post_write=panic"],
            )
        ):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
    except UIError:
        pass

    # Complete migration and check objects.
    c.up("materialized")
    check_objects(c, 0)


def workflow_test_epoch_migration(c: Composition) -> None:
    """
    Tests that Materialize doesn't crash due to the epoch going backwards after a migration.
    """
    reboots = 2
    c.down(destroy_volumes=True)
    c.up("testdrive", persistent=True)

    # Use emergency stash catalog so only the stash epoch is incremented.
    with c.override(Materialized(catalog_store="emergency-stash")):
        # Start and stop Materialize with stash multiple times to increment the epoch. Create some
        # objects each time.
        c.up("materialized")
        for i in range(0, reboots):
            create_objects(c, i)
            c.kill("materialized")
            c.up("materialized")

        # Switch to persist catalog.
        c.testdrive(
            input=dedent(
                """
        $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
        ALTER SYSTEM SET catalog_kind TO 'persist'
        """
            )
        )
        c.kill("materialized")

    # Reboot and check that objects still exist.
    c.up("materialized")
    for j in range(0, reboots):
        check_objects(c, j)

    # Start and stop Materialize with persist multiple times to increment the epoch. Create some
    # objects each time.
    for i in range(0, reboots):
        create_objects(c, reboots + i)
        c.kill("materialized")
        c.up("materialized")

    # Switch back to stash catalog.
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'stash'
    """
        )
    )
    c.kill("materialized")
    c.up("materialized")

    # Check that objects still exists.
    for i in range(0, reboots * 2):
        check_objects(c, i)


def create_objects(c: Composition, i: int):
    c.testdrive(
        input=dedent(
            f"""
    > CREATE CLUSTER c{i} SIZE = '1';
    > CREATE DATABASE db{i};
    > CREATE ROLE role{i};
    > CREATE SCHEMA sc{i};
    > CREATE TABLE t{i} (a INT);
    > CREATE VIEW v{i} AS SELECT * FROM t{i};
    > CREATE MATERIALIZED VIEW mv{i} AS SELECT * FROM t{i};
    > CREATE INDEX i{i} ON t{i}(a);
    > COMMENT ON TABLE t{i} IS 'comment{i}';

    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER DEFAULT PRIVILEGES FOR ROLE role{i} IN SCHEMA sc{i} GRANT SELECT ON TABLES TO PUBLIC;
    GRANT CREATEROLE ON SYSTEM TO role{i};
    """
        )
    )


def check_objects(c: Composition, i: int):
    c.testdrive(
        input=dedent(
            f"""
    > SELECT name FROM mz_clusters WHERE id LIKE 'u%' AND name LIKE '%{i}';
    c{i}

    > SELECT name FROM mz_databases WHERE id LIKE 'u%' AND name LIKE '%{i}';
    db{i}

    > SELECT name FROM mz_roles WHERE id LIKE 'u%' AND name LIKE '%{i}';
    role{i}

    > SELECT name FROM mz_schemas WHERE id LIKE 'u%' AND name LIKE '%{i}';
    sc{i}

    > SELECT name FROM mz_objects WHERE ID LIKE 'u%' AND name LIKE '%{i}' ORDER BY name;
    i{i}
    mv{i}
    t{i}
    v{i}

    > SELECT c.comment FROM mz_internal.mz_comments c JOIN mz_objects o ON c.id = o.id WHERE c.id LIKE 'u%' AND o.name LIKE '%{i}';
    comment{i}

    > SELECT r.name, s.name, dp.grantee, dp.privileges FROM mz_default_privileges dp JOIN mz_roles r ON dp.role_id = r.id JOIN mz_schemas s ON dp.schema_id = s.id WHERE dp.role_id LIKE 'u%' AND r.name LIKE '%{i}';
    role{i}  sc{i}  p  r

    > SELECT grantee, privilege_type FROM (SHOW PRIVILEGES ON SYSTEM) WHERE grantee LIKE '%{i}';
    role{i} CREATEROLE
    """
        )
    )


def workflow_test_version_skips(c: Composition) -> None:
    """
    Test that skipping versions when upgrading will fail.
    """

    current_version = MzVersion.parse_cargo()

    # If the current version is `v0.X.0-dev`, two_minor_releases_before will be `v0.X-2.Y`.
    # where Y is the most recent patch version of the minor version.
    two_minor_releases_before = get_minor_mz_versions_listed_in_docs()[-2]

    print(
        f"Testing that a migration from two minor releases before (={two_minor_releases_before})"
        f" to the current version (={current_version}) should fail"
    )

    c.down(destroy_volumes=True)

    with c.override(
        Materialized(
            image=f"materialize/materialized:{two_minor_releases_before}",
            options=[
                opt
                for start_version, opt in mz_options.items()
                if two_minor_releases_before >= start_version
            ],
            catalog_store="persist",
        )
    ):
        c.up("materialized")
        c.kill("materialized")

    try:
        # This will bring up version `0.X.0-dev`.
        with c.override(Materialized(catalog_store="persist")):
            # Note: We actually want to retry this 0 times, but we need to retry at least once so a
            # UIError is raised instead of an AssertionError
            c.up("materialized", max_tries=1)
            assert False, "skipping versions should fail"
    except UIError:
        # Noting useful in the error message to assert. Ideally we'd check that the error is due to
        # skipping versions.
        pass
