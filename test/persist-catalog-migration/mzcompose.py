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
    for i, name in enumerate(c.workflows):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_test_migration_and_rollback(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("minio", "cockroach")
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


# Tests that Materialize doesn't crash due to the epoch going backwards after a migration.
def workflow_test_epoch_migration(c: Composition) -> None:
    reboots = 2
    c.down(destroy_volumes=True)
    c.up("minio", "cockroach")
    c.up("testdrive", persistent=True)

    # Switch to emergency stash catalog so only the stash epoch is incremented.
    c.up("materialized")
    c.testdrive(
        input=dedent(
            """
    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
    ALTER SYSTEM SET catalog_kind TO 'emergency-stash'
    """
        )
    )
    c.kill("materialized")

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
