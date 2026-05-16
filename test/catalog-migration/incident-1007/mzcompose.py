# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
End-to-end repro and repair of the v80-form Role byte-drift bug.

Catalog versions:
  v26.17.x -> catalog 80 (pre-bug)
  v26.18.0 -> catalog 81, introduces the buggy `v80_to_v81::upgrade` whose
             `is_cloud` heuristic silently no-ops on self-managed envs.
             Role rows stay in their v80 byte form
             (no `auto_provision_source` key) while the catalog version
             advances. Any subsequent ALTER reads the v80 form, parses it,
             and writes a retract+insert pair re-serialized through the
             current proto — which always includes `auto_provision_source:
             null` — leaving a dangling `-1`.
  v26.24.0 -> catalog 83, introduces the buggy `v82_to_v83::upgrade` whose
            who fixes the bug in v80_to_v81 but misses a case where roles that
            didn't have negative multiplicities had the stale/corrupted schema
            after the migration. We've since created a fix in 26.25.X.
  current  -> catalog 84, this PR's `v83_to_v84::upgrade` scans for the
             structural signature of the drift and emits compensating
             updates to retire the dangling `-1` and the stale `+1`.

The workflow chains v26.17 -> v26.18 -> v26.24 -> current. At v26.18, we expect querying
`mz_internal.mz_catalog_raw` to fail — `PersistPeek` enforces a per-row
non-negativity check that fires on the dangling `-1`. After upgrading to
current, the repair fires and the same query succeeds.
"""

from textwrap import dedent

from materialize.docker import image_registry
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import PostgresMetadata
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError

# Pre-bug release: catalog 80.
PRE_BUG_VERSION = MzVersion.parse_mz("v26.17.1")

# First release containing the buggy v80->v81 migration: catalog 81.
CATALOG_CORRUPTION_MIGRATION_VERSION = MzVersion.parse_mz("v26.18.0")

# First release containing the v82->v83 migration.
# We missed a case where roles that didn't have negative multiplicities
# still had the stale/corrupted schema after the migration. We've since created a
# fix in 26.25.X.
CATALOG_CORRUPTION_MIGRATION_PARTIALLY_FIXED_VERSION = MzVersion.parse_mz("v26.24.0")

SERVICES = [
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Materialized(
        external_metadata_store=True,
        external_blob_store=True,
        metadata_store="postgres-metadata",
        sanity_restart=False,
    ),
    Testdrive(
        external_blob_store=True,
        no_reset=True,
    ),
]


def _mz(image: str | None) -> Materialized:
    return Materialized(
        name="materialized",
        image=image,
        metadata_store="postgres-metadata",
        external_metadata_store=True,
        external_blob_store=True,
        sanity_restart=False,
        # Set the default replication factor since
        # one of the heuristics of a "non-cloud" environment is that the
        # replication factor is 0 for mz_system.
        default_replication_factor=0,
        # MZ_SOFT_ASSERTIONS=1 turns
        # soft asserts into panics, killing environmentd mid-upgrade
        # before v82->v83 ever runs. Demote them to log-only so the upgrade
        # chain can complete and the repair can do its work.
        soft_assertions=False,
    )


def _start_at(c: Composition, version: MzVersion | None) -> None:
    if version is None:
        image: str | None = None
        label = "current"
    else:
        image = f"{image_registry()}/materialized:{version}"
        label = str(version)
    print(f"Starting Materialize at {label} ({image})")
    with c.override(_mz(image=image), fail_on_new_service=False):
        c.up("materialized")


def _select_roles(c: Composition) -> None:
    # `PersistPeek::do_peek` enforces per-row non-negativity when
    # serving `mz_internal.mz_catalog_raw`, so the SELECT should error
    # if the row has a negative multiplicity.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            SELECT * FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'Role';
            """))


def _ensure_select_roles_fails(c: Composition) -> None:
    failed_as_expected = False
    try:
        _select_roles(c)
    except Exception as e:
        print(f"Got expected failure: {type(e).__name__}: {e}")
        failed_as_expected = True

    if not failed_as_expected:
        raise UIError(
            "expected SELECT from mz_internal.mz_catalog_raw to fail, but it "
            "succeeded"
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """v26.17 -> v26.18 (expect failure) -> v26.24 (expect success on user1, but failure on user2 and user3) -> current (expect repair)."""
    c.up("postgres-metadata", "minio")

    # Pre-bug release. Role rows written in the v80 catalog format.
    _start_at(c, PRE_BUG_VERSION)
    c.testdrive(dedent("""
            > CREATE ROLE user1 WITH LOGIN PASSWORD 'password';
            > CREATE ROLE user2 WITH LOGIN PASSWORD 'password';
            > CREATE ROLE user3 WITH LOGIN PASSWORD 'password';
            """))
    c.kill("materialized")

    # Buggy release. The v80->v81 migration runs and silently no-ops
    # on this self-managed env, so the role row doesn't get written in the v81 catalog format.
    #  The ALTER below causes a negative multiplicity, so consolidation can't merge.=
    # the update caused by the ALTER, leaving a dangling `-1` diff.
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_VERSION)
    c.testdrive(
        dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE user1 SUPERUSER;
            """),
    )

    # Still on the buggy release. The corruption persists across the
    # restart;
    c.kill("materialized")
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_VERSION)
    print("Expecting catalog corruption to surface as a query failure...")
    _ensure_select_roles_fails(c)

    # Drop the role to ensure its -1 diff gets consolidated with its +1 creation diff.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            DROP ROLE user1;
            """))
    c.testdrive(dedent("""
            > SELECT EXISTS (SELECT 1 FROM mz_roles WHERE name = 'user1');
            false
            """))
    # Ensure select roles still fails after the drop.
    _ensure_select_roles_fails(c)

    c.kill("materialized")
    # Current release. The upgrade chain runs v82->v83 (the repair) which
    # retires dangling `-1`s and normalizes any remaining v80-form rows.
    # After that no negative multiplicities remain and the SELECT succeeds.
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_PARTIALLY_FIXED_VERSION)
    _select_roles(c)

    # However, user2 is still in v80 byte
    # form, so the ALTER reads the v80 row and writes the retract+insert
    # through the current proto, leaving a fresh dangling `-1` for user2.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE user2 SUPERUSER;
            """))

    c.kill("materialized")
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_PARTIALLY_FIXED_VERSION)
    print("Expecting catalog corruption from user2 to surface as a query failure...")
    _ensure_select_roles_fails(c)

    c.kill("materialized")
    _start_at(c, None)
    # With the newest version, user2 is migrated successfully causing no negative multiplicities.
    _select_roles(c)

    # With the newest version, all roles (including user3) should have the latest schema and ALTERs
    # to it should not cause any negative multiplicities. We repeat the same test on user3 as we did on user2.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE user3 SUPERUSER;
            """))

    c.kill("materialized")
    _start_at(c, None)
    # We expect there to not be a negative multiplicity on user3 like there was for the same test on user2.
    _select_roles(c)
