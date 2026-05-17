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

from materialize import buildkite
from materialize.docker import image_registry
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mz import Mz
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
    Mz(app_password=""),
]


def _mz(image: str | None, default_replication_factor: int = 0) -> Materialized:
    return Materialized(
        name="materialized",
        image=image,
        metadata_store="postgres-metadata",
        external_metadata_store=True,
        external_blob_store=True,
        sanity_restart=False,
        # default_replication_factor controls mz_system's replication factor,
        # which is the primary axis the is_cloud_env heuristic keys off.
        #   0 -> mz_system Managed with rf=0  -> self-managed
        #   1 -> mz_system Managed with rf=1+ -> cloud
        default_replication_factor=default_replication_factor,
        # MZ_SOFT_ASSERTIONS=1 turns
        # soft asserts into panics, killing environmentd mid-upgrade
        # before v82->v83 ever runs. Demote them to log-only so the upgrade
        # chain can complete and the repair can do its work.
        soft_assertions=False,
    )


def _start_at(
    c: Composition,
    version: MzVersion | None,
    default_replication_factor: int = 0,
) -> None:
    if version is None:
        image: str | None = None
        label = "current"
    else:
        image = f"{image_registry()}/materialized:{version}"
        label = str(version)
    print(
        f"Starting Materialize at {label} ({image}, "
        f"default_replication_factor={default_replication_factor})"
    )
    with c.override(
        _mz(image=image, default_replication_factor=default_replication_factor),
        fail_on_new_service=False,
    ):
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
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)
            c.down()

    workflows = buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    )
    c.test_parts(workflows, process)


def workflow_dangling(c: Composition, parser: WorkflowArgumentParser) -> None:
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


def workflow_cloud_unmanaged(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Cloud variant with mz_system Unmanaged + replicas — the originally-
    missed cloud case in v80->v81's heuristic.

    Setup: start at v26.17.1 with mz_system Managed + rf=1, then ALTER
    CLUSTER mz_system SET (MANAGED = false). The cluster becomes Unmanaged
    but retains its existing replica. The bug reproduces (the v80->v81
    heuristic returns false on Unmanaged regardless of replica count),
    leaving roles in v80 byte form. Subsequent ALTER triggers a dangling
    `-1`. v83->v84's fixed `is_cloud_env` (which also accepts
    Unmanaged + replicas) takes the cloud branch and applies the email-
    regex backfill alongside the byte-shape repair.

    Limitations:
      - Assumes `ALTER CLUSTER mz_system SET (MANAGED = false)` is supported
        at v26.17.1. If it isn't, this workflow would need a
        `Materialized` service flag to start environmentd with mz_system
        Unmanaged at bootstrap. Open question pending verification.
      - The replica persists across the ALTER (SQL semantics: switching to
        Unmanaged hands replica management to the user but does not auto-
        drop the existing managed replica). If the replica gets dropped on
        ALTER, we would need to CREATE CLUSTER REPLICA explicitly with
        unorchestrated addresses, which would require a separate clusterd
        process in this composition.
    """
    rf = 1  # start as Managed+rf=1 so we have a replica to inherit
    c.up("postgres-metadata", "minio")

    # v26.17.x (catalog 80): start Managed, then switch mz_system to
    # Unmanaged while preserving its replica.
    _start_at(c, PRE_BUG_VERSION, default_replication_factor=rf)
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER CLUSTER mz_system SET (MANAGED = false);
            """))
    # Restart at v26.17 to confirm the unmanaged state survives a restart,
    # then create roles in the resulting v80-form snapshot.
    c.kill("materialized")
    _start_at(c, PRE_BUG_VERSION, default_replication_factor=rf)
    c.testdrive(dedent("""
            > CREATE ROLE "alice@materialize.com" WITH LOGIN PASSWORD 'pw';
            > CREATE ROLE admin WITH LOGIN PASSWORD 'pw';
            """))
    c.kill("materialized")

    # v26.18.0 (catalog 81): v80->v81 sees Unmanaged mz_system -> heuristic
    # returns false (the originally-missed case) -> no backfill. Role rows
    # stay in v80 byte form.
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_VERSION, default_replication_factor=rf)
    # Trigger a dangling -1 via ALTER on a v80-form row.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE "alice@materialize.com" SUPERUSER;
            """))
    c.kill("materialized")
    _start_at(c, CATALOG_CORRUPTION_MIGRATION_VERSION, default_replication_factor=rf)
    print(
        "Expecting catalog corruption from Unmanaged mz_system path to "
        "surface as a query failure..."
    )
    _ensure_select_roles_fails(c)
    c.kill("materialized")

    # v26.24.0 (catalog 83): pass-1 of v82->v83 repairs the dangling -1 on
    # alice. admin is still in v80 byte form (no ALTER yet).
    _start_at(
        c,
        CATALOG_CORRUPTION_MIGRATION_PARTIALLY_FIXED_VERSION,
        default_replication_factor=rf,
    )
    _select_roles(c)
    c.kill("materialized")

    # current (catalog 84): v83->v84 pass-2 normalizes admin's byte shape
    # and (because is_cloud_env sees Unmanaged + replicas -> true) applies
    # the email-regex backfill where appropriate. No new dangling diffs.
    _start_at(c, None, default_replication_factor=rf)
    _select_roles(c)

    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE admin SUPERUSER;
            """))
    c.kill("materialized")
    _start_at(c, None, default_replication_factor=rf)
    _select_roles(c)


def workflow_cloud_managed(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Cloud-flavored variant: mz_system Managed with replication_factor > 0.

    Unlike workflow_dangling, the v80-form drift bug does NOT reproduce on
    this env: the v80->v81 heuristic correctly returns true on Managed+rf>0,
    so its backfill actually runs at v26.18 and roles do not sit in v80 byte
    form long enough to dangling-diff on a subsequent ALTER.

    This workflow validates the success path on cloud:
      - The full upgrade chain (v26.17 -> v26.18 -> v26.24 -> current) runs
        to completion without introducing any corruption.
      - Post-migration ALTERs on cloud do not produce dangling `-1` diffs.

    The Unmanaged-mz_system-with-replicas case (the originally-missed cloud
    variant in v80->v81) is not reachable via this workflow today; it is
    covered by the unit test
    cloud_env_with_unmanaged_mz_system_and_replicas_backfills in
    src/catalog/src/durable/upgrade/v83_to_v84.rs.
    """
    rf = 1  # cloud: Managed mz_system with replication_factor > 0
    c.up("postgres-metadata", "minio")

    # v26.17.x (catalog 80): create roles in v80 byte form. Mix of email-
    # pattern names (eligible for Frontegg backfill) and non-email names.
    _start_at(c, PRE_BUG_VERSION, default_replication_factor=rf)
    c.testdrive(dedent("""
            > CREATE ROLE "alice@materialize.com" WITH LOGIN PASSWORD 'pw';
            > CREATE ROLE "bob@materialize.com" WITH LOGIN PASSWORD 'pw';
            > CREATE ROLE admin WITH LOGIN PASSWORD 'pw';
            """))
    c.kill("materialized")

    # v26.18.0 (catalog 81): v80->v81 sees Managed+rf>0 -> true, backfills
    # auto_provision_source on email-named roles. No dangling diffs expected.
    _start_at(
        c, CATALOG_CORRUPTION_MIGRATION_VERSION, default_replication_factor=rf
    )
    _select_roles(c)
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE "alice@materialize.com" SUPERUSER;
            """))
    c.kill("materialized")
    _start_at(
        c, CATALOG_CORRUPTION_MIGRATION_VERSION, default_replication_factor=rf
    )
    _select_roles(c)
    c.kill("materialized")

    # v26.24.0 (catalog 83): v82->v83 pass-1 finds nothing to repair (no
    # dangling negatives on a healthy cloud env).
    _start_at(
        c,
        CATALOG_CORRUPTION_MIGRATION_PARTIALLY_FIXED_VERSION,
        default_replication_factor=rf,
    )
    _select_roles(c)
    c.kill("materialized")

    # current (catalog 84): v83->v84 normalizes byte shapes to canonical
    # form and reapplies the email-regex backfill on cloud envs. is_cloud_env
    # returns true. No corruption expected end-to-end.
    _start_at(c, None, default_replication_factor=rf)
    _select_roles(c)

    # Post-migration ALTERs on each role must not introduce dangling diffs.
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE "bob@materialize.com" SUPERUSER;
            ALTER ROLE admin SUPERUSER;
            """))
    c.kill("materialized")
    _start_at(c, None, default_replication_factor=rf)
    _select_roles(c)


def workflow_stale_rows(c: Composition, parser: WorkflowArgumentParser) -> None:
    """v26.17 -> current (only cause negative diff here) -> current."""
    c.up("postgres-metadata", "minio")

    _start_at(c, PRE_BUG_VERSION)
    c.testdrive(dedent("""
            > CREATE ROLE user1 WITH LOGIN PASSWORD 'password';
            """))
    c.kill("materialized")

    _start_at(c, None)
    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER ROLE user1 SUPERUSER;
            """))

    c.kill("materialized")
    _start_at(c, None)
    _select_roles(c)

    c.testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            DROP ROLE user1;
            """))
    c.testdrive(dedent("""
            > SELECT EXISTS (SELECT 1 FROM mz_roles WHERE name = 'user1');
            false
            """))
    _select_roles(c)

    c.kill("materialized")
    _start_at(c, None)
    _select_roles(c)

    c.testdrive(dedent("""
            > SELECT EXISTS (SELECT 1 FROM mz_roles WHERE name = 'user1');
            false
            """))
