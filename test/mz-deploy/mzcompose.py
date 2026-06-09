# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Integration tests for mz-deploy."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres

PROJECTS_DIR = MZ_ROOT / "test" / "mz-deploy" / "projects"

SERVICES = [
    Postgres(
        image="postgres:17",
        environment=[
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "POSTGRES_PASSWORD=postgres",
        ],
        volumes=[
            "./"
            + os.path.relpath(
                PROJECTS_DIR / "postgres" / "init.sql",
                loader.composition_path,
            )
            + ":/docker-entrypoint-initdb.d/init.sql",
        ],
    ),
    Materialized(
        additional_system_parameter_defaults={
            "enable_create_table_from_source": "true",
        },
    ),
]


def create_profiles(c: Composition) -> None:
    """Write profiles.toml into the projects directory, pointing at the
    mzcompose Materialized instance.  Every project under projects/ can
    then use ``--profiles-dir <PROJECTS_DIR>``.

    Every profile sets ``enable_session_rbac_checks = true`` via the
    libpq-style ``options`` map. Materialize's compiled session default is
    ``false``, and ``mz-deploy setup``'s ``is_rbac_enabled`` check ANDs the
    global and session flags — without this override, setup silently skips
    the role/grant phase in the local mzcompose environment and downstream
    GRANTs fail with "unknown role 'materialize_deployer'". Production
    deployments have the session flag flipped at the server level."""
    mz_port = c.default_port("materialized")
    # `setup` issues GRANT ... ON SYSTEM, which only mz_system can perform.
    mz_system_port = c.port("materialized", 6877)
    rbac_options = '\noptions = { enable_session_rbac_checks = "true" }'
    with open(PROJECTS_DIR / "profiles.toml", "w") as f:
        f.write(
            f'[admin]\nhost = "127.0.0.1"\nport = {mz_system_port}\nusername = "mz_system"{rbac_options}\n\n'
            f'[default]\nhost = "127.0.0.1"\nport = {mz_port}\nusername = "deploy_user"{rbac_options}\n\n'
            f'[staging]\nhost = "127.0.0.1"\nport = {mz_port}\nusername = "deploy_user"{rbac_options}\n\n'
            f'[dev]\nhost = "127.0.0.1"\nport = {mz_port}\nusername = "dev_user"{rbac_options}\n\n'
            f'[monitor]\nhost = "127.0.0.1"\nport = {mz_port}\nusername = "monitor_user"{rbac_options}\n'
        )


def run_mz_deploy(
    c: Composition,
    project_name: str,
    *args: str,
    check: bool = True,
    set_default_profile: bool = True,
) -> subprocess.CompletedProcess[str]:
    create_profiles(c)
    # Honor CARGO_TARGET_DIR (the CI builder sets it to /mnt/build) so we look
    # wherever `cargo build` actually wrote the binary; fall back to the
    # default workspace target dir for local runs. A relative value resolves
    # against MZ_ROOT, matching the cwd cargo is invoked from.
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        target_dir = Path(cargo_target)
        if not target_dir.is_absolute():
            target_dir = MZ_ROOT / target_dir
    else:
        target_dir = MZ_ROOT / "target"
    binary = target_dir / "debug" / "mz-deploy"
    project_dir = PROJECTS_DIR / project_name

    # Seed the per-project default-profile pointer (equivalent of
    # `mz-deploy profile set default`). Tests that want a non-default profile
    # still pass `--profile <name>` in `args`, which takes precedence over
    # the file. Tests exercising the no-profile code path pass
    # `set_default_profile=False` and ensure `.mzprofile` is absent.
    mzprofile = project_dir / ".mzprofile"
    if set_default_profile and not mzprofile.exists():
        mzprofile.write_text("default\n")

    # MZ_DEPLOY_PROFILE in the parent env would defeat the no-profile path.
    env = os.environ.copy()
    env.pop("MZ_DEPLOY_PROFILE", None)

    cmd = [
        str(binary),
        "-d",
        str(project_dir),
        "--profiles-dir",
        str(PROJECTS_DIR),
        *args,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"mz-deploy stdout: {result.stdout}", file=sys.stderr)
        print(f"mz-deploy stderr: {result.stderr}", file=sys.stderr)
        if check:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )
    return result


def parse_dry_run_json(result: subprocess.CompletedProcess[str]) -> dict:
    """Parse JSON output from --dry-run --output json."""
    return json.loads(result.stdout)


def count_actions(phases: list[dict], action: str) -> int:
    """Count how many items across all phases have a given action."""
    total = 0
    for phase in phases:
        for item in phase.get("results", []):
            if item.get("action") == action:
                total += 1
    return total


def find_phase(phases: list[dict], phase_name: str) -> dict | None:
    """Find a phase by name."""
    for phase in phases:
        if phase.get("phase") == phase_name:
            return phase
    return None


def phase_actions(phase: dict | None, action: str) -> list[dict]:
    """Get all actions of a given type from a phase."""
    if phase is None:
        return []
    return [a for a in phase.get("results", []) if a.get("action") == action]


def setup_base(c: Composition) -> None:
    """Build mz-deploy, start services, and initialize with roles.

    Shared by all workflows. Runs ``setup`` as superuser (admin profile)
    to create the ``materialize_*`` roles, then creates three users with
    appropriate role grants and system privileges."""
    subprocess.run(
        ["cargo", "build", "--bin", "mz-deploy"],
        cwd=MZ_ROOT,
        check=True,
    )
    c.down(destroy_volumes=True)
    c.up("postgres", "materialized")

    # Ensure profiles exist before first run_mz_deploy call
    create_profiles(c)

    # Run setup as superuser (creates materialize_* roles). The production
    # default size is not available in the mzcompose environment, so pass a
    # locally-valid size via --cluster-size.
    result = run_mz_deploy(
        c,
        "basic/v1",
        "setup",
        "--profile",
        "admin",
        "--cluster-size",
        "scale=1,workers=1",
    )
    assert result.returncode == 0, f"setup failed: {result.stderr}"

    # Create users with specific roles (system privileges require mz_system)
    c.sql("CREATE ROLE deploy_user LOGIN", user="mz_system", port=6877)
    c.sql(
        "GRANT materialize_deployer TO deploy_user",
        user="mz_system",
        port=6877,
    )
    c.sql("GRANT CREATEDB ON SYSTEM TO deploy_user", user="mz_system", port=6877)
    c.sql(
        "GRANT CREATECLUSTER ON SYSTEM TO deploy_user",
        user="mz_system",
        port=6877,
    )

    c.sql("CREATE ROLE dev_user LOGIN", user="mz_system", port=6877)
    c.sql(
        "GRANT materialize_developer TO dev_user",
        user="mz_system",
        port=6877,
    )
    c.sql("GRANT CREATEDB ON SYSTEM TO dev_user", user="mz_system", port=6877)
    c.sql(
        "GRANT CREATECLUSTER ON SYSTEM TO dev_user",
        user="mz_system",
        port=6877,
    )

    c.sql("CREATE ROLE monitor_user LOGIN", user="mz_system", port=6877)
    c.sql(
        "GRANT materialize_monitor TO monitor_user",
        user="mz_system",
        port=6877,
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    setup_base(c)

    with c.test_case("apply-initial"):
        # ── 1. Initial apply ─────────────────────────────────────
        # Dry-run first
        result = run_mz_deploy(c, "basic/v1", "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        created = count_actions(dry_run["phases"], "created")
        assert (
            created > 0
        ), f"Expected created actions in initial dry-run, got {dry_run}"

        # Execute
        result = run_mz_deploy(c, "basic/v1", "apply")
        assert result.returncode == 0, f"apply failed: {result.stderr}"

        # Verify clusters
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name IN ('compute', 'ingest', 'app') ORDER BY name"
        )
        assert (
            len(rows) == 3
        ), f"Expected clusters 'app', 'compute', 'ingest', got {rows}"

        # Verify database
        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app'")
        assert len(rows) == 1, f"Expected database 'app', got {rows}"

        # Verify secret
        rows = c.sql_query(
            "SELECT name FROM mz_secrets WHERE name = 'pgpass'",
            database="app",
        )
        assert len(rows) == 1, f"Expected secret 'pgpass', got {rows}"

        # Verify connection
        rows = c.sql_query(
            "SELECT name FROM mz_connections WHERE name = 'pg_conn'",
            database="app",
        )
        assert len(rows) == 1, f"Expected connection 'pg_conn', got {rows}"

        # Verify source in ingest schema
        rows = c.sql_query(
            "SELECT s.name FROM mz_sources s "
            "JOIN mz_schemas sc ON s.schema_id = sc.id "
            "WHERE s.name = 'pg_source' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected source 'pg_source' in ingest, got {rows}"

        # Verify tables in ingest schema
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE t.name IN ('users', 'orders') AND sc.name = 'ingest' ORDER BY t.name",
            database="app",
        )
        assert (
            len(rows) == 2
        ), f"Expected 2 tables in ingest (orders, users), got {rows}"

    with c.test_case("apply-idempotent"):
        # ── 2. Idempotent re-apply ────────────────────────────────
        # Dry-run: everything should be up_to_date
        result = run_mz_deploy(c, "basic/v1", "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        created = count_actions(dry_run["phases"], "created")
        assert (
            created == 0
        ), f"Expected no created actions on re-apply dry-run, got {created}"

        up_to_date = count_actions(dry_run["phases"], "up_to_date")
        assert up_to_date > 0, f"Expected up_to_date actions, got {up_to_date}"

        # Execute
        result = run_mz_deploy(c, "basic/v1", "apply")
        assert result.returncode == 0, f"Idempotent apply failed: {result.stderr}"

        # Verify counts unchanged
        rows = c.sql_query(
            "SELECT count(*) FROM mz_clusters WHERE name IN ('compute', 'ingest', 'app')"
        )
        assert int(rows[0][0]) == 3, f"Expected 3 clusters, got {rows[0][0]}"

        rows = c.sql_query(
            "SELECT count(*) FROM mz_sources WHERE name = 'pg_source'",
            database="app",
        )
        assert int(rows[0][0]) == 1, f"Expected 1 source, got {rows[0][0]}"

        rows = c.sql_query(
            "SELECT count(*) FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE sc.name = 'ingest'",
            database="app",
        )
        assert int(rows[0][0]) == 2, f"Expected 2 tables, got {rows[0][0]}"

    with c.test_case("apply-incremental"):
        # ── 3. Incremental apply: add table ───────────────────────
        # Dry-run: should show 1 created, 2 up_to_date for tables
        result = run_mz_deploy(c, "basic/v2", "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        tables_phase = find_phase(dry_run["phases"], "tables")
        tables_created = phase_actions(tables_phase, "created")
        tables_up_to_date = phase_actions(tables_phase, "up_to_date")
        assert (
            len(tables_created) == 1
        ), f"Expected 1 created table (products), got {len(tables_created)}"
        assert (
            len(tables_up_to_date) == 2
        ), f"Expected 2 up_to_date tables, got {len(tables_up_to_date)}"

        # Execute
        result = run_mz_deploy(c, "basic/v2", "apply")
        assert result.returncode == 0, f"Incremental apply failed: {result.stderr}"

        # Verify products table exists
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE t.name = 'products' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected products table in ingest, got {rows}"

        # Verify 3 tables total
        rows = c.sql_query(
            "SELECT count(*) FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE sc.name = 'ingest'",
            database="app",
        )
        assert int(rows[0][0]) == 3, f"Expected 3 tables, got {rows[0][0]}"

    with c.test_case("apply-alter-connection"):
        # ── 4. Modify connection ──────────────────────────────────
        # Dry-run
        result = run_mz_deploy(c, "basic/v3", "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        conn_phase = find_phase(dry_run["phases"], "connections")
        conn_altered = phase_actions(conn_phase, "altered")
        assert (
            len(conn_altered) == 1
        ), f"Expected 1 altered connection, got {len(conn_altered)}"

        # Execute
        result = run_mz_deploy(c, "basic/v3", "apply")
        assert (
            result.returncode == 0
        ), f"Modify connection apply failed: {result.stderr}"

    with c.test_case("apply-grants"):
        # ── 5. Grant changes ─────────────────────────────────────
        result = run_mz_deploy(c, "basic/v4", "apply")
        assert result.returncode == 0, f"Grant apply failed: {result.stderr}"

        # Verify apply succeeded (grant was applied without error)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE t.name = 'orders' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected orders table after grant apply, got {rows}"

        # Re-apply v1 to restore original state
        run_mz_deploy(c, "basic/v1", "apply")

    with c.test_case("unit-tests"):
        # ── 6. Run unit tests ─────────────────────────────────────
        result = run_mz_deploy(c, "basic/v1", "test", check=False)
        assert (
            result.returncode != 0
        ), "Expected non-zero exit from test (some tests intentionally fail)"

        combined_output = result.stdout + result.stderr
        # Passing tests
        for test_name in [
            "test_order_aggregation",
            "test_user_activity_counts",
            "test_top_spenders_filter",
        ]:
            assert (
                test_name in combined_output
            ), f"Expected passing test '{test_name}' in output"

        # Failing tests
        for test_name in [
            "test_user_activity_wrong_expectation",
            "test_order_stats_wrong",
        ]:
            assert (
                test_name in combined_output
            ), f"Expected failing test '{test_name}' in output"

    with c.test_case("stage-promote-v1"):
        # ── 7. Stage ──────────────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v1", "stage", "--deploy-id", "v1", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage v1 failed: {result.stderr}"

        # ops_v1 schema should exist (blue-green swap)
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name = 'ops_v1'",
            database="app",
        )
        assert len(rows) == 1, f"Expected ops_v1 schema, got {rows}"

        # compute_v1 cluster should exist (core MVs)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'compute_v1'")
        assert len(rows) == 1, f"Expected compute_v1 cluster, got {rows}"

        # app_v1 cluster should exist (ops views)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'app_v1'")
        assert len(rows) == 1, f"Expected app_v1 cluster, got {rows}"

        # ingest should NOT be staged (sources/tables are apply-only)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'ingest_v1'")
        assert len(rows) == 0, f"Expected no ingest_v1 cluster, got {rows}"

        # ── 8. Wait ──────────────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v1", "wait", "v1", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait v1 failed: {result.stderr}"

        # ── 9. Promote ───────────────────────────────────────────
        result = run_mz_deploy(c, "basic/v1", "promote", "v1", "--no-ready-check")
        assert result.returncode == 0, f"promote v1 failed: {result.stderr}"

        # Source still in ingest (untouched by promote)
        rows = c.sql_query(
            "SELECT s.name FROM mz_sources s "
            "JOIN mz_schemas sc ON s.schema_id = sc.id "
            "WHERE s.name = 'pg_source' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected pg_source in ingest after promote, got {rows}"

        # Tables still in ingest
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "WHERE t.name IN ('users', 'orders') AND sc.name = 'ingest' ORDER BY t.name",
            database="app",
        )
        assert len(rows) == 2, f"Expected tables in ingest after promote, got {rows}"

        # MV order_summary in core
        rows = c.sql_query(
            "SELECT mv.name FROM mz_materialized_views mv "
            "JOIN mz_schemas sc ON mv.schema_id = sc.id "
            "WHERE mv.name = 'order_summary' AND sc.name = 'core'",
            database="app",
        )
        assert len(rows) == 1, f"Expected order_summary MV in core, got {rows}"

        # Views in ops
        rows = c.sql_query(
            "SELECT v.name FROM mz_views v "
            "JOIN mz_schemas sc ON v.schema_id = sc.id "
            "WHERE v.name IN ('top_spenders', 'order_stats') AND sc.name = 'ops' "
            "ORDER BY v.name",
            database="app",
        )
        assert len(rows) == 2, f"Expected ops views after promote, got {rows}"

        # ingest cluster still exists
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'ingest'")
        assert len(rows) == 1, f"Expected ingest cluster after promote, got {rows}"

        # ── 10. Log ──────────────────────────────────────────────
        result = run_mz_deploy(c, "basic/v1", "log")
        assert result.returncode == 0, f"log failed: {result.stderr}"
        assert "v1" in result.stderr, f"Expected v1 in log output: {result.stderr}"

    with c.test_case("stage-abort-restage-v2"):
        # ── 11-12. Stage v2 ───────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v5", "stage", "--deploy-id", "v2", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage v2 failed: {result.stderr}"

        # compute_v2 cluster should exist (core objects use compute)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'compute_v2'")
        assert len(rows) == 1, f"Expected compute_v2 cluster, got {rows}"

        # Verify staging schemas exist
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name LIKE '%\\_v2'",
            database="app",
        )
        assert len(rows) > 0, f"Expected v2 staging schemas, got {rows}"

        # ── 13. Abort v2 ─────────────────────────────────────────
        result = run_mz_deploy(c, "basic/v5", "abort", "v2")
        assert result.returncode == 0, f"abort v2 failed: {result.stderr}"

        # No _v2 schemas or clusters remain
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name LIKE '%\\_v2'",
            database="app",
        )
        assert len(rows) == 0, f"Expected no v2 schemas after abort, got {rows}"

        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name LIKE '%\\_v2'")
        assert len(rows) == 0, f"Expected no v2 clusters after abort, got {rows}"

        # ── 14. Re-stage v2 + promote ─────────────────────────────
        result = run_mz_deploy(
            c, "basic/v5", "stage", "--deploy-id", "v2", "--allow-dirty"
        )
        assert result.returncode == 0, f"re-stage v2 failed: {result.stderr}"

        result = run_mz_deploy(
            c, "basic/v5", "wait", "v2", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait v2 failed: {result.stderr}"

        result = run_mz_deploy(c, "basic/v5", "promote", "v2", "--no-ready-check")
        assert result.returncode == 0, f"promote v2 failed: {result.stderr}"

        # Verify ingest untouched
        rows = c.sql_query(
            "SELECT s.name FROM mz_sources s "
            "JOIN mz_schemas sc ON s.schema_id = sc.id "
            "WHERE s.name = 'pg_source' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected pg_source in ingest after v2, got {rows}"

        # Core + ops views still exist
        rows = c.sql_query(
            "SELECT mv.name FROM mz_materialized_views mv "
            "JOIN mz_schemas sc ON mv.schema_id = sc.id "
            "WHERE sc.name = 'core'",
            database="app",
        )
        assert len(rows) >= 1, f"Expected core MVs after v2, got {rows}"

        rows = c.sql_query(
            "SELECT v.name FROM mz_views v "
            "JOIN mz_schemas sc ON v.schema_id = sc.id "
            "WHERE sc.name = 'ops'",
            database="app",
        )
        assert len(rows) >= 1, f"Expected ops views after v2, got {rows}"

    with c.test_case("stage-abort-restage-v3"):
        # ── 15-16. Stage v3 ───────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v6", "stage", "--deploy-id", "v3", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage v3 failed: {result.stderr}"

        # Verify staging schemas exist
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name LIKE '%\\_v3'",
            database="app",
        )
        assert len(rows) > 0, f"Expected v3 staging schemas, got {rows}"

        # ── 17. Abort v3 ─────────────────────────────────────────
        result = run_mz_deploy(c, "basic/v6", "abort", "v3")
        assert result.returncode == 0, f"abort v3 failed: {result.stderr}"

        # No _v3 schemas or clusters remain
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name LIKE '%\\_v3'",
            database="app",
        )
        assert len(rows) == 0, f"Expected no v3 schemas after abort, got {rows}"

        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name LIKE '%\\_v3'")
        assert len(rows) == 0, f"Expected no v3 clusters after abort, got {rows}"

        # ── 18. Re-stage v3 + promote ─────────────────────────────
        result = run_mz_deploy(
            c, "basic/v6", "stage", "--deploy-id", "v3", "--allow-dirty"
        )
        assert result.returncode == 0, f"re-stage v3 failed: {result.stderr}"

        result = run_mz_deploy(
            c, "basic/v6", "wait", "v3", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait v3 failed: {result.stderr}"

        result = run_mz_deploy(c, "basic/v6", "promote", "v3", "--no-ready-check")
        assert result.returncode == 0, f"promote v3 failed: {result.stderr}"

        # Verify ingest untouched
        rows = c.sql_query(
            "SELECT s.name FROM mz_sources s "
            "JOIN mz_schemas sc ON s.schema_id = sc.id "
            "WHERE s.name = 'pg_source' AND sc.name = 'ingest'",
            database="app",
        )
        assert len(rows) == 1, f"Expected pg_source after v3, got {rows}"

        # All objects intact after promote
        rows = c.sql_query(
            "SELECT mv.name FROM mz_materialized_views mv "
            "JOIN mz_schemas sc ON mv.schema_id = sc.id "
            "WHERE sc.name = 'core'",
            database="app",
        )
        assert len(rows) >= 1, f"Expected core MVs after v3, got {rows}"

        rows = c.sql_query(
            "SELECT v.name FROM mz_views v "
            "JOIN mz_schemas sc ON v.schema_id = sc.id "
            "WHERE sc.name = 'ops'",
            database="app",
        )
        assert len(rows) >= 1, f"Expected ops views after v3, got {rows}"

    with c.test_case("mz-deploy-multi-profile"):
        # ── 1. Apply default profile ──────────────────────
        result = run_mz_deploy(c, "multi-profile/v1", "apply")
        assert result.returncode == 0

        # Verify database "app" exists (no suffix for default)
        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app'")
        assert len(rows) == 1

        # Verify cluster "compute" exists (no suffix)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'compute'")
        assert len(rows) == 1

        # Verify secret comment is 'default secret' (default profile override)
        rows = c.sql_query(
            "SELECT comment FROM mz_internal.mz_comments "
            "JOIN mz_secrets ON mz_comments.id = mz_secrets.id "
            "JOIN mz_schemas ON mz_secrets.schema_id = mz_schemas.id "
            "JOIN mz_databases ON mz_schemas.database_id = mz_databases.id "
            "WHERE mz_secrets.name = 'my_secret' AND mz_databases.name = 'app'",
            database="app",
        )
        assert len(rows) == 1, f"Expected 1 comment row, got {rows}"
        assert (
            rows[0][0] == "default secret"
        ), f"Expected 'default secret', got '{rows[0][0]}'"

        # Verify default_config exists (default-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'default_config' AND sc.name = 'public' AND db.name = 'app'",
            database="app",
        )
        assert len(rows) == 1, f"Expected default_config table in app, got {rows}"

        # Verify staging_config does NOT exist (staging-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'staging_config' AND sc.name = 'public' AND db.name = 'app'",
            database="app",
        )
        assert len(rows) == 0, f"Expected no staging_config table in app, got {rows}"

        # ── 2. Stage + promote default profile (deploys MV) ──
        result = run_mz_deploy(
            c, "multi-profile/v1", "stage", "--deploy-id", "mp_default", "--allow-dirty"
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c,
            "multi-profile/v1",
            "wait",
            "mp_default",
            "--timeout",
            "300",
            "--allowed-lag",
            "86400",
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c, "multi-profile/v1", "promote", "mp_default", "--no-ready-check"
        )
        assert result.returncode == 0

        # Verify MV 'summary' has env column = 'production' (variable resolved).
        # The table is owned by deploy_user (who ran `apply`), so insert as
        # deploy_user — the default superuser `materialize` isn't the owner
        # and has no explicit INSERT grant.
        c.sql(
            "INSERT INTO app.public.my_table VALUES (1, 'test')",
            database="app",
            user="deploy_user",
        )
        rows = c.sql_query(
            "SELECT env FROM app.core.summary",
            database="app",
            user="deploy_user",
        )
        assert len(rows) == 1
        assert rows[0][0] == "production"

        # Verify ambiguous view works (variable pragma + slice syntax)
        rows = c.sql_query(
            "SELECT arr_slice::text, b FROM app.core.ambiguous ORDER BY b",
            database="app",
            user="deploy_user",
        )
        assert len(rows) == 3, f"Expected 3 rows from ambiguous view, got {rows}"

        # ── 3. Apply staging profile ──────────────────────
        result = run_mz_deploy(c, "multi-profile/v1", "apply", "--profile", "staging")
        assert result.returncode == 0

        # Verify database "app__staging" exists (suffix applied)
        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app__staging'")
        assert len(rows) == 1

        # Verify cluster "compute__staging" exists (suffix applied)
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'compute__staging'"
        )
        assert len(rows) == 1

        # Verify secret comment is 'staging secret' (staging profile override)
        rows = c.sql_query(
            "SELECT comment FROM mz_internal.mz_comments "
            "JOIN mz_secrets ON mz_comments.id = mz_secrets.id "
            "JOIN mz_schemas ON mz_secrets.schema_id = mz_schemas.id "
            "JOIN mz_databases ON mz_schemas.database_id = mz_databases.id "
            "WHERE mz_secrets.name = 'my_secret' AND mz_databases.name = 'app__staging'",
            database="app__staging",
        )
        assert (
            len(rows) == 1
        ), f"Expected 1 comment row for my_secret in app__staging, got {rows}"
        assert (
            rows[0][0] == "staging secret"
        ), f"Expected 'staging secret', got '{rows[0][0]}'"

        # Verify staging_config exists (staging-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'staging_config' AND sc.name = 'public' AND db.name = 'app__staging'",
            database="app__staging",
        )
        assert (
            len(rows) == 1
        ), f"Expected staging_config table in app__staging, got {rows}"

        # Verify default_config does NOT exist (default-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'default_config' AND sc.name = 'public' AND db.name = 'app__staging'",
            database="app__staging",
        )
        assert (
            len(rows) == 0
        ), f"Expected no default_config table in app__staging, got {rows}"

        # ── 4. Stage + promote staging profile (deploys MV) ──
        result = run_mz_deploy(
            c,
            "multi-profile/v1",
            "stage",
            "--deploy-id",
            "mp_staging",
            "--allow-dirty",
            "--profile",
            "staging",
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c,
            "multi-profile/v1",
            "wait",
            "mp_staging",
            "--timeout",
            "300",
            "--allowed-lag",
            "86400",
            "--profile",
            "staging",
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c,
            "multi-profile/v1",
            "promote",
            "mp_staging",
            "--no-ready-check",
            "--profile",
            "staging",
        )
        assert result.returncode == 0

        # Verify MV 'summary' in staging has env column = 'staging'
        c.sql(
            "INSERT INTO app__staging.public.my_table VALUES (1, 'test')",
            database="app__staging",
            user="deploy_user",
        )
        rows = c.sql_query(
            "SELECT env FROM app__staging.core.summary",
            database="app__staging",
            user="deploy_user",
        )
        assert len(rows) == 1
        assert rows[0][0] == "staging"

        # Verify ambiguous view works in staging
        rows = c.sql_query(
            "SELECT arr_slice::text, b FROM app__staging.core.ambiguous ORDER BY b",
            database="app__staging",
            user="deploy_user",
        )
        assert (
            len(rows) == 3
        ), f"Expected 3 rows from ambiguous view in staging, got {rows}"

        # ── 5. Idempotent re-apply for both profiles ──────
        # Default
        result = run_mz_deploy(
            c, "multi-profile/v1", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        assert count_actions(dry_run["phases"], "created") == 0

        # Staging
        result = run_mz_deploy(
            c,
            "multi-profile/v1",
            "apply",
            "--profile",
            "staging",
            "--dry-run",
            "--output",
            "json",
        )
        dry_run = parse_dry_run_json(result)
        assert count_actions(dry_run["phases"], "created") == 0

    with c.test_case("mz-deploy-undefined-var"):
        result = run_mz_deploy(c, "undefined-var/v1", "compile", check=False)
        assert (
            result.returncode != 0
        ), f"Expected compile to fail for undefined variable, got rc={result.returncode}"
        combined = result.stdout + result.stderr
        assert (
            "undefined_var" in combined
        ), f"Expected error to mention 'undefined_var', got: {combined}"

    with c.test_case("mz-deploy-compile-no-profile"):
        # `compile` works without an active profile when the project doesn't
        # reference any psql-style variables. No `.mzprofile`, no `--profile`,
        # no MZ_DEPLOY_PROFILE — just compile.
        result = run_mz_deploy(c, "no-profile/v1", "compile", set_default_profile=False)
        assert (
            result.returncode == 0
        ), f"Expected compile without profile to succeed, got rc={result.returncode} stderr={result.stderr}"

    with c.test_case("mz-deploy-explain-no-profile"):
        # `explain` similarly works without a profile.  It still rejects views
        # (only materialized views are explainable), so we just check that
        # profile resolution itself doesn't block the command.
        result = run_mz_deploy(
            c,
            "no-profile/v1",
            "explain",
            "app.views.foo",
            check=False,
            set_default_profile=False,
        )
        combined = result.stdout + result.stderr
        assert (
            "no profile selected" not in combined
        ), f"Expected explain to bypass profile resolution, got: {combined}"

    with c.test_case("mz-deploy-no-profile-with-variable"):
        # When a project references a variable and no profile is selected,
        # compile fails with a hint pointing the user at `mz-deploy profile set`
        # rather than the generic [<name>.variables] hint.
        result = run_mz_deploy(
            c,
            "no-profile-var/v1",
            "compile",
            check=False,
            set_default_profile=False,
        )
        assert (
            result.returncode != 0
        ), f"Expected compile to fail when variable is unresolved, got rc={result.returncode}"
        combined = result.stdout + result.stderr
        assert (
            "env_id" in combined
        ), f"Expected error to mention ':env_id', got: {combined}"
        assert (
            "no profile is selected" in combined
        ), f"Expected 'no profile is selected' hint, got: {combined}"
        assert (
            "mz-deploy profile set" in combined
        ), f"Expected hint to suggest `mz-deploy profile set`, got: {combined}"

    with c.test_case("mz-deploy-connection-cmd-still-requires-profile"):
        # Connection-requiring commands must still hard-error when no profile
        # is set.  `lock` is the simplest such command that doesn't depend on
        # prior `apply` state.
        result = run_mz_deploy(
            c,
            "no-profile/v1",
            "lock",
            check=False,
            set_default_profile=False,
        )
        assert (
            result.returncode != 0
        ), f"Expected lock to fail without a profile, got rc={result.returncode}"
        combined = result.stdout + result.stderr
        assert (
            "no profile selected" in combined
        ), f"Expected 'no profile selected' error, got: {combined}"

    # Run every other workflow as a sub-case so the default workflow exercises
    # the full suite.
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_dev(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test the dev command — developer-role overlay for inner-loop iteration."""
    setup_base(c)

    # Apply v1 for infrastructure, then stage + wait + promote so the ops
    # schema's MV is tracked as a production deployment on the `compute`
    # cluster. That promotion is what the dev-time cluster guard reads from.
    result = run_mz_deploy(c, "dev-basic/v1", "apply")
    assert result.returncode == 0, f"apply v1 failed: {result.stderr}"

    result = run_mz_deploy(
        c, "dev-basic/v1", "stage", "--deploy-id", "v1", "--allow-dirty"
    )
    assert result.returncode == 0, f"stage v1 failed: {result.stderr}"

    result = run_mz_deploy(
        c,
        "dev-basic/v1",
        "wait",
        "v1",
        "--timeout",
        "300",
        "--allowed-lag",
        "86400",
    )
    assert result.returncode == 0, f"wait v1 failed: {result.stderr}"

    result = run_mz_deploy(c, "dev-basic/v1", "promote", "v1", "--no-ready-check")
    assert result.returncode == 0, f"promote v1 failed: {result.stderr}"

    # Grant dev_user privileges on the app database so it can read production
    # tables when building the overlay MV (CREATEDB is already granted by
    # setup_base; only schema/table access needs to be added here).
    # Use mz_system (port 6877) because app is owned by deploy_user, not
    # materialize, so the default superuser cannot grant on it directly.
    c.sql(
        "GRANT USAGE ON DATABASE app TO dev_user",
        user="mz_system",
        port=6877,
    )
    rows = c.sql_query(
        "SELECT name FROM mz_schemas WHERE database_id = "
        "(SELECT id FROM mz_databases WHERE name = 'app')",
    )
    for (schema,) in rows:
        c.sql(
            f"GRANT USAGE ON SCHEMA app.{schema} TO dev_user",
            user="mz_system",
            port=6877,
        )
        c.sql(
            f"GRANT SELECT ON ALL TABLES IN SCHEMA app.{schema} TO dev_user",
            user="mz_system",
            port=6877,
        )

    # Provision a dedicated dev cluster that the `dev` profile routes to
    # via [dev.variables].compute_cluster = "compute_dev". The
    # `compute` cluster is promoted, so targeting it directly is blocked
    # by the dev-time production-cluster guard (exercised below).
    c.sql(
        "CREATE CLUSTER compute_dev (SIZE = 'scale=1,workers=1')",
        user="mz_system",
        port=6877,
    )
    c.sql(
        "GRANT CREATE ON CLUSTER compute_dev TO dev_user",
        user="mz_system",
        port=6877,
    )

    with c.test_case("mz-deploy-dev-happy-path"):
        # Run dev against v2 (the edited project with an extra column),
        # targeting the dedicated dev cluster.
        result = run_mz_deploy(
            c, "dev-basic/v2", "dev", "compute_dev", "--profile", "dev"
        )
        assert result.returncode == 0, f"dev failed: {result.stderr}"

        # Overlay database exists.
        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app__dev'")
        assert len(rows) == 1, f"expected app__dev database, got {rows}"

        # Manifest has one row for this profile+project.
        # project_name = directory.file_name() = "v2" (basename of dev-basic/v2).
        rows = c.sql_query(
            "SELECT overlay_db FROM _mz_deploy.tables.dev_overlays "
            "WHERE profile = 'dev' AND project = 'v2'",
            user="mz_system",
            port=6877,
        )
        assert rows == [("app__dev",)], f"unexpected manifest: {rows}"

        # Overlay view is queryable (run as dev_user, who owns app__dev).
        rows = c.sql_query(
            "SELECT count(*) FROM app__dev.ops.customer_ltv",
            user="dev_user",
        )
        assert len(rows) == 1

    with c.test_case("mz-deploy-dev-rerun-rebuilds"):
        # Re-run dev with no changes. Overlay is drop-and-rebuilt; final state identical.
        result = run_mz_deploy(
            c, "dev-basic/v2", "dev", "compute_dev", "--profile", "dev"
        )
        assert result.returncode == 0, f"second dev run failed: {result.stderr}"

        rows = c.sql_query(
            "SELECT overlay_db FROM _mz_deploy.tables.dev_overlays "
            "WHERE profile = 'dev' AND project = 'v2'",
            user="mz_system",
            port=6877,
        )
        assert rows == [("app__dev",)], f"manifest drifted on re-run: {rows}"

        # Overlay view still queryable after the rebuild (run as dev_user).
        rows = c.sql_query(
            "SELECT count(*) FROM app__dev.ops.customer_ltv",
            user="dev_user",
        )
        assert len(rows) == 1

    with c.test_case("mz-deploy-dev-teardown"):
        result = run_mz_deploy(
            c,
            "dev-basic/v2",
            "dev",
            "--down",
            "--profile",
            "dev",
        )
        assert result.returncode == 0, f"dev --down failed: {result.stderr}"

        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app__dev'")
        assert len(rows) == 0, f"overlay database should be dropped, got {rows}"

        rows = c.sql_query(
            "SELECT overlay_db FROM _mz_deploy.tables.dev_overlays "
            "WHERE profile = 'dev' AND project = 'v2'",
            user="mz_system",
            port=6877,
        )
        assert rows == [], f"manifest should be empty after --down, got {rows}"

    with c.test_case("mz-deploy-dev-refuses-production-cluster"):
        # Passing `compute` (the promoted cluster) as the positional target
        # must trip the dev-time guard before any DDL runs — so we use
        # `check=False` and inspect the result instead of letting
        # `run_mz_deploy` raise on non-zero exit.
        result = run_mz_deploy(
            c,
            "dev-basic/v2",
            "dev",
            "compute",
            "--profile",
            "dev",
            check=False,
        )
        assert result.returncode != 0, (
            f"dev should refuse against a promoted cluster but exited 0: "
            f"{result.stdout}\n{result.stderr}"
        )
        combined = result.stdout + result.stderr
        # The `#[error]` message is color-free and unique to this variant;
        # match on it to make sure we hit the right guard and not some other
        # failure that happens to mention `compute`. Also sanity-check that
        # the cluster name and the word "promoted" both appear.
        assert (
            "refusing to deploy dev overlay onto production cluster" in combined
        ), f"expected DevTargetsProductionCluster error; got: {combined}"
        assert (
            "compute" in combined
        ), f"error should name the protected cluster; got: {combined}"
        assert (
            "promoted" in combined.lower()
        ), f"error should explain the cluster is promoted; got: {combined}"

        # The guard must fire before any overlay state is created. The
        # teardown case above removed `app__dev`; the refused run must not
        # bring it back, nor reinsert a manifest row.
        rows = c.sql_query(
            "SELECT name FROM mz_databases WHERE name = 'app__dev'",
        )
        assert (
            len(rows) == 0
        ), f"refused dev run must not create an overlay database, got: {rows}"
        rows = c.sql_query(
            "SELECT overlay_db FROM _mz_deploy.tables.dev_overlays "
            "WHERE profile = 'dev' AND project = 'v2'",
            user="mz_system",
            port=6877,
        )
        assert (
            rows == []
        ), f"refused dev run must not insert a manifest row, got: {rows}"


def workflow_system_deps(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test that system-catalog objects are accepted as 2-part dependencies.

    Verifies:
    - `lock` succeeds with a project.toml that declares `mz_catalog.mz_objects`,
      `pg_catalog.pg_class`, etc. without a database prefix.
    - The generated `types.lock` records each system-schema entry as
      `schema.object` (no leading database).
    - Mutating the project.toml to declare a 2-part name with a non-system
      schema (e.g. `someschema.foo`) is rejected.
    """
    setup_base(c)

    project_dir = PROJECTS_DIR / "system-deps" / "v1"
    types_lock = project_dir / "types.lock"
    if types_lock.exists():
        types_lock.unlink()

    # ── Happy path: lock with system-schema deps ──────────────────────────
    result = run_mz_deploy(c, "system-deps/v1", "lock")
    assert result.returncode == 0, f"lock failed: {result.stderr}"

    assert types_lock.exists(), f"expected {types_lock} to be created"
    contents = types_lock.read_text()
    for system_dep in [
        "mz_catalog.mz_objects",
        "pg_catalog.pg_class",
        "mz_internal.mz_comments",
        "information_schema.tables",
    ]:
        assert (
            f'name = "{system_dep}"' in contents
        ), f"expected {system_dep} in types.lock; got:\n{contents}"
        # Confirm the entry is 2-part — no leading database.
        assert (
            f'name = "materialize.{system_dep}"' not in contents
        ), f"unexpected 3-part form for {system_dep} in types.lock:\n{contents}"

    # ── Negative case: 2-part non-system dep is rejected ──────────────────
    project_toml = project_dir / "project.toml"
    original_toml = project_toml.read_text()
    try:
        project_toml.write_text(
            original_toml + "\n# bad entry below\n# (overwritten)\n"
        )
        project_toml.write_text('dependencies = ["someschema.foo"]\n')
        result = run_mz_deploy(c, "system-deps/v1", "lock", check=False)
        assert (
            result.returncode != 0
        ), f"lock should reject 2-part non-system dep, got rc=0: {result.stdout}"
        assert (
            "invalid dependency" in result.stderr.lower()
            or "invalid object id" in result.stderr.lower()
        ), f"expected invalid-dependency error, got stderr:\n{result.stderr}"
    finally:
        project_toml.write_text(original_toml)


def workflow_connection_updates(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Exercise `apply` re-runs for CONNECTION objects.

    For each scenario the workflow:
      1. Applies ``connection-updates/baseline`` to put the catalog in a
         known state (creates ``pg_conn`` on the first run, otherwise
         resets it back to the baseline shape).
      2. Dry-runs the variant and asserts the ``connections`` phase plans
         exactly one ``altered`` action.
      3. Executes the variant and queries ``SHOW CREATE CONNECTION`` to
         confirm the option change is persisted in the catalog.
      4. Re-runs the variant in dry-run mode and asserts the
         ``connections`` phase plans zero altered actions and at least
         one ``up_to_date`` — i.e. apply is idempotent after the update.

    Covers the four shapes of connection drift handled by
    ``apply_connections.rs``:

    * add-option   — desired SQL contains an option the catalog lacks.
    * drop-port    — catalog has an option the desired SQL lacks.
    * change-user  — both sides have the option; the value differs.
    * change-secret— ``SECRET`` reference name differs (structural diff).
    """
    setup_base(c)

    def show_create(name: str = "pg_conn") -> str:
        rows = c.sql_query(
            f"SHOW CREATE CONNECTION app.public.{name}",
            database="app",
        )
        return rows[0][1]

    def assert_alter_plan(project: str, expected_altered: int) -> None:
        result = run_mz_deploy(c, project, "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        conn_phase = find_phase(dry_run["phases"], "connections")
        altered = phase_actions(conn_phase, "altered")
        assert len(altered) == expected_altered, (
            f"[{project}] expected {expected_altered} altered connection(s) "
            f"in dry-run, got {len(altered)}: {altered}"
        )

    def assert_idempotent_after_update(project: str) -> None:
        result = run_mz_deploy(c, project, "apply", "--dry-run", "--output", "json")
        dry_run = parse_dry_run_json(result)
        conn_phase = find_phase(dry_run["phases"], "connections")
        altered = phase_actions(conn_phase, "altered")
        up_to_date = phase_actions(conn_phase, "up_to_date")
        assert len(altered) == 0, (
            f"[{project}] re-apply should be a no-op, "
            f"got {len(altered)} altered: {altered}"
        )
        assert len(up_to_date) >= 1, (
            f"[{project}] re-apply should report up_to_date, " f"got {len(up_to_date)}"
        )

    # ── 1. Initial baseline apply ────────────────────────────────────────
    # First baseline apply also creates the `app` database, secrets, and the
    # connection — every subsequent baseline apply is a reset to that shape.
    result = run_mz_deploy(c, "connection-updates/baseline", "apply")
    assert result.returncode == 0, f"initial baseline apply failed: {result.stderr}"
    assert "PORT = 5432" in show_create()

    with c.test_case("connection-add-option"):
        # Baseline lacks SSL MODE; add-option adds it. Exercises the SET
        # branch of the SET/DROP diff for an option absent from the catalog.
        assert_alter_plan("connection-updates/add-option", expected_altered=1)
        result = run_mz_deploy(c, "connection-updates/add-option", "apply")
        assert result.returncode == 0, f"add-option apply failed: {result.stderr}"
        assert (
            "ssl mode" in show_create().lower()
        ), f"expected SSL MODE in SHOW CREATE after add, got:\n{show_create()}"
        assert_idempotent_after_update("connection-updates/add-option")

    with c.test_case("connection-drop-option"):
        # Reset to baseline (which has PORT and lacks SSL MODE), then drop
        # PORT. The reset itself drops SSL MODE — sanity check the reset
        # also reports altered=1.
        assert_alter_plan("connection-updates/baseline", expected_altered=1)
        result = run_mz_deploy(c, "connection-updates/baseline", "apply")
        assert result.returncode == 0, f"baseline reset apply failed: {result.stderr}"
        assert "ssl mode" not in show_create().lower()

        # Now drop PORT.
        assert_alter_plan("connection-updates/drop-port", expected_altered=1)
        result = run_mz_deploy(c, "connection-updates/drop-port", "apply")
        assert result.returncode == 0, f"drop-port apply failed: {result.stderr}"
        assert (
            "PORT = 5432" not in show_create()
        ), f"expected PORT removed from SHOW CREATE, got:\n{show_create()}"
        assert_idempotent_after_update("connection-updates/drop-port")

    with c.test_case("connection-change-option-value"):
        # Reset to baseline (re-adds PORT), then change USER value. The
        # presence/absence of the substring `mz_alt_user` is the unambiguous
        # signal — the literal `postgres` also appears in HOST and DATABASE,
        # and the SHOW CREATE output's exact quoting around USER is
        # version-sensitive.
        run_mz_deploy(c, "connection-updates/baseline", "apply")
        assert (
            "mz_alt_user" not in show_create()
        ), f"expected no mz_alt_user in baseline SHOW CREATE, got:\n{show_create()}"

        assert_alter_plan("connection-updates/change-user", expected_altered=1)
        result = run_mz_deploy(c, "connection-updates/change-user", "apply")
        assert result.returncode == 0, f"change-user apply failed: {result.stderr}"
        assert (
            "mz_alt_user" in show_create()
        ), f"expected mz_alt_user in SHOW CREATE after change, got:\n{show_create()}"
        assert_idempotent_after_update("connection-updates/change-user")

    with c.test_case("connection-change-secret-ref"):
        # Reset to baseline (which references pgpass), then switch to
        # pgpass_v2. The two secrets resolve to the same plaintext value;
        # apply must still issue an ALTER because the diff is structural.
        run_mz_deploy(c, "connection-updates/baseline", "apply")
        assert "SECRET app.public.pgpass" in show_create()
        assert "SECRET app.public.pgpass_v2" not in show_create()

        assert_alter_plan("connection-updates/change-secret", expected_altered=1)
        result = run_mz_deploy(c, "connection-updates/change-secret", "apply")
        assert result.returncode == 0, f"change-secret apply failed: {result.stderr}"
        sql = show_create()
        assert "SECRET app.public.pgpass_v2" in sql, (
            f"expected pgpass_v2 in SHOW CREATE after secret change, " f"got:\n{sql}"
        )
        assert_idempotent_after_update("connection-updates/change-secret")
