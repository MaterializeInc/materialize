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
    then use ``--profiles-dir <PROJECTS_DIR>``."""
    mz_port = c.default_port("materialized")
    with open(PROJECTS_DIR / "profiles.toml", "w") as f:
        f.write(
            f'[default]\nhost = "127.0.0.1"\nport = {mz_port}\nuser = "materialize"\n\n'
            f'[staging]\nhost = "127.0.0.1"\nport = {mz_port}\nuser = "materialize"\n'
        )


def run_mz_deploy(
    c: Composition,
    project_name: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    create_profiles(c)
    binary = MZ_ROOT / "target" / "debug" / "mz-deploy"
    project_dir = PROJECTS_DIR / project_name
    cmd = [
        str(binary),
        "-d",
        str(project_dir),
        "--profiles-dir",
        str(PROJECTS_DIR),
        *args,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
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


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    subprocess.run(
        ["cargo", "build", "--bin", "mz-deploy"],
        cwd=MZ_ROOT,
        check=True,
    )
    c.down(destroy_volumes=True)
    c.up("postgres", "materialized")

    with c.test_case("mz-deploy"):
        # ════════════════════════════════════════════════════════════
        # Phase 1: Apply
        # ════════════════════════════════════════════════════════════

        # ── 1. Initial apply ─────────────────────────────────────
        # Dry-run first
        result = run_mz_deploy(
            c, "basic/v1", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        created = count_actions(dry_run["phases"], "created")
        assert created > 0, f"Expected created actions in initial dry-run, got {dry_run}"

        # Execute
        result = run_mz_deploy(c, "basic/v1", "apply")
        assert result.returncode == 0, f"apply failed: {result.stderr}"

        # Verify clusters
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name IN ('compute', 'ingest', 'app') ORDER BY name"
        )
        assert len(rows) == 3, f"Expected clusters 'app', 'compute', 'ingest', got {rows}"

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
        assert len(rows) == 2, f"Expected 2 tables in ingest (orders, users), got {rows}"

        # ── 2. Idempotent re-apply ────────────────────────────────
        # Dry-run: everything should be up_to_date
        result = run_mz_deploy(
            c, "basic/v1", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        created = count_actions(dry_run["phases"], "created")
        assert created == 0, f"Expected no created actions on re-apply dry-run, got {created}"

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

        # ── 3. Incremental apply: add table ───────────────────────
        # Dry-run: should show 1 created, 2 up_to_date for tables
        result = run_mz_deploy(
            c, "basic/v2", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        tables_phase = find_phase(dry_run["phases"], "tables")
        tables_created = phase_actions(tables_phase, "created")
        tables_up_to_date = phase_actions(tables_phase, "up_to_date")
        assert len(tables_created) == 1, (
            f"Expected 1 created table (products), got {len(tables_created)}"
        )
        assert len(tables_up_to_date) == 2, (
            f"Expected 2 up_to_date tables, got {len(tables_up_to_date)}"
        )

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

        # ── 4. Modify connection ──────────────────────────────────
        # Dry-run
        result = run_mz_deploy(
            c, "basic/v3", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        conn_phase = find_phase(dry_run["phases"], "connections")
        conn_altered = phase_actions(conn_phase, "altered")
        assert len(conn_altered) == 1, (
            f"Expected 1 altered connection, got {len(conn_altered)}"
        )

        # Execute
        result = run_mz_deploy(c, "basic/v3", "apply")
        assert result.returncode == 0, f"Modify connection apply failed: {result.stderr}"

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

        # ════════════════════════════════════════════════════════════
        # Phase 2: Unit tests
        # ════════════════════════════════════════════════════════════

        # ── 6. Run unit tests ─────────────────────────────────────
        result = run_mz_deploy(c, "basic/v1", "test", check=False)
        assert result.returncode != 0, (
            f"Expected non-zero exit from test (some tests intentionally fail)"
        )

        combined_output = result.stdout + result.stderr
        # Passing tests
        for test_name in [
            "test_order_aggregation",
            "test_user_activity_counts",
            "test_top_spenders_filter",
        ]:
            assert test_name in combined_output, (
                f"Expected passing test '{test_name}' in output"
            )

        # Failing tests
        for test_name in [
            "test_user_activity_wrong_expectation",
            "test_order_stats_wrong",
        ]:
            assert test_name in combined_output, (
                f"Expected failing test '{test_name}' in output"
            )

        # ════════════════════════════════════════════════════════════
        # Phase 3: Initial stage & promote (full deployment)
        # ════════════════════════════════════════════════════════════

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
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'compute_v1'"
        )
        assert len(rows) == 1, f"Expected compute_v1 cluster, got {rows}"

        # app_v1 cluster should exist (ops views)
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'app_v1'"
        )
        assert len(rows) == 1, f"Expected app_v1 cluster, got {rows}"

        # ingest should NOT be staged (sources/tables are apply-only)
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'ingest_v1'"
        )
        assert len(rows) == 0, f"Expected no ingest_v1 cluster, got {rows}"

        # ── 8. Wait ──────────────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v1", "wait", "v1", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait v1 failed: {result.stderr}"

        # ── 9. Promote ───────────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v1", "promote", "v1", "--no-ready-check"
        )
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
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'ingest'"
        )
        assert len(rows) == 1, f"Expected ingest cluster after promote, got {rows}"

        # ── 10. Log ──────────────────────────────────────────────
        result = run_mz_deploy(c, "basic/v1", "log")
        assert result.returncode == 0, f"log failed: {result.stderr}"
        assert "v1" in result.stderr, f"Expected v1 in log output: {result.stderr}"

        # ════════════════════════════════════════════════════════════
        # Phase 4: Change stable-tier view → scoped staging
        # ════════════════════════════════════════════════════════════

        # ── 11-12. Stage v2 ───────────────────────────────────────
        result = run_mz_deploy(
            c, "basic/v5", "stage", "--deploy-id", "v2", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage v2 failed: {result.stderr}"

        # compute_v2 cluster should exist (core objects use compute)
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name = 'compute_v2'"
        )
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

        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name LIKE '%\\_v2'"
        )
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

        result = run_mz_deploy(
            c, "basic/v5", "promote", "v2", "--no-ready-check"
        )
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

        # ════════════════════════════════════════════════════════════
        # Phase 5: Change downstream-only view → scoped staging
        # ════════════════════════════════════════════════════════════

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

        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name LIKE '%\\_v3'"
        )
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

        result = run_mz_deploy(
            c, "basic/v6", "promote", "v3", "--no-ready-check"
        )
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
        assert rows[0][0] == "default secret", f"Expected 'default secret', got '{rows[0][0]}'"

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
            c, "multi-profile/v1", "wait", "mp_default", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c, "multi-profile/v1", "promote", "mp_default", "--no-ready-check"
        )
        assert result.returncode == 0

        # Verify MV 'summary' has env column = 'production' (variable resolved)
        c.sql("INSERT INTO app.public.my_table VALUES (1, 'test')", database="app")
        rows = c.sql_query("SELECT env FROM app.core.summary", database="app")
        assert len(rows) == 1
        assert rows[0][0] == "production"

        # Verify ambiguous view works (variable pragma + slice syntax)
        rows = c.sql_query(
            "SELECT arr_slice::text, b FROM app.core.ambiguous ORDER BY b",
            database="app",
        )
        assert len(rows) == 3, f"Expected 3 rows from ambiguous view, got {rows}"

        # ── 3. Apply staging profile ──────────────────────
        result = run_mz_deploy(
            c, "multi-profile/v1", "apply", "--profile", "staging"
        )
        assert result.returncode == 0

        # Verify database "app__staging" exists (suffix applied)
        rows = c.sql_query("SELECT name FROM mz_databases WHERE name = 'app__staging'")
        assert len(rows) == 1

        # Verify cluster "compute__staging" exists (suffix applied)
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'compute__staging'")
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
        assert len(rows) == 1, f"Expected 1 comment row for my_secret in app__staging, got {rows}"
        assert rows[0][0] == "staging secret", f"Expected 'staging secret', got '{rows[0][0]}'"

        # Verify staging_config exists (staging-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'staging_config' AND sc.name = 'public' AND db.name = 'app__staging'",
            database="app__staging",
        )
        assert len(rows) == 1, f"Expected staging_config table in app__staging, got {rows}"

        # Verify default_config does NOT exist (default-only table)
        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'default_config' AND sc.name = 'public' AND db.name = 'app__staging'",
            database="app__staging",
        )
        assert len(rows) == 0, f"Expected no default_config table in app__staging, got {rows}"

        # ── 4. Stage + promote staging profile (deploys MV) ──
        result = run_mz_deploy(
            c, "multi-profile/v1", "stage", "--deploy-id", "mp_staging",
            "--allow-dirty", "--profile", "staging"
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c, "multi-profile/v1", "wait", "mp_staging", "--timeout", "300",
            "--allowed-lag", "86400", "--profile", "staging"
        )
        assert result.returncode == 0

        result = run_mz_deploy(
            c, "multi-profile/v1", "promote", "mp_staging", "--no-ready-check",
            "--profile", "staging"
        )
        assert result.returncode == 0

        # Verify MV 'summary' in staging has env column = 'staging'
        c.sql(
            "INSERT INTO app__staging.public.my_table VALUES (1, 'test')",
            database="app__staging",
        )
        rows = c.sql_query(
            "SELECT env FROM app__staging.core.summary",
            database="app__staging",
        )
        assert len(rows) == 1
        assert rows[0][0] == "staging"

        # Verify ambiguous view works in staging
        rows = c.sql_query(
            "SELECT arr_slice::text, b FROM app__staging.core.ambiguous ORDER BY b",
            database="app__staging",
        )
        assert len(rows) == 3, f"Expected 3 rows from ambiguous view in staging, got {rows}"

        # ── 5. Idempotent re-apply for both profiles ──────
        # Default
        result = run_mz_deploy(
            c, "multi-profile/v1", "apply", "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        assert count_actions(dry_run["phases"], "created") == 0

        # Staging
        result = run_mz_deploy(
            c, "multi-profile/v1", "apply", "--profile", "staging",
            "--dry-run", "--output", "json"
        )
        dry_run = parse_dry_run_json(result)
        assert count_actions(dry_run["phases"], "created") == 0

    with c.test_case("mz-deploy-undefined-var"):
        result = run_mz_deploy(c, "undefined-var/v1", "compile", check=False)
        assert result.returncode != 0, (
            f"Expected compile to fail for undefined variable, got rc={result.returncode}"
        )
        combined = result.stdout + result.stderr
        assert "undefined_var" in combined, (
            f"Expected error to mention 'undefined_var', got: {combined}"
        )
