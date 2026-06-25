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
import time

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda

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
    # Kafka broker for the sinks workflow. Only started by workflows that
    # exercise sinks; the others never bring it up.
    Redpanda(),
    # mz-deploy runs as a prebuilt mzbuild image (see src/mz-deploy/ci) rather
    # than a host `cargo build`, so CI doesn't recompile it on every run. The
    # projects directory is mounted at /projects; the binary reaches the
    # `materialized` service over the compose network (see create_profiles).
    Service(
        name="mz-deploy",
        config={
            "mzbuild": "mz-deploy",
            "volumes": [
                "./"
                + os.path.relpath(PROJECTS_DIR, loader.composition_path)
                + ":/projects",
            ],
        },
    ),
]


def create_profiles() -> None:
    """Write profiles.toml into the projects directory, pointing at the
    mzcompose Materialized instance.  Every project under projects/ can
    then use ``--profiles-dir <PROJECTS_DIR>``.

    The mz-deploy container runs on the compose network, so profiles connect
    to the ``materialized`` service by its network hostname and internal
    ports (6875 for users, 6877 for ``mz_system``) rather than the host-mapped
    ports.

    Each profile sets ``sslmode = "disable"``: the mzcompose Materialized
    instance serves plaintext pgwire, but mz-deploy defaults an unset sslmode
    to ``require`` for any non-loopback host (only loopback defaults to
    ``prefer``). Without the override, connecting to the ``materialized``
    hostname would force a TLS handshake that the server can't satisfy.

    Every profile sets ``enable_session_rbac_checks = true`` via the
    libpq-style ``options`` map. Materialize's compiled session default is
    ``false``, and ``mz-deploy setup``'s ``is_rbac_enabled`` check ANDs the
    global and session flags — without this override, setup silently skips
    the role/grant phase in the local mzcompose environment and downstream
    GRANTs fail with "unknown role 'materialize_deployer'". Production
    deployments have the session flag flipped at the server level."""
    opts = '\nsslmode = "disable"\noptions = { enable_session_rbac_checks = "true" }'
    # `setup` issues GRANT ... ON SYSTEM, which only mz_system can perform via
    # the internal port (6877); everything else uses the user port (6875).
    with open(PROJECTS_DIR / "profiles.toml", "w") as f:
        f.write(
            f'[admin]\nhost = "materialized"\nport = 6877\nusername = "mz_system"{opts}\n\n'
            f'[default]\nhost = "materialized"\nport = 6875\nusername = "deploy_user"{opts}\n\n'
            f'[staging]\nhost = "materialized"\nport = 6875\nusername = "deploy_user"{opts}\n\n'
            f'[dev]\nhost = "materialized"\nport = 6875\nusername = "dev_user"{opts}\n\n'
            f'[monitor]\nhost = "materialized"\nport = 6875\nusername = "monitor_user"{opts}\n'
        )


def run_mz_deploy(
    c: Composition,
    project_name: str,
    *args: str,
    check: bool = True,
    set_default_profile: bool = True,
    env_extra: dict[str, str] = {},
) -> subprocess.CompletedProcess[str]:
    create_profiles()
    project_dir = PROJECTS_DIR / project_name

    # Seed the per-project default-profile pointer (equivalent of
    # `mz-deploy profile set default`). Tests that want a non-default profile
    # still pass `--profile <name>` in `args`, which takes precedence over
    # the file. Tests exercising the no-profile code path pass
    # `set_default_profile=False` and ensure `.mzprofile` is absent.
    mzprofile = project_dir / ".mzprofile"
    if set_default_profile and not mzprofile.exists():
        mzprofile.write_text("default\n")

    # The projects directory is mounted at /projects inside the container and
    # the image's entrypoint is the binary itself, so these arguments form the
    # command. `rm=True` discards the container after each invocation.
    cmd = ["-d", f"/projects/{project_name}", "--profiles-dir", "/projects", *args]
    result = c.run(
        "mz-deploy",
        *cmd,
        capture=True,
        capture_stderr=True,
        check=False,
        rm=True,
        env_extra=env_extra,
    )
    if result.returncode != 0 and check:
        print(f"mz-deploy stdout: {result.stdout}", file=sys.stderr)
        print(f"mz-deploy stderr: {result.stderr}", file=sys.stderr)
        raise subprocess.CalledProcessError(
            result.returncode, ["mz-deploy", *cmd], result.stdout, result.stderr
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
    """Start services and initialize with roles.

    Shared by all workflows. Runs ``setup`` as superuser (admin profile)
    to create the ``materialize_*`` roles, then creates three users with
    appropriate role grants and system privileges. The mz-deploy binary is
    supplied by its prebuilt mzbuild image (see src/mz-deploy/ci)."""
    c.down(destroy_volumes=True)
    c.up("postgres", "materialized")

    # On PRs, mkpipeline drops this job's dependency on the image build so it
    # starts immediately while the build runs concurrently, signalled by
    # CI_WAITING_FOR_BUILD (see remove_dependencies_on_prs). `c.run` issues a
    # plain pull with no retry, so the first mz-deploy invocation would fail if
    # the image hasn't been pushed yet. Pull it up front, polling the build the
    # same way `c.up` does for the other services. Only needed in that CI
    # window; locally the image is loaded directly and never pulled.
    if build := os.getenv("CI_WAITING_FOR_BUILD"):
        c.invoke(
            "pull", "mz-deploy", max_tries=300, build=build, stdin=subprocess.DEVNULL
        )

    # Ensure profiles exist before first run_mz_deploy call
    create_profiles()

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
    """Run every other workflow as a sub-case so the default workflow
    exercises the full suite."""
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_basic(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Core apply / stage / promote / abort lifecycle over the ``basic/*``
    projects.

    A single sequential, stateful chain sharing one ``setup_base``: each test
    case builds on the catalog state left by the previous one, so the cases
    are not independently runnable."""
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

        rows = c.sql_query(
            "SELECT grantee.name, priv.privilege_type FROM ("
            "  SELECT mz_internal.mz_aclexplode(t.privileges).* "
            "  FROM mz_tables t "
            "  JOIN mz_schemas sc ON t.schema_id = sc.id "
            "  WHERE t.name = 'products' AND sc.name = 'ingest'"
            ") priv "
            "JOIN mz_roles grantee ON priv.grantee = grantee.id "
            "WHERE grantee.name = 'materialize' AND priv.privilege_type = 'SELECT'",
            database="app",
        )
        assert (
            len(rows) == 1
        ), f"Expected SELECT grant on products TO materialize, got {rows}"

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
        # `--no-docker` runs the tests against the mzcompose Materialized
        # service via the profile connection; mz-deploy's default test path
        # spins up its own Docker container, which isn't available inside the
        # mz-deploy container.
        result = run_mz_deploy(c, "basic/v1", "test", "--no-docker", check=False)
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


def workflow_multi_profile(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Per-profile name suffixing, variable overrides, and the apply / stage /
    promote lifecycle across the ``default`` and ``staging`` profiles."""
    setup_base(c)

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


def workflow_profiles(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Profile- and variable-resolution edge cases: which commands require an
    active profile, and how unresolved variables surface at compile time."""
    setup_base(c)

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


def workflow_reserved_words(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Reserved-word object names must work end to end.

    Regression for the bug where an object whose name is a SQL reserved word
    (e.g. `table`) was unusable:

    - **Path matching** rejected `CREATE TABLE "table"` in `table.sql` with
      `ObjectNameMismatch` (the statement's re-quoted `"table"` never matched
      the unquoted file stem).
    - A **declared external dependency** `db.schema."table"` never resolved,
      because the quoted form didn't line up with the catalog's unquoted name.

    Exercises both via `apply` (compiles the project — running path-matching on
    `table.sql` — and creates the reserved-word table) and `lock` (resolves the
    reserved-word external dependency against the catalog).
    """
    setup_base(c)

    project_dir = PROJECTS_DIR / "reserved-words" / "v1"
    types_lock = project_dir / "types.lock"
    if types_lock.exists():
        types_lock.unlink()

    c.sql(
        'CREATE TABLE materialize.public."select" (id int)',
        user="mz_system",
        port=6877,
    )

    with c.test_case("reserved-word-external-dependency-lock"):
        result = run_mz_deploy(c, "reserved-words/v1", "lock")
        assert result.returncode == 0, f"lock failed: {result.stderr}"
        assert types_lock.exists(), f"expected {types_lock} to be created"
        contents = types_lock.read_text()
        assert (
            "select" in contents
        ), f"expected reserved-word dependency in types.lock; got:\n{contents}"

    with c.test_case("reserved-word-object-apply"):
        result = run_mz_deploy(c, "reserved-words/v1", "apply")
        assert result.returncode == 0, f"apply failed: {result.stderr}"

        rows = c.sql_query(
            "SELECT t.name FROM mz_tables t "
            "JOIN mz_schemas sc ON t.schema_id = sc.id "
            "JOIN mz_databases db ON sc.database_id = db.id "
            "WHERE t.name = 'table' AND sc.name = 'public' AND db.name = 'app'",
            database="app",
        )
        assert len(rows) == 1, f"expected reserved-word table 'table', got {rows}"


def workflow_index_on_storage(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Indexes on tables and sources are rejected at compile time.

    Compute objects (views, materialized views) can carry indexes; storage
    objects (tables, sources) cannot. `compile` must fail with a clean error
    that points the user at indexing a view instead.
    """
    setup_base(c)

    result = run_mz_deploy(c, "index-on-storage/v1", "compile", check=False)
    assert (
        result.returncode != 0
    ), f"compile should reject an index on a table, got rc=0: {result.stdout}"
    combined = result.stdout + result.stderr
    assert (
        "is not supported on table" in combined
    ), f"expected index-on-table error, got:\n{combined}"
    assert (
        "create a view" in combined
    ), f"expected hint to suggest indexing a view, got:\n{combined}"


def workflow_promote_resume(c: Composition, parser: WorkflowArgumentParser) -> None:
    """`promote` is crash-safe and resumable.

    A crash mid-promote leaves a marker state in `_mz_deploy`; re-running
    `promote` resumes from it to a complete promotion, with no data loss. This
    exercises the `ApplyState` PreSwap and PostSwap recovery paths end to end,
    using deterministic crash injection (`MZ_DEPLOY_FAIL_AT`).
    """
    setup_base(c)

    def marker(deploy_id: str) -> str | None:
        # The persisted apply-state lives as a comment on the `apply_<id>_pre`
        # marker schema in `_mz_deploy`.
        rows = c.sql_query(
            "SELECT c.comment FROM mz_catalog.mz_schemas s "
            "JOIN mz_catalog.mz_databases d ON s.database_id = d.id "
            "LEFT JOIN mz_internal.mz_comments c ON s.id = c.id "
            f"WHERE s.name = 'apply_{deploy_id}_pre' AND d.name = '_mz_deploy'",
            user="mz_system",
            port=6877,
        )
        return rows[0][0] if rows else None

    def promoted_at(deploy_id: str):
        rows = c.sql_query(
            "SELECT promoted_at FROM _mz_deploy.tables.deployments "
            f"WHERE deploy_id = '{deploy_id}'",
            user="mz_system",
            port=6877,
        )
        return rows[0][0] if rows else None

    def orders_exists() -> bool:
        # The source-backed table is apply-only — it must survive every crash.
        rows = c.sql_query(
            "SELECT 1 FROM mz_tables t "
            "JOIN mz_schemas s ON t.schema_id = s.id "
            "JOIN mz_databases d ON s.database_id = d.id "
            "WHERE t.name = 'orders' AND s.name = 'ingest' AND d.name = 'app'",
            database="app",
        )
        return len(rows) == 1

    assert run_mz_deploy(c, "basic/v1", "apply").returncode == 0
    assert orders_exists()

    def stage_and_wait(deploy_id: str) -> None:
        # `--redeploy-all` forces a full stage even once a prior deploy is
        # promoted, so each sub-case has something to promote.
        assert (
            run_mz_deploy(
                c,
                "basic/v1",
                "stage",
                "--deploy-id",
                deploy_id,
                "--allow-dirty",
                "--redeploy-all",
            ).returncode
            == 0
        )
        assert (
            run_mz_deploy(
                c,
                "basic/v1",
                "wait",
                deploy_id,
                "--timeout",
                "300",
                "--allowed-lag",
                "86400",
            ).returncode
            == 0
        )

    with c.test_case("promote-resume-preswap"):
        stage_and_wait("rp1")
        # Crash after markers are written but before the swap commits.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "promote",
            "rp1",
            "--no-ready-check",
            env_extra={"MZ_DEPLOY_FAIL_AT": "after-markers"},
            check=False,
        )
        assert r.returncode != 0, "crash injection should make promote exit non-zero"
        assert (
            marker("rp1") == "swapped=false"
        ), f"expected PreSwap marker, got {marker('rp1')!r}"
        assert promoted_at("rp1") is None, "must not be promoted after a pre-swap crash"
        # Resume: a plain re-run finishes the promotion.
        assert (
            run_mz_deploy(
                c, "basic/v1", "promote", "rp1", "--no-ready-check"
            ).returncode
            == 0
        )
        assert marker("rp1") is None, "markers should be cleaned up after resume"
        assert promoted_at("rp1") is not None, "should be promoted after resume"
        assert orders_exists(), "no data loss across crash + resume"

    with c.test_case("promote-resume-postswap"):
        stage_and_wait("rp2")
        # Crash after the swap commits but before cleanup.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "promote",
            "rp2",
            "--no-ready-check",
            env_extra={"MZ_DEPLOY_FAIL_AT": "after-swap"},
            check=False,
        )
        assert r.returncode != 0
        assert (
            marker("rp2") == "swapped=true"
        ), f"expected PostSwap marker, got {marker('rp2')!r}"
        assert promoted_at("rp2") is None, "post-swap work must not have run yet"
        # Resume: re-run finishes post-swap work + cleanup (and must not swap again).
        assert (
            run_mz_deploy(
                c, "basic/v1", "promote", "rp2", "--no-ready-check"
            ).returncode
            == 0
        )
        assert marker("rp2") is None
        assert promoted_at("rp2") is not None
        assert orders_exists(), "no data loss across crash + resume"

    with c.test_case("promote-resume-postcleanup"):
        stage_and_wait("rp3")
        # Crash after post-swap work (promotion recorded) but before the apply
        # markers are cleaned up.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "promote",
            "rp3",
            "--no-ready-check",
            env_extra={"MZ_DEPLOY_FAIL_AT": "after-post-swap"},
            check=False,
        )
        assert r.returncode != 0
        assert (
            marker("rp3") == "swapped=true"
        ), f"expected PostSwap marker, got {marker('rp3')!r}"
        assert (
            promoted_at("rp3") is not None
        ), "post-swap work should have recorded the promotion before the crash"
        # Resume: re-running promote finishes the leftover cleanup even though the
        # deployment is already promoted (it must not error "already promoted").
        assert (
            run_mz_deploy(
                c, "basic/v1", "promote", "rp3", "--no-ready-check"
            ).returncode
            == 0
        ), "promote must resume cleanup of an already-promoted-but-uncleaned deploy"
        assert marker("rp3") is None, "markers should be cleaned up on resume"
        assert promoted_at("rp3") is not None
        assert orders_exists(), "no data loss across crash + resume"


def workflow_redeploy_flags(c: Composition, parser: WorkflowArgumentParser) -> None:
    """`stage --redeploy-schema` / `--redeploy-all` force a redeploy.

    `--redeploy-schema` marks a schema — and its downstream dependents — dirty
    even when nothing changed; `--redeploy-all` redeploys every schema. The two
    flags are mutually exclusive.
    """
    setup_base(c)

    # Establish a promoted production deployment so later stages run
    # incrementally (otherwise every stage is a full deploy and the flags are
    # no-ops).
    assert run_mz_deploy(c, "basic/v1", "apply").returncode == 0
    assert (
        run_mz_deploy(
            c, "basic/v1", "stage", "--deploy-id", "rf", "--allow-dirty"
        ).returncode
        == 0
    )
    assert (
        run_mz_deploy(
            c, "basic/v1", "wait", "rf", "--timeout", "300", "--allowed-lag", "86400"
        ).returncode
        == 0
    )
    assert (
        run_mz_deploy(c, "basic/v1", "promote", "rf", "--no-ready-check").returncode
        == 0
    )

    def plan_object_schemas(*args: str) -> set[str]:
        # Dry-run never creates the deployment, so the deploy-id is reusable.
        result = run_mz_deploy(
            c,
            "basic/v1",
            "stage",
            "--deploy-id",
            "rf-dry",
            "--allow-dirty",
            "--dry-run",
            "--output",
            "json",
            *args,
        )
        assert result.returncode == 0, f"stage dry-run failed: {result.stderr}"
        plan = json.loads(result.stdout)
        # A schema is "in the plan" if any of its objects are staged in any
        # bucket — regular objects, replacement MVs, or sinks.
        return {
            o["schema"]
            for bucket in ("objects", "replacement_mvs", "sinks")
            for o in plan[bucket]
        }

    with c.test_case("redeploy-no-changes-is-noop"):
        # No changes and no force flag → nothing to stage.
        result = run_mz_deploy(
            c,
            "basic/v1",
            "stage",
            "--deploy-id",
            "rf-noop",
            "--allow-dirty",
            "--dry-run",
            "--output",
            "json",
        )
        assert result.returncode == 0, result.stderr
        assert (
            "No changes detected" in result.stderr
        ), f"expected no-change message, got: {result.stderr}"

    with c.test_case("redeploy-schema-forces-downstream-only"):
        # `ops` depends on `core`. Forcing `ops` redeploys ops but must not pull
        # in the upstream `core` schema.
        schemas = plan_object_schemas("--redeploy-schema", "app.ops")
        assert "ops" in schemas, f"ops should be forced dirty, got {schemas}"
        assert (
            "core" not in schemas
        ), f"upstream core must not be pulled in by forcing ops, got {schemas}"

    with c.test_case("redeploy-schema-comma-list"):
        schemas = plan_object_schemas("--redeploy-schema", "app.core,app.ops")
        assert {"core", "ops"} <= schemas, f"both schemas expected, got {schemas}"

    with c.test_case("redeploy-all-forces-everything"):
        schemas = plan_object_schemas("--redeploy-all")
        assert {"core", "ops"} <= schemas, f"all schemas expected, got {schemas}"

    with c.test_case("redeploy-flags-validation"):
        # Mutually exclusive.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "stage",
            "--deploy-id",
            "rf-x",
            "--allow-dirty",
            "--redeploy-schema",
            "app.core",
            "--redeploy-all",
            check=False,
        )
        assert (
            r.returncode != 0
        ), "conflicting --redeploy-schema/--redeploy-all should fail"
        # Unqualified schema.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "stage",
            "--deploy-id",
            "rf-x",
            "--allow-dirty",
            "--redeploy-schema",
            "core",
            check=False,
        )
        assert r.returncode != 0, "unqualified schema should fail"
        # Unknown schema.
        r = run_mz_deploy(
            c,
            "basic/v1",
            "stage",
            "--deploy-id",
            "rf-x",
            "--allow-dirty",
            "--redeploy-schema",
            "app.bogus",
            check=False,
        )
        assert r.returncode != 0, "unknown schema should fail"

    def schema_exists(name: str) -> bool:
        return bool(
            c.sql_query(
                "SELECT 1 FROM mz_schemas s JOIN mz_databases d ON s.database_id = d.id "
                f"WHERE s.name = '{name}' AND d.name = 'app'",
                database="app",
            )
        )

    def core_mv_count() -> int:
        return len(
            c.sql_query(
                "SELECT 1 FROM mz_materialized_views mv "
                "JOIN mz_schemas s ON mv.schema_id = s.id "
                "JOIN mz_databases d ON s.database_id = d.id "
                "WHERE d.name = 'app' AND s.name = 'core' "
                "AND mv.name IN ('order_summary', 'user_activity')",
                database="app",
            )
        )

    with c.test_case("redeploy-all-promotes-stable-schema"):
        assert core_mv_count() == 2, "precondition: production core MVs exist"
        assert (
            run_mz_deploy(
                c,
                "basic/v1",
                "stage",
                "--deploy-id",
                "rfa",
                "--allow-dirty",
                "--redeploy-all",
            ).returncode
            == 0
        )
        assert schema_exists("core_rfa"), "stage should create staging schema core_rfa"
        assert (
            run_mz_deploy(
                c,
                "basic/v1",
                "wait",
                "rfa",
                "--timeout",
                "300",
                "--allowed-lag",
                "86400",
            ).returncode
            == 0
        )
        assert (
            run_mz_deploy(
                c, "basic/v1", "promote", "rfa", "--no-ready-check"
            ).returncode
            == 0
        )

        assert (
            core_mv_count() == 2
        ), "DATA LOSS: --redeploy-all promote dropped production core MVs"
        assert not schema_exists(
            "core_rfa"
        ), "leaked orphan staging schema app.core_rfa; core not redeployed"


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


def get_object_id(c: Composition, catalog: str, schema: str, name: str) -> str:
    """Look up a catalog ID by schema-qualified name in the app database.

    ``catalog`` is the mz_catalog relation to search (e.g. ``mz_sinks``,
    ``mz_materialized_views``, ``mz_views``)."""
    rows = c.sql_query(
        f"SELECT o.id FROM {catalog} o "
        "JOIN mz_schemas sc ON o.schema_id = sc.id "
        f"WHERE o.name = '{name}' AND sc.name = '{schema}'",
        database="app",
    )
    assert len(rows) == 1, f"Expected one {schema}.{name} in {catalog}, got {rows}"
    return rows[0][0]


def wait_for_sink_running(
    c: Composition, sink_name: str, timeout_secs: int = 120
) -> None:
    """Poll mz_sink_statuses until the sink is running (proving it reached
    the broker), failing fast on stalled/failed."""
    deadline = time.time() + timeout_secs
    status = None
    while time.time() < deadline:
        rows = c.sql_query(
            "SELECT status FROM mz_internal.mz_sink_statuses "
            f"WHERE name = '{sink_name}'",
            database="app",
        )
        status = rows[0][0] if rows else None
        if status == "running":
            return
        assert status not in ("stalled", "failed"), f"sink status: {status}"
        time.sleep(2)
    raise AssertionError(f"sink never reached 'running', last status: {status}")


def workflow_sinks(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Exercise the sink deployment lifecycle.

    Sinks are deferred during ``stage`` (they must not start producing
    until promotion) and created by ``promote`` after the schema swap.
    When a later deployment swaps a schema containing a sink's upstream
    materialized view, ``promote`` repoints the sink to the replacement
    object via ``ALTER SINK ... SET FROM`` instead of recreating it.

    Covers:
    - stage + promote of a new project creates the deferred sink.
    - updating the upstream MV and promoting repoints the sink in place
      (same sink ID, new upstream MV ID).
    """
    setup_base(c)
    c.up("redpanda")

    def sink_id() -> str:
        return get_object_id(c, "mz_sinks", "egress", "event_sink")

    def mv_id() -> str:
        return get_object_id(c, "mz_materialized_views", "core", "event_totals")

    # ── 1. Apply infrastructure ──────────────────────────────────────────
    # Creates clusters, the app database, the Kafka connection, and the
    # events table. Sinks and MVs are deploy-time objects and are not
    # touched by apply.
    result = run_mz_deploy(c, "sinks/v1", "apply")
    assert result.returncode == 0, f"apply failed: {result.stderr}"

    c.sql(
        "INSERT INTO app.public.events VALUES "
        "(1, 'signup', 10.0), (2, 'signup', 5.0), (3, 'purchase', NULL)",
        database="app",
        user="deploy_user",
    )

    with c.test_case("sink-stage-promote"):
        # ── 2. Stage v1: sink is deferred ────────────────────────────────
        result = run_mz_deploy(
            c, "sinks/v1", "stage", "--deploy-id", "s1", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage s1 failed: {result.stderr}"

        # The MV's schema and cluster are staged.
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name = 'core_s1'", database="app"
        )
        assert len(rows) == 1, f"Expected core_s1 schema, got {rows}"
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'compute_s1'")
        assert len(rows) == 1, f"Expected compute_s1 cluster, got {rows}"

        # The sink is deferred: not created during stage, and its cluster
        # is not cloned (sinks don't propagate cluster dirtiness).
        rows = c.sql_query(
            "SELECT name FROM mz_sinks WHERE name = 'event_sink'", database="app"
        )
        assert len(rows) == 0, f"Sink must not exist before promote, got {rows}"
        rows = c.sql_query("SELECT name FROM mz_clusters WHERE name = 'egress_s1'")
        assert len(rows) == 0, f"Expected no egress_s1 cluster, got {rows}"

        result = run_mz_deploy(
            c, "sinks/v1", "wait", "s1", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait s1 failed: {result.stderr}"

        # ── 3. Promote v1: deferred sink is created ──────────────────────
        result = run_mz_deploy(
            c,
            "sinks/v1",
            "promote",
            "s1",
            "--no-ready-check",
            "--dry-run",
            "--output",
            "json",
        )
        plan = json.loads(result.stdout)
        assert len(plan["sinks_to_create"]) == 1, f"Expected 1 sink to create: {plan}"
        assert plan["sinks_to_create"][0]["object"] == "event_sink", f"got {plan}"
        assert len(plan["sinks_to_repoint"]) == 0, f"Expected no repoints: {plan}"

        result = run_mz_deploy(c, "sinks/v1", "promote", "s1", "--no-ready-check")
        assert result.returncode == 0, f"promote s1 failed: {result.stderr}"

        # Sink exists in the egress schema on the egress cluster.
        rows = c.sql_query(
            "SELECT s.name FROM mz_sinks s "
            "JOIN mz_schemas sc ON s.schema_id = sc.id "
            "JOIN mz_clusters cl ON s.cluster_id = cl.id "
            "WHERE s.name = 'event_sink' AND sc.name = 'egress' "
            "AND cl.name = 'egress'",
            database="app",
        )
        assert len(rows) == 1, f"Expected event_sink on egress cluster, got {rows}"

        # And it actually connects to the broker and starts producing.
        wait_for_sink_running(c, "event_sink")

    with c.test_case("sink-repoint-on-mv-update"):
        sink_id_before = sink_id()
        mv_id_before = mv_id()

        # ── 4. Stage v2 (MV definition changed) ──────────────────────────
        result = run_mz_deploy(
            c, "sinks/v2", "stage", "--deploy-id", "s2", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage s2 failed: {result.stderr}"

        result = run_mz_deploy(
            c, "sinks/v2", "wait", "s2", "--timeout", "300", "--allowed-lag", "86400"
        )
        assert result.returncode == 0, f"wait s2 failed: {result.stderr}"

        # ── 5. Promote v2: sink is repointed, not recreated ──────────────
        result = run_mz_deploy(
            c,
            "sinks/v2",
            "promote",
            "s2",
            "--no-ready-check",
            "--dry-run",
            "--output",
            "json",
        )
        plan = json.loads(result.stdout)
        # The sink still appears under sinks_to_create: stage records every
        # project sink as a pending statement and promote skips the ones
        # that already exist. The ID checks below prove it is not recreated.
        repoints = plan["sinks_to_repoint"]
        assert len(repoints) == 1, f"Expected 1 sink to repoint: {plan}"
        assert repoints[0]["sink_name"] == "event_sink", f"got {plan}"
        assert repoints[0]["dependency_name"] == "event_totals", f"got {plan}"

        result = run_mz_deploy(c, "sinks/v2", "promote", "s2", "--no-ready-check")
        assert result.returncode == 0, f"promote s2 failed: {result.stderr}"

        # The MV was replaced by the swap; the sink survived in place.
        assert mv_id() != mv_id_before, "MV should have a new ID after the swap"
        assert sink_id() == sink_id_before, "Sink must be repointed, not recreated"

        # The sink now depends on the replacement MV.
        rows = c.sql_query(
            "SELECT mv.id FROM mz_sinks s "
            "JOIN mz_internal.mz_object_dependencies d ON s.id = d.object_id "
            "JOIN mz_materialized_views mv ON d.referenced_object_id = mv.id "
            "WHERE s.name = 'event_sink'",
            database="app",
        )
        assert [r[0] for r in rows] == [
            mv_id()
        ], f"Sink should depend on the new MV {mv_id()}, got {rows}"

        wait_for_sink_running(c, "event_sink")


def workflow_replacement_mvs(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Exercise ``SET api = stable`` replacement semantics.

    A changed materialized view in a stable-API schema is updated in place
    via ``ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`` instead of a
    schema swap, so its catalog identity is preserved and downstream
    consumers — views and sinks — are never redeployed or repointed.
    """
    setup_base(c)
    c.up("redpanda")

    def mv_id() -> str:
        return get_object_id(c, "mz_materialized_views", "stable", "event_totals")

    def view_id() -> str:
        return get_object_id(c, "mz_views", "ops", "event_report")

    def sink_id() -> str:
        return get_object_id(c, "mz_sinks", "egress", "event_sink")

    def report_categories() -> list[str]:
        rows = c.sql_query(
            "SELECT category FROM app.ops.event_report ORDER BY category",
            database="app",
            user="deploy_user",
        )
        return [r[0] for r in rows]

    result = run_mz_deploy(c, "replacement/v1", "apply")
    assert result.returncode == 0, f"apply failed: {result.stderr}"

    c.sql(
        "INSERT INTO app.public.events VALUES "
        "(1, 'signup', 10.0), (2, 'signup', 5.0), (3, 'purchase', NULL)",
        database="app",
        user="deploy_user",
    )

    # ── 1. Initial deploy: everything is new, deployed via normal swap ────
    result = run_mz_deploy(
        c, "replacement/v1", "stage", "--deploy-id", "r1", "--allow-dirty"
    )
    assert result.returncode == 0, f"stage r1 failed: {result.stderr}"
    result = run_mz_deploy(
        c, "replacement/v1", "wait", "r1", "--timeout", "300", "--allowed-lag", "86400"
    )
    assert result.returncode == 0, f"wait r1 failed: {result.stderr}"
    result = run_mz_deploy(c, "replacement/v1", "promote", "r1", "--no-ready-check")
    assert result.returncode == 0, f"promote r1 failed: {result.stderr}"

    assert report_categories() == ["purchase", "signup"]
    wait_for_sink_running(c, "event_sink")

    with c.test_case("replacement-mv-updated-in-place"):
        mv_id_before = mv_id()
        view_id_before = view_id()
        sink_id_before = sink_id()

        # ── 2. Stage v2: only the stable MV changed ──────────────────────
        result = run_mz_deploy(
            c, "replacement/v2", "stage", "--deploy-id", "r2", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage r2 failed: {result.stderr}"

        # Replacement MVs do not propagate dirtiness: the downstream ops
        # schema is not staged.
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name = 'ops_r2'", database="app"
        )
        assert len(rows) == 0, f"ops must not be staged, got {rows}"

        result = run_mz_deploy(
            c,
            "replacement/v2",
            "wait",
            "r2",
            "--timeout",
            "300",
            "--allowed-lag",
            "86400",
        )
        assert result.returncode == 0, f"wait r2 failed: {result.stderr}"

        # ── 3. Promote v2: APPLY REPLACEMENT, no swap, no repoint ────────
        result = run_mz_deploy(
            c,
            "replacement/v2",
            "promote",
            "r2",
            "--no-ready-check",
            "--dry-run",
            "--output",
            "json",
        )
        plan = json.loads(result.stdout)
        replacements = plan["replacement_mvs"]
        assert len(replacements) == 1, f"Expected 1 replacement MV: {plan}"
        assert replacements[0]["target_schema"] == "stable", f"got {plan}"
        assert replacements[0]["target_name"] == "event_totals", f"got {plan}"
        assert plan["schema_swaps"] == [], f"Expected no schema swaps: {plan}"
        assert plan["sinks_to_repoint"] == [], f"Expected no repoints: {plan}"

        result = run_mz_deploy(c, "replacement/v2", "promote", "r2", "--no-ready-check")
        assert result.returncode == 0, f"promote r2 failed: {result.stderr}"

        # APPLY REPLACEMENT moves the target MV onto the replacement's
        # catalog ID while keeping its name and versioned collections, so
        # consumers stay valid without being recreated: the downstream
        # view and sink keep their IDs.
        assert mv_id() != mv_id_before, "MV adopts the replacement's catalog ID"
        assert view_id() == view_id_before, "Downstream view must not be redeployed"
        assert sink_id() == sink_id_before, "Downstream sink must not be recreated"

        # The replacement definition is live: the NULL-amount purchase row
        # is now filtered out.
        assert report_categories() == ["signup"]
        wait_for_sink_running(c, "event_sink")


def workflow_concurrent_deploys(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Exercise concurrent non-overlapping deployments.

    Two staging deployments that touch disjoint schemas and clusters run
    side by side, and both promote independently without tripping conflict
    detection (no ``--force`` anywhere).
    """
    setup_base(c)

    def mv_count(schema: str, name: str) -> int:
        rows = c.sql_query(
            f"SELECT n FROM app.{schema}.{name}",
            database="app",
            user="deploy_user",
        )
        return int(rows[0][0])

    result = run_mz_deploy(c, "concurrent/v1", "apply")
    assert result.returncode == 0, f"apply failed: {result.stderr}"

    c.sql(
        "INSERT INTO app.public.events VALUES "
        "(1, 'signup', 10.0), (2, 'signup', NULL), "
        "(3, 'purchase', 7.0), (4, 'purchase', NULL)",
        database="app",
        user="deploy_user",
    )

    # ── 1. Baseline: deploy v1 so subsequent stages diff against it ──────
    result = run_mz_deploy(
        c, "concurrent/v1", "stage", "--deploy-id", "c0", "--allow-dirty"
    )
    assert result.returncode == 0, f"stage c0 failed: {result.stderr}"
    result = run_mz_deploy(
        c, "concurrent/v1", "wait", "c0", "--timeout", "300", "--allowed-lag", "86400"
    )
    assert result.returncode == 0, f"wait c0 failed: {result.stderr}"
    result = run_mz_deploy(c, "concurrent/v1", "promote", "c0", "--no-ready-check")
    assert result.returncode == 0, f"promote c0 failed: {result.stderr}"

    assert mv_count("alpha", "signups") == 2
    assert mv_count("beta", "purchases") == 2

    with c.test_case("concurrent-stage-disjoint"):
        # ── 2. Stage both deployments side by side ───────────────────────
        # ca changes only alpha.signups; cb changes only beta.purchases.
        result = run_mz_deploy(
            c, "concurrent/va", "stage", "--deploy-id", "ca", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage ca failed: {result.stderr}"
        result = run_mz_deploy(
            c, "concurrent/vb", "stage", "--deploy-id", "cb", "--allow-dirty"
        )
        assert result.returncode == 0, f"stage cb failed: {result.stderr}"

        # Both staging environments coexist, each confined to its own
        # schema and cluster.
        rows = c.sql_query(
            "SELECT name FROM mz_schemas "
            "WHERE name IN ('alpha_ca', 'beta_cb', 'alpha_cb', 'beta_ca') "
            "ORDER BY name",
            database="app",
        )
        assert [r[0] for r in rows] == [
            "alpha_ca",
            "beta_cb",
        ], f"Each deployment must stage only its own schema, got {rows}"

        rows = c.sql_query(
            "SELECT name FROM mz_clusters "
            "WHERE name IN ('alpha_ca', 'beta_cb', 'alpha_cb', 'beta_ca') "
            "ORDER BY name"
        )
        assert [r[0] for r in rows] == [
            "alpha_ca",
            "beta_cb",
        ], f"Each deployment must stage only its own cluster, got {rows}"

    with c.test_case("concurrent-promote-independently"):
        # ── 3. Promote ca, then cb — no conflicts, no --force ────────────
        result = run_mz_deploy(
            c,
            "concurrent/va",
            "wait",
            "ca",
            "--timeout",
            "300",
            "--allowed-lag",
            "86400",
        )
        assert result.returncode == 0, f"wait ca failed: {result.stderr}"
        result = run_mz_deploy(c, "concurrent/va", "promote", "ca", "--no-ready-check")
        assert result.returncode == 0, f"promote ca failed: {result.stderr}"

        # Alpha is updated; beta production and beta's staging are intact.
        assert mv_count("alpha", "signups") == 1
        assert mv_count("beta", "purchases") == 2

        # cb was staged before ca promoted, but they are disjoint, so
        # conflict detection must let it through.
        result = run_mz_deploy(
            c,
            "concurrent/vb",
            "wait",
            "cb",
            "--timeout",
            "300",
            "--allowed-lag",
            "86400",
        )
        assert result.returncode == 0, f"wait cb failed: {result.stderr}"
        result = run_mz_deploy(c, "concurrent/vb", "promote", "cb", "--no-ready-check")
        assert result.returncode == 0, (
            f"promote cb must succeed without --force on a disjoint "
            f"deployment: {result.stderr}"
        )

        assert mv_count("alpha", "signups") == 1
        assert mv_count("beta", "purchases") == 1

        # No staging debris left behind by either promotion.
        rows = c.sql_query(
            "SELECT name FROM mz_schemas WHERE name LIKE '%\\_ca' OR name LIKE '%\\_cb'",
            database="app",
        )
        assert len(rows) == 0, f"Expected no staging schemas, got {rows}"
        rows = c.sql_query(
            "SELECT name FROM mz_clusters WHERE name LIKE '%\\_ca' OR name LIKE '%\\_cb'"
        )
        assert len(rows) == 0, f"Expected no staging clusters, got {rows}"
