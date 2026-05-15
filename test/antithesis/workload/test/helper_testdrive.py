# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run repository-checked-in testdrive .td files under Antithesis.

The testdrive binary is bundled into the antithesis-workload image via
the multi-stage MZFROM in the Dockerfile. The repo's `test/pg-cdc/*.td`
files are bundled in too (see `pre-image: copy` in mzbuild.yml) under
/opt/testdrive-files/test/pg-cdc/.

The drivers pass repo-relative file paths; this helper:
  1. Reads the bundled file.
  2. Strips the `$ skip-if` block — many checked-in `.td` files start
     with `$ skip-if / SELECT true` to disable themselves pending an
     upstream fix. Antithesis is the upstream-fix proving ground, so we
     un-skip at runtime. The repo file stays untouched; only the
     in-memory copy passed to testdrive is rewritten.
  3. Writes the rewritten file to a tmp path.
  4. Spawns testdrive with the right URLs for the antithesis topology.
  5. Returns (exit_code, stdout, stderr) so the driver can decide
     whether a failure is a real property violation or a transient
     fault-injection artifact.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass

import helper_pg
import helper_pg_upstream

LOG = logging.getLogger("antithesis.helper_testdrive")

# Names that the data-loss workload owns. Anything else in the user-
# visible Materialize catalog is fair game for the pre-run reset — the
# checked-in `test/pg-cdc/*.td` files assume a clean slate and create
# objects like `mz_source`, `pgpass`, `pg`, `storage`, `t1` without
# guarding for IF NOT EXISTS.
#
# The system clusters (`mz_introspection`, `mz_system`, `mz_catalog_server`,
# `mz_probe`) are owned by `mz_system` and rejected by the regular DROP
# CLUSTER, so we filter on owner rather than enumerating names.
_PRESERVE_SOURCES = {"pg_cdc_source", "mysql_cdc_source"}
_PRESERVE_CONNECTIONS = {"antithesis_pg_conn", "antithesis_mysql_conn"}
_PRESERVE_SECRETS = {"antithesis_pg_password", "antithesis_mysql_password"}
_PRESERVE_CLUSTERS = {"antithesis_cluster", "quickstart"}
# Tables under the data-loss workload's source are subsources — DROP TABLE
# on them is rejected unless we also drop the source. The reset filters
# them out by name; they live inside `pg_cdc_source` / `mysql_cdc_source`
# which is preserved.
_PRESERVE_TABLES = {"antithesis_pg_cdc", "antithesis_cdc", "antithesis_cdc_myisam"}

# Pool clusters share a `pool_cluster_*` prefix.
_PRESERVE_CLUSTER_PREFIX = "pool_cluster_"

# Where the workload Dockerfile lands the bundled .td files. The
# pre-image: copy step in mzbuild.yml preserves the repo-relative path
# under this prefix, so callers reference files exactly the way they
# appear in the repo.
TESTDRIVE_FILES_ROOT = "/opt/testdrive-files"
TESTDRIVE_BINARY = "/usr/local/bin/testdrive"

# Defaults matching the antithesis topology. Overridable via env so the
# same helper works under `snouty validate` (local docker-compose) and
# inside Antithesis (same image, same network).
MATERIALIZE_URL = os.environ.get(
    "MZ_MATERIALIZE_URL", "postgres://materialize@materialized:6875"
)
MATERIALIZE_INTERNAL_URL = os.environ.get(
    "MZ_MATERIALIZE_INTERNAL_URL", "postgres://mz_system@materialized:6877"
)
KAFKA_ADDR = os.environ.get("KAFKA_BROKER", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
)

# Patterns we treat as transient fault-injection artifacts rather than
# property violations. Testdrive surfaces these as non-zero exits when an
# Antithesis fault window happens to overlap a sensitive moment in the
# script. The driver layer demotes these to "didn't get a clean signal,
# try again next time" rather than firing `always(False)`.
TRANSIENT_PATTERNS = (
    # Network / process-down windows.
    "connection refused",
    "connection reset",
    "no route to host",
    "broken pipe",
    "could not connect to server",
    "Failed to resolve hostname",
    "Temporary failure in name resolution",
    # Materialize admission control during a restart.
    "is (re)initializing",
    "TooManyRequests",
    # Postgres source visiting its own restart window.
    "terminating connection due to administrator command",
    # testdrive's post-test `--check-catalog-consistency` does a GET to
    # http://materialized:6878/api/catalog/dump.  If Antithesis kills
    # environmentd mid-response, reqwest reports `error sending request
    # ... connection closed before message completed`, wrapped by
    # testdrive as `error: catalog inconsistency: catalog state`.  The
    # catalog isn't inconsistent — the dump request just died.
    "connection closed before message completed",
)

# `$ skip-if` directive removal regex. testdrive parses skip-if as:
#   $ skip-if
#   <SQL that returns >0 rows skips the rest of the file>
# Many checked-in regression tests use `$ skip-if / SELECT true` as a
# manual disable. We strip *only* that specific shape — any skip-if with
# a non-trivial query is left intact (those gate on feature flags and
# should still gate under Antithesis).
_SKIP_IF_TRUE_RE = re.compile(
    r"^\$ skip-if\s*\n\s*SELECT\s+true\s*\n",
    re.MULTILINE | re.IGNORECASE,
)


@dataclass
class TestdriveResult:
    exit_code: int
    stdout: str
    stderr: str

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0

    @property
    def looks_transient(self) -> bool:
        """True if a non-zero exit is plausibly a fault-injection artifact.

        Used by the driver layer to demote transient failures to
        `sometimes(False)` rather than fire a hard `always(False)`. The
        match is intentionally generous — false-positive transients
        sacrifice some signal but avoid false-positive property
        violations, which are far costlier in triage.
        """
        if self.succeeded:
            return False
        blob = (self.stdout + "\n" + self.stderr).lower()
        return any(p.lower() in blob for p in TRANSIENT_PATTERNS)


def _strip_skip_if_true(content: str) -> tuple[str, bool]:
    """Remove `$ skip-if / SELECT true` blocks from a testdrive script.

    Returns (rewritten content, True if a block was removed). Other
    `skip-if` shapes are left intact because they encode meaningful
    feature-flag gates we still want to respect.
    """
    rewritten, n = _SKIP_IF_TRUE_RE.subn("", content)
    return rewritten, n > 0


def run(
    td_file: str,
    *,
    timeout_s: float = 600.0,
    extra_args: list[str] | None = None,
) -> TestdriveResult:
    """Run a bundled testdrive file. `td_file` is repo-relative, e.g.
    "test/pg-cdc/dropped-slot-errors.td".

    The repo file is read, rewritten in memory to strip a skip-if-true
    header if present, and the rewritten copy is fed to testdrive. The
    on-disk repo file is never modified.
    """
    src_path = os.path.join(TESTDRIVE_FILES_ROOT, td_file)
    if not os.path.isfile(src_path):
        raise FileNotFoundError(
            f"testdrive file {src_path!r} not bundled in workload image; "
            f"check mzbuild.yml pre-image:copy 'matching:' glob"
        )

    with open(src_path) as f:
        content = f.read()
    rewritten, stripped = _strip_skip_if_true(content)
    if stripped:
        LOG.info("td %s: stripped `$ skip-if / SELECT true` header", td_file)

    # testdrive treats its positional arg as a *glob pattern* and matches
    # it against files found by `WalkDir::new(".").sort_by_file_name()` —
    # i.e. it walks the process cwd, not the glob's directory. Passing an
    # absolute path like `/tmp/foo.td` therefore matches nothing under
    # `./` and exits with "glob did not match any patterns". We work
    # around this by writing the rewritten file into a dedicated dir and
    # running testdrive with that dir as cwd, passing just the basename.
    tmp_dir = tempfile.mkdtemp(prefix="td-run-", dir="/tmp")
    tmp_name = "input.td"
    tmp_path = os.path.join(tmp_dir, tmp_name)
    with open(tmp_path, "w") as f:
        f.write(rewritten)

    cmd = [
        TESTDRIVE_BINARY,
        f"--materialize-url={MATERIALIZE_URL}",
        f"--materialize-internal-url={MATERIALIZE_INTERNAL_URL}",
        f"--kafka-addr={KAFKA_ADDR}",
        f"--schema-registry-url={SCHEMA_REGISTRY_URL}",
        "--no-reset",
        # Per-statement default timeout; testdrive retries `>` and `!`
        # statements internally until this expires. Long enough to span
        # one full faults-ON+OFF orchestrator cycle (~80s default) plus
        # margin for the statement itself.
        "--default-timeout=180s",
        # Vars referenced by many of the checked-in `test/pg-cdc/*.td`
        # files (and other testdrive suites). Values mirror what
        # `test/pg-cdc/mzcompose.py` passes when it runs the same files
        # in CI — `scale=4,workers=4` for cluster replicas,
        # `scale=4,workers=1` for storage. Without them, files that
        # construct `> CREATE CLUSTER cdc_cluster SIZE
        # '${arg.default-replica-size}'` fail at parse time with
        # "unknown variable: arg.default-replica-size".
        "--var=default-replica-size=scale=4,workers=4",
        "--var=default-storage-size=scale=4,workers=1",
        tmp_name,
    ]
    if extra_args:
        cmd.extend(extra_args)

    LOG.info("running testdrive (cwd=%s): %s", tmp_dir, " ".join(cmd))
    try:
        proc = subprocess.run(
            cmd,
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        result = TestdriveResult(
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )
    except subprocess.TimeoutExpired as exc:
        LOG.warning("testdrive %s timed out after %.0fs", td_file, timeout_s)
        result = TestdriveResult(
            exit_code=124,
            stdout=exc.stdout.decode()
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or ""),
            stderr=(
                exc.stderr.decode()
                if isinstance(exc.stderr, bytes)
                else (exc.stderr or "")
            )
            + f"\n[helper_testdrive] timed out after {timeout_s:.0f}s",
        )
    finally:
        # The whole staging dir holds the rewritten file plus any tmp
        # scratch testdrive wrote; shutil.rmtree clears both. Errors
        # here are best-effort — the OS will reap /tmp eventually.
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)

    LOG.info(
        "testdrive %s exited %d (transient=%s, stdout=%d bytes, stderr=%d bytes)",
        td_file,
        result.exit_code,
        result.looks_transient,
        len(result.stdout),
        len(result.stderr),
    )
    return result


def _quote_ident(name: str) -> str:
    """Double-quote an identifier and escape embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def reset_materialize_user_state() -> None:
    """Drop user-visible Materialize objects not owned by our workload.

    Used as both pre- and post-cleanup around testdrive runs. The
    checked-in `test/pg-cdc/*.td` files don't guard their `CREATE
    SECRET/CONNECTION/SOURCE/CLUSTER/TABLE` statements with `IF NOT
    EXISTS` and don't clean up after themselves, so consecutive runs of
    different files (or two runs of the same file) collide on names
    like `pgpass`, `mz_source`, `storage`, `t1`. This function leaves
    only the data-loss workload's objects in place; anything else is
    dropped CASCADE.

    Best-effort: a missing object or permission denial logs a warning
    but doesn't raise. The next td run's own CREATE will surface any
    real residue.
    """

    # Drop sources first — they CASCADE to their subsource tables,
    # which lets us avoid enumerating td-created tables individually.
    # Filtering happens in Python because mixing the preserve-set into
    # SQL would need parameter-binding for an IN-list, which psycopg
    # doesn't expand transparently.
    try:
        rows = helper_pg.query_retry("SELECT name FROM mz_sources WHERE id LIKE 'u%%'")
        for (name,) in rows:
            if name in _PRESERVE_SOURCES:
                continue
            LOG.info("reset: dropping source %s", name)
            try:
                helper_pg.execute_retry(
                    f"DROP SOURCE IF EXISTS {_quote_ident(name)} CASCADE"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop source %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate sources failed: %s", exc)

    # Drop user-owned clusters (`id LIKE 'u%'`) that aren't ours. The
    # system clusters live under `s%` ids and would be rejected anyway.
    try:
        rows = helper_pg.query_retry("SELECT name FROM mz_clusters WHERE id LIKE 'u%%'")
        for (name,) in rows:
            if name in _PRESERVE_CLUSTERS or name.startswith(_PRESERVE_CLUSTER_PREFIX):
                continue
            LOG.info("reset: dropping cluster %s", name)
            try:
                helper_pg.execute_retry(
                    f"DROP CLUSTER IF EXISTS {_quote_ident(name)} CASCADE"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop cluster %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate clusters failed: %s", exc)

    # Drop user tables that aren't subsources of preserved sources
    # (subsources show up in mz_tables with non-null source_id; those
    # were already dropped by the source CASCADE above).
    try:
        rows = helper_pg.query_retry(
            "SELECT name FROM mz_tables WHERE id LIKE 'u%%' AND source_id IS NULL"
        )
        for (name,) in rows:
            if name in _PRESERVE_TABLES:
                continue
            LOG.info("reset: dropping table %s", name)
            try:
                helper_pg.execute_retry(
                    f"DROP TABLE IF EXISTS {_quote_ident(name)} CASCADE"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop table %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate tables failed: %s", exc)

    # Connections last — sources reference them, so they have to be
    # gone before we can drop a connection without CASCADE-killing a
    # preserved source.
    try:
        rows = helper_pg.query_retry(
            "SELECT name FROM mz_connections WHERE id LIKE 'u%%'"
        )
        for (name,) in rows:
            if name in _PRESERVE_CONNECTIONS:
                continue
            LOG.info("reset: dropping connection %s", name)
            try:
                helper_pg.execute_retry(
                    f"DROP CONNECTION IF EXISTS {_quote_ident(name)} CASCADE"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop connection %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate connections failed: %s", exc)

    # Secrets — last because connections reference them.
    try:
        rows = helper_pg.query_retry("SELECT name FROM mz_secrets WHERE id LIKE 'u%%'")
        for (name,) in rows:
            if name in _PRESERVE_SECRETS:
                continue
            LOG.info("reset: dropping secret %s", name)
            try:
                helper_pg.execute_retry(f"DROP SECRET IF EXISTS {_quote_ident(name)}")
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop secret %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate secrets failed: %s", exc)

    # Drop user-created schemas in the materialize database. Td files like
    # subsource-names.td do `> CREATE SCHEMA a;` against materialize and
    # don't clean up. Filter by database name = 'materialize' so we don't
    # touch other databases (system, etc.), and drop only user schemas
    # (`id LIKE 'u%'`). The default schema `public` is preserved.
    try:
        rows = helper_pg.query_retry(
            "SELECT s.name FROM mz_schemas s "
            "JOIN mz_databases d ON s.database_id = d.id "
            "WHERE s.id LIKE 'u%%' AND d.name = 'materialize'"
        )
        for (name,) in rows:
            if name == "public":
                continue
            LOG.info("reset: dropping schema %s", name)
            try:
                helper_pg.execute_retry(
                    f"DROP SCHEMA IF EXISTS {_quote_ident(name)} CASCADE"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("reset: drop schema %s failed: %s", name, exc)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset: enumerate schemas failed: %s", exc)


def reset_upstream_state() -> None:
    """Wipe the upstream PG's `public` schema and any leftover publication.

    Most `test/pg-cdc/*.td` files start by dropping+recreating `public`
    themselves, but doing it here too defends against a previous run
    that crashed mid-script and left objects behind. The data-loss
    workload lives in `antithesis_pg_cdc`, which we never touch.
    """
    # Drop non-data-loss publications. testdrive scripts use names
    # like `mz_source`, `mz_source_extra`, etc; enumerating from the
    # catalog is more reliable than a hardcoded list. The data-loss
    # workload's `antithesis_pub` is preserved so `parallel_driver_pg_cdc`
    # keeps working.
    try:
        rows = helper_pg_upstream.query("SELECT pubname FROM pg_publication")
        for (pubname,) in rows:
            if pubname == "antithesis_pub":
                continue
            try:
                helper_pg_upstream.execute(
                    f'DROP PUBLICATION IF EXISTS "{pubname}"'
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning(
                    "reset upstream: drop publication %s failed: %s",
                    pubname,
                    exc,
                )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset upstream: enumerate publications failed: %s", exc)

    # Terminate + drop any leftover logical replication slots. Materialize's
    # source workers hold a slot open; when we `DROP SOURCE` on the MZ side
    # the slot release races with the next test's CREATE — and a still-
    # active slot can't be dropped, which makes `dropped-slot-errors.td`
    # fail at `pg_drop_replication_slot` with "slot is active for PID …".
    # Force-terminate then drop. Best-effort.
    try:
        rows = helper_pg_upstream.query(
            "SELECT slot_name, active_pid FROM pg_replication_slots"
        )
        for slot_name, active_pid in rows:
            if active_pid is not None:
                try:
                    helper_pg_upstream.execute(
                        "SELECT pg_terminate_backend(%s)", (active_pid,)
                    )
                except Exception as exc:  # noqa: BLE001
                    LOG.warning(
                        "reset upstream: terminate slot pid %s failed: %s",
                        active_pid,
                        exc,
                    )
            try:
                helper_pg_upstream.execute(
                    "SELECT pg_drop_replication_slot(%s)", (slot_name,)
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning(
                    "reset upstream: drop slot %s failed: %s", slot_name, exc
                )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset upstream: enumerate replication slots failed: %s", exc)

    # Drop non-system user schemas. Schemas the td files commonly create
    # in passing — `a`, `other`, `schema1`, `conflict_schema`, etc. —
    # need to go, but the data-loss workload's `antithesis_pg_cdc` and
    # postgres' system schemas must stay.
    preserve_schemas = {
        "public",
        "antithesis_pg_cdc",
        "pg_catalog",
        "information_schema",
        "pg_toast",
        "pg_temp_1",
        "pg_toast_temp_1",
    }
    try:
        # psycopg treats `%` as a parameter placeholder; escape with `%%`
        # for the literal LIKE-pattern wildcard.
        rows = helper_pg_upstream.query(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname NOT LIKE 'pg_%%' AND nspname <> 'information_schema'"
        )
        for (nspname,) in rows:
            if nspname in preserve_schemas:
                continue
            try:
                helper_pg_upstream.execute(
                    f'DROP SCHEMA IF EXISTS "{nspname}" CASCADE'
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning(
                    "reset upstream: drop schema %s failed: %s", nspname, exc
                )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset upstream: enumerate schemas failed: %s", exc)

    # Re-create `public` last — many tests assume it exists. DROP CASCADE
    # clears everything inside it.
    try:
        helper_pg_upstream.execute("DROP SCHEMA IF EXISTS public CASCADE")
        helper_pg_upstream.execute("CREATE SCHEMA public")
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset upstream: drop/create schema public failed: %s", exc)

    # Drop non-superuser, non-system roles. `privileges.td` creates a
    # `priv` role and doesn't clean up; left in place it fails the next
    # CREATE USER with "already exists". Postgres + replication role
    # names (postgres, pg_*) are preserved.
    try:
        rows = helper_pg_upstream.query(
            "SELECT rolname FROM pg_roles "
            "WHERE rolname NOT LIKE 'pg_%%' AND rolname <> 'postgres'"
        )
        for (rolname,) in rows:
            try:
                helper_pg_upstream.execute(f'DROP ROLE IF EXISTS "{rolname}"')
            except Exception as exc:  # noqa: BLE001
                LOG.warning(
                    "reset upstream: drop role %s failed: %s", rolname, exc
                )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("reset upstream: enumerate roles failed: %s", exc)
