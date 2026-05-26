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

LOG = logging.getLogger("antithesis.helper_testdrive")

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

# Upstream credentials surfaced as `${arg.*}` to checked-in .td files.
# Sourced from the Workload service's environment (see the Workload
# class in test/antithesis/mzcompose.py) so a credential change in the
# mzcompose service propagates here automatically.
MYSQL_ROOT_PASSWORD = os.environ.get("MYSQL_PASSWORD", "p@ssw0rd")
SQL_SERVER_USER = os.environ.get("SQL_SERVER_USER", "SA")
SQL_SERVER_PASSWORD = os.environ.get("SQL_SERVER_PASSWORD", "RPSsql12345")

# Fault-shape pattern list and matcher live in `helper_fault_tolerance`
# so every Antithesis driver agrees on what "looks like Antithesis killed
# something" means.  Re-exported here as `TRANSIENT_PATTERNS` for
# backwards-compatibility with any external caller that imported the
# module-level tuple before the extraction.
from helper_fault_tolerance import looks_like_fault

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

        Delegates to `helper_fault_tolerance.looks_like_fault` so the
        pattern list stays canonical across all Antithesis drivers.  See
        that module's docstring for what qualifies.
        """
        if self.succeeded:
            return False
        return looks_like_fault(self.stdout + "\n" + self.stderr)


def _strip_skip_if_true(content: str) -> tuple[str, bool]:
    """Remove `$ skip-if / SELECT true` blocks from a testdrive script.

    Returns (rewritten content, True if a block was removed). Other
    `skip-if` shapes are left intact because they encode meaningful
    feature-flag gates we still want to respect.
    """
    rewritten, n = _SKIP_IF_TRUE_RE.subn("", content)
    return rewritten, n > 0


def _read_and_rewrite(td_file: str) -> str:
    """Load a bundled testdrive file and strip skip-if-true headers.

    Returns the rewritten file contents. Raises FileNotFoundError if
    the bundle is missing the requested path (most likely cause: the
    pre-image: copy glob in mzbuild.yml doesn't cover the directory).
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
    return rewritten


def run(
    td_file: str,
    *,
    timeout_s: float = 600.0,
    extra_args: list[str] | None = None,
    prelude_files: list[str] | None = None,
) -> TestdriveResult:
    """Run a bundled testdrive file. `td_file` is repo-relative, e.g.
    "test/pg-cdc/dropped-slot-errors.td".

    The repo file is read, rewritten in memory to strip a skip-if-true
    header if present, and the rewritten copy is fed to testdrive. The
    on-disk repo file is never modified.

    `prelude_files` lets the caller prepend the contents of one or more
    other bundled .td files before the target. Used by the sql-server-cdc
    testdrive runner to inline `test/sql-server-cdc/setup/setup.td`
    (which DROPs and re-creates the `test` database + DummyTicker job)
    ahead of each random .td file, mirroring what the repo's
    workflow_cdc does. Each prelude file is also rewritten by the
    skip-if-true stripper.
    """
    rewritten = _read_and_rewrite(td_file)
    if prelude_files:
        parts = [_read_and_rewrite(p) for p in prelude_files]
        # Append the target last; a `\n` between parts so a non-newline-
        # terminated prelude doesn't merge with the next file's first line.
        rewritten = "\n".join([*parts, rewritten])
    return _run_content(
        rewritten, label=td_file, timeout_s=timeout_s, extra_args=extra_args
    )


def run_inline(
    content: str,
    *,
    label: str = "inline",
    timeout_s: float = 180.0,
    extra_args: list[str] | None = None,
) -> TestdriveResult:
    """Run a literal testdrive script.  Used by drivers that need to assert
    on an external system (notably iceberg/duckdb) where there's no good
    way to round-trip through Materialize and a checked-in `.td` doesn't
    cover the per-invocation parameterisation we want (batch_id, expected
    count).

    `content` is fed to testdrive verbatim — skip-if-true stripping does
    not apply.  `label` is logged for triage but is otherwise opaque.
    """
    return _run_content(
        content, label=label, timeout_s=timeout_s, extra_args=extra_args
    )


def _run_content(
    content: str,
    *,
    label: str,
    timeout_s: float,
    extra_args: list[str] | None,
) -> TestdriveResult:
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
        f.write(content)

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
        # `test/mysql-cdc/*.td` references the upstream MySQL password
        # as `${arg.mysql-root-password}`. The antithesis topology has
        # one MySQL primary at `mysql`; tests that create connections
        # via `HOST mysql` work unmodified.
        f"--var=mysql-root-password={MYSQL_ROOT_PASSWORD}",
        # `test/sql-server-cdc/*.td` references the upstream SQL
        # Server creds as `${arg.default-sql-server-user}` and
        # `${arg.default-sql-server-password}`. Test files expect the
        # `test` database to already exist with CDC enabled —
        # first_sql_server_cdc_setup.py handles that during the setup
        # phase, before any testdrive run.
        f"--var=default-sql-server-user={SQL_SERVER_USER}",
        f"--var=default-sql-server-password={SQL_SERVER_PASSWORD}",
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
        LOG.warning("testdrive %s timed out after %.0fs", label, timeout_s)
        result = TestdriveResult(
            exit_code=124,
            stdout=(
                exc.stdout.decode()
                if isinstance(exc.stdout, bytes)
                else (exc.stdout or "")
            ),
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
        label,
        result.exit_code,
        result.looks_transient,
        len(result.stdout),
        len(result.stderr),
    )
    return result
