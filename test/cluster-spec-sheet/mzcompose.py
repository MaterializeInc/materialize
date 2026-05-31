# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Reproduces cluster spec sheet results on Materialize Cloud (or local Docker).
"""

import argparse
import csv
import glob
import itertools
import os
import re
import shlex
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, TextIO

import matplotlib.pyplot as plt
import pandas as pd
import psycopg
from psycopg import InterfaceError, OperationalError
from psycopg import sql as psycopg_sql

from materialize import MZ_ROOT, buildkite
from materialize.mz_version import MzVersion
from materialize.mzcompose import _wait_for_pg
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import metadata_store_services
from materialize.mzcompose.services.mz import Mz
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.cluster_spec_sheet import (
    cluster_spec_sheet_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.ui import CommandFailureCausedUIError, UIError

CLUSTER_SPEC_SHEET_VERSION = "1.0.0"  # Used for uploading test analytics results

PRODUCTION_REGION = "aws/us-east-1"
PRODUCTION_ENVIRONMENT = "production"
PRODUCTION_USERNAME = os.getenv("NIGHTLY_MZ_USERNAME", "infra+bot@materialize.com")
PRODUCTION_APP_PASSWORD = os.getenv("MZ_CLI_APP_PASSWORD")

STAGING_REGION = "aws/eu-west-1"
STAGING_ENVIRONMENT = "staging"
STAGING_USERNAME = os.getenv(
    "NIGHTLY_CANARY_USERNAME", "infra+nightly-canary@materialize.com"
)
STAGING_APP_PASSWORD = os.getenv("NIGHTLY_CANARY_APP_PASSWORD")

MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS = {
    "memory_limiter_interval": "0s",
    "max_credit_consumption_rate": "1024",
    # Headroom over the 30k default cap from `--envd-objects-scalability-sizes`,
    # plus the per-pad-cluster MV split. Cloud targets need
    # matching limits configured server-side.
    "max_tables": "200000",
    "max_materialized_views": "200000",
    "max_objects_per_schema": "200000",
    "max_clusters": "50",
}


# --- Intra-region parallelism (see workflow_default / run_parallel) ---
# Per-statement server-side timeout (ms). Generous headroom over any healthy
# spec-sheet operation (tens of minutes at worst), but bounds a hung query or
# crash-looping region to minutes instead of the multi-day Buildkite cap that
# a missing timeout previously allowed. Set to 0 to disable.
DEFAULT_STATEMENT_TIMEOUT_MS = 60 * 60 * 1000  # 60 minutes
# Each parallel job owns a throwaway "quickstart" cluster for ad-hoc queries
# issued before its measurement cluster `c` exists.
PARALLEL_QUICKSTART_SIZE_CLOUD = "50cc"
PARALLEL_QUICKSTART_SIZE_DOCKER = "scale=1,workers=1"
# Clusters a single parallel job holds at its peak: its own quickstart, the
# load-generator cluster `lg`, and the measurement cluster `c`.
PARALLEL_CLUSTERS_PER_JOB = 3
# Fraction of the configured server-side limits (max_credit_consumption_rate,
# max_clusters) the scheduler may reserve, leaving headroom for estimate drift
# and transient overlap while clusters are torn down / recreated.
PARALLEL_LIMIT_SAFETY = 0.8


def staging_version() -> str:
    return f"{MzVersion.parse_cargo()}--pr.g{os.environ['BUILDKITE_COMMIT']}"


SERVICES = [
    # Overridden below
    Mz(app_password=""),
    *metadata_store_services(),
    Materialized(
        propagate_crashes=True,
        additional_system_parameter_defaults=MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    ),
    # dbbench service built from our mzbuild Dockerfile (test/dbbench)
    MzComposeService(
        "dbbench",
        {
            "mzbuild": "dbbench",
        },
    ),
]

SCENARIO_AUCTION_STRONG = "auction_strong"
SCENARIO_AUCTION_WEAK = "auction_weak"
SCENARIO_TPCH_MV_STRONG = "tpch_mv_strong"
SCENARIO_TPCH_QUERIES_STRONG = "tpch_queries_strong"
SCENARIO_TPCH_QUERIES_WEAK = "tpch_queries_weak"
SCENARIO_TPCH_STRONG = "tpch_strong"
SCENARIO_SOURCE_INGESTION_STRONG = "source_ingestion_strong"
SCENARIO_QPS_ENVD_STRONG_SCALING = "qps_envd_strong_scaling"
SCENARIO_COPY_FROM_STDIN_ENVD_STRONG_SCALING = "copy_from_stdin_envd_strong_scaling"
SCENARIO_ENVD_OBJECTS_SCALABILITY_TABLES = "envd_objects_scalability_tables"
SCENARIO_ENVD_OBJECTS_SCALABILITY_MVS = "envd_objects_scalability_mvs"
SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_PERSIST_SOURCES = (
    "cluster_object_limits_indexes_from_persist_sources"
)
SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_INDEX = (
    "cluster_object_limits_indexes_from_index"
)
SCENARIO_CLUSTER_OBJECT_LIMITS_MVS_FROM_PERSIST_SOURCES = (
    "cluster_object_limits_mvs_from_persist_sources"
)

# Scenario groupings (`SCENARIO_GROUPS`, `SCENARIOS_BY_NAME`) are derived
# from the `SCENARIOS` registry further down — see `ScenarioSpec`.

REPLICA_SCALES = [1, 2, 4, 8, 16, 32]

ENVD_OBJECTS_SCALABILITY_SIZES = [
    1,
    10,
    100,
    1_000,
    3_000,
    5_000,
    10_000,
    20_000,
    30_000,
]
ENVD_OBJECTS_SCALABILITY_MVS_PER_CLUSTER = 10_000

# Default N-walk for the cluster_object_limits scenarios: geometric up to 1k,
# then +1k increments. The cap is configurable via --cluster-object-limits-max
# (default mirrored in `default_cluster_object_limits_sizes()`).
CLUSTER_OBJECT_LIMITS_DEFAULT_MAX = 30_000
CLUSTER_OBJECT_LIMITS_GEOMETRIC_HEAD = [100, 200, 500, 1_000]
CLUSTER_OBJECT_LIMITS_LINEAR_STEP = 1_000

# Freshness probe knobs. Healthy = max local lag below threshold.
CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS = 2_000
# Cap on recorded lag values for plot legibility; the `healthy`
# column preserves the underlying truth.
CLUSTER_OBJECT_LIMITS_LAG_CAP_MS = 10 * CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS
# Per-batch deadline for all N materializations to report
# `hydrated=true` in `mz_hydration_statuses`. Steady-state lag
# check below handles overloaded clusters. Probes that time out
# are recorded with `failure_mode="hydration_timeout"`.
CLUSTER_OBJECT_LIMITS_HYDRATION_TIMEOUT_S = 300
# Longer timeout for the first probe after `CREATE CLUSTER c`,
# to absorb cold-start cost on larger replicas. Subsequent probes
# (warmer replica) use the shorter timeout above.
CLUSTER_OBJECT_LIMITS_FIRST_PROBE_HYDRATION_TIMEOUT_S = 180
# Steady-state sampling window after hydration: take this many samples spaced
# CLUSTER_OBJECT_LIMITS_SAMPLE_INTERVAL_S apart and use the max as the
# representative lag.
CLUSTER_OBJECT_LIMITS_SAMPLES = 5
CLUSTER_OBJECT_LIMITS_SAMPLE_INTERVAL_S = 2
# After the coarse N-walk locates the first unhealthy N for a given cluster
# size, bisect the (last_healthy, first_unhealthy) interval this many times to
# narrow the cliff. With a coarse +1k step, 4 bisection steps narrow the cliff
# to ±~60. Set to 0 to disable.
CLUSTER_OBJECT_LIMITS_BISECT_STEPS = 4


def default_cluster_object_limits_sizes(max_n: int) -> list[int]:
    """Geometric ramp up to 1k, then +1k increments up to ``max_n``."""
    sizes = [n for n in CLUSTER_OBJECT_LIMITS_GEOMETRIC_HEAD if n <= max_n]
    next_n = (
        CLUSTER_OBJECT_LIMITS_GEOMETRIC_HEAD[-1] if sizes else 0
    ) + CLUSTER_OBJECT_LIMITS_LINEAR_STEP
    while next_n <= max_n:
        sizes.append(next_n)
        next_n += CLUSTER_OBJECT_LIMITS_LINEAR_STEP
    return sizes


class ConnectionHandler:
    def __init__(
        self,
        new_connection: Callable[..., psycopg.Connection],
        *,
        dbname: str | None = None,
        cluster: str | None = None,
        statement_timeout_ms: int | None = None,
    ) -> None:
        # ``new_connection`` accepts ``dbname`` / ``cluster`` /
        # ``statement_timeout_ms`` keyword args; the values stored here are
        # re-applied on every (re)connect so that the worker's database,
        # default cluster, and statement timeout survive a dropped connection
        # (e.g. a TLS EOF against a crash-looping region).
        self.new_connection = new_connection
        self.dbname = dbname
        self.cluster = cluster
        self.statement_timeout_ms = statement_timeout_ms
        self.connection = self._open()
        self.cursor: psycopg.Cursor | None = None

    def _open(self) -> psycopg.Connection:
        return self.new_connection(
            dbname=self.dbname,
            cluster=self.cluster,
            statement_timeout_ms=self.statement_timeout_ms,
        )

    def set_default_cluster(self, cluster: str) -> None:
        """Make subsequent (re)connections default to ``cluster``.

        Used after a job's measurement cluster is (re)created so that a
        mid-measurement reconnect resumes on the right cluster instead of the
        bootstrap quickstart cluster."""
        self.cluster = cluster

    def __ensure_connection(self):
        if not self.connection or self.connection.closed:
            self.connection = self._open()

    def __enter__(self) -> psycopg.Cursor:
        self.__ensure_connection()
        assert not self.cursor
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None

    def retryable(self, func: Callable[[], Any], retries: int = 5) -> Any:
        while True:
            try:
                self.__ensure_connection()
                return func()
            except (InterfaceError, OperationalError) as e:
                if retries <= 0:
                    raise e
                print(f"Retryable error: {e}, reconnecting...")
                time.sleep(5)
                self.connection.close()
                retries -= 1


class ScenarioRunner:
    def __init__(
        self,
        scenario: str,
        scenario_version: str,
        scale: int | float,
        mode: str,
        connection: ConnectionHandler,
        results_writer: csv.DictWriter,
        target: "BenchTarget",
        write_lock: "threading.Lock | None" = None,
    ) -> None:
        # Serializes writes to ``results_writer``; parallel workers writing to
        # the same CSV stream must share one lock.
        self.write_lock = write_lock or threading.Lock()
        self.scenario = scenario
        self.scenario_version = scenario_version
        self.scale = scale
        self.mode = mode
        self.connection = connection
        self.results_writer = results_writer
        # Per-point fields populated by the scenario's `apply()` (cluster
        # size and envd CPU count) before each `measure()` call.
        self.replica_size: str | None = None
        self.target = target
        self.envd_cpus: int | None = None
        # Cluster names used by the generic helpers and scenario SQL. Defaults
        # match the historical hard-coded names so the serial path is
        # unchanged; the parallel driver overrides them per worker so
        # concurrently-running jobs don't collide on the global cluster
        # namespace.
        self.cluster: str = "c"
        self.lg_cluster: str = "lg"
        # Prefix for log lines, set per worker in parallel mode so interleaved
        # output is attributable.
        self.log_prefix: str = ""

    def add_result(
        self,
        category: str,
        name: str,
        repetition: int,
        size_bytes: int | None,
        time_ms: int | None = None,
        qps: float | None = None,
        healthy: int | None = None,
        failure_mode: str | None = None,
    ) -> None:
        """Write one result row.

        ``healthy`` and ``failure_mode`` are recorded for
        cluster_object_limits only; extra columns are silently dropped
        on streams whose schema doesn't include them
        (``csv.DictWriter(extrasaction="ignore")``).
        """
        with self.write_lock:
            self.results_writer.writerow(
                {
                    "scenario": self.scenario,
                    "scenario_version": self.scenario_version,
                    "scale": self.scale,
                    "mode": self.mode,
                    "category": category,
                    "test_name": name,
                    "cluster_size": self.replica_size,
                    "envd_cpus": self.envd_cpus,
                    "repetition": repetition,
                    "size_bytes": size_bytes,
                    "time_ms": time_ms,
                    "qps": qps,
                    "healthy": healthy,
                    "failure_mode": failure_mode,
                }
            )

    def run_query(
        self, query: str, fetch: bool = False, retries: int = 5, **params
    ) -> list | None:
        query = dedent(query).strip()
        if "CREATE SECRET" not in query:
            print(f"{self.log_prefix}> {query} {params or ''}")
        for retry in range(retries + 1):
            try:
                with self.connection as cur:
                    cur.execute(query.encode(), params)
                    if fetch:
                        return cur.fetchall()
                    else:
                        return None
            except (
                InterfaceError,
                OperationalError,
                psycopg.errors.SystemError,
            ) as e:
                if retry >= retries:
                    raise
                print(f"Retryable error (attempt {retry + 1}/{retries}): {e}")
                time.sleep(5 * (retry + 1))
        return None

    def measure(
        self,
        category: str,
        name: str,
        setup: list[str],
        query: list[str | tuple[str, list[tuple]]],
        after: list[str] = [],
        repetitions: int = 1,
        size_of_index: str | None = None,
        setup_delay: float = 0.0,
    ) -> None:
        print(
            f"{self.log_prefix}--- Running {name} for {self.replica_size} with {repetitions} repetitions..."
        )
        for repetition in range(repetitions):

            def inner() -> None:
                for setup_part in setup:
                    self.run_query(setup_part)
                time.sleep(setup_delay)
                start_time = time.time()
                for query_part in query:
                    if isinstance(query_part, str):
                        self.run_query(query_part)
                    else:
                        q = query_part[0]
                        expected = query_part[1]
                        actual = self.run_query(q, fetch=True)
                        assert actual == expected, f"Expected {expected}, got {actual}"
                end_time = time.time()
                if size_of_index:
                    # We need to wait for the introspection source to catch up.
                    time.sleep(2)
                    size_bytes = self.size_of_dataflow(f"%{size_of_index}")
                    print(f"Size of index {size_of_index}: {size_bytes}")
                else:
                    size_bytes = None
                self.add_result(
                    category,
                    name,
                    repetition,
                    size_bytes,
                    time_ms=int((end_time - start_time) * 1000),
                )
                for after_part in after:
                    self.run_query(after_part)

            self.connection.retryable(inner)

    def measure_dbbench(
        self,
        category: str,
        name: str,
        *,
        setup: list[str],
        query: list[str],
        after: list[str],
        duration: str,
        concurrency: int | None = None,
    ) -> None:
        """
        Run dbbench via the 'dbbench' mzcompose service and record its final QPS.

        Parameters somewhat mirror ScenarioRunner.measure for consistency, but are applied to a
        generated dbbench.ini:
          - setup:    SQL statements run once before the workload (dbbench [setup])
          - query:    SQL statements that form the workload (dbbench [loadtest] query=...)
          - after:    SQL statements run once after the workload (dbbench [teardown])
          - duration: length of the workload (top-level duration=...)
          - concurrency: concurrent sessions for the workload.

        We capture and parse dbbench's output (stderr or stdout) to find the last
        occurrence of a "QPS" value that dbbench reports in its summary, and store that
        as the 'qps' column in the results CSV. If no QPS is found, the test fails.
        (We omit time_ms for these rows since wall-clock time is not meaningful here.)
        """

        # Build a dbbench command that targets the current Materialize instance and
        # the active cluster 'c' using target-provided flags.
        flags = self.target.dbbench_connection_flags()

        # Render a minimal INI file for dbbench.
        def ini_escape(s: str) -> str:
            return s.replace("\n", " ")

        lines: list[str] = []
        lines.append(f"duration={duration}")
        # [setup]
        if setup:
            lines.append("")
            lines.append("[setup]")
            for q in setup:
                q = dedent(q).strip()
                if q:
                    lines.append(f"query={ini_escape(q)}")
        # [teardown]
        if after:
            lines.append("")
            lines.append("[teardown]")
            for q in after:
                q = dedent(q).strip()
                if q:
                    lines.append(f"query={ini_escape(q)}")
        # [loadtest]
        lines.append("")
        lines.append("[loadtest]")
        for q in query:
            q = dedent(q).strip()
            if q:
                lines.append(f"query={ini_escape(q)}")
        if concurrency is not None:
            lines.append(f"concurrency={int(concurrency)}")

        ini_text = "\n".join(lines) + "\n"

        # Construct a shell snippet to write the INI and execute dbbench
        quoted_flags = " ".join(shlex.quote(x) for x in flags)
        script = (
            "set -euo pipefail; "
            'tmp="$(mktemp -t dbbench.XXXXXX)"; '
            'cat > "$tmp"; '
            f'exec dbbench {quoted_flags} -intermediate-stats=false "$tmp"'
        )

        print(f"--- Running dbbench step '{name}' for {self.envd_cpus} ...")
        # Execute the command in the 'dbbench' service container and capture output
        result = self.target.composition.run(
            "dbbench",
            "-lc",  # sh arg to make it run `script`
            script,
            entrypoint="bash",
            rm=True,
            capture_and_print=True,
            stdin=ini_text,
        )

        # dbbench writes its final summary to stderr; prefer stderr but also
        # consider stdout just in case.
        stderr_out = result.stderr or ""
        stdout_out = result.stdout or ""
        combined_out = f"{stderr_out}\n{stdout_out}".strip()

        # Persist full dbbench output to a log file for later inspection
        logs_dir = os.path.join("test", "cluster-spec-sheet", "dbbench-logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Build a descriptive, filesystem-safe filename
        def _slug(x: Any) -> str:
            return re.sub(r"[^A-Za-z0-9._=-]+", "-", str(x))

        fname = f"{_slug(self.scenario)}_{_slug(self.mode)}_{_slug(self.envd_cpus)}_{_slug(name)}.log"
        log_path = os.path.join(logs_dir, fname)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(combined_out + ("\n" if not combined_out.endswith("\n") else ""))
        print(f"dbbench logs saved to {log_path}")

        # Parse QPS from output.
        # (Alternatively, we could make our fork of dbbench print a more machine-readable output.)
        # TODO: Later, we'll want to also look at latency (avg and tail-latency), but seems too unstable for now.
        qps_val: float | None = None
        try:
            qps_matches = re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*QPS", combined_out)
            if len(qps_matches) > 1:
                raise UIError(
                    f"dbbench: found multiple QPS values in output: {qps_matches}"
                )
            if qps_matches:
                qps_val = float(qps_matches[0])
        except Exception as e:
            raise UIError(f"Failed to parse dbbench QPS from output: {e}")

        if qps_val is None:
            tail = "\n".join(combined_out.splitlines()[-25:])
            raise UIError("dbbench: failed to find QPS in output. Last lines:\n" + tail)
        else:
            print(f"dbbench parsed QPS: {qps_val}")

        # Record a result row: put QPS into the 'qps' column, and omit time_ms (not applicable)
        self.add_result(
            category,
            name,
            # For QPS measurements, we have only one repetition (i.e., repetition 0). We have `duration` instead of
            # `repetitions` in QPS benchmarks currently (but maybe it's good to keep `repetition` in the schema in case
            # we ever want also multiple repetitions for QPS).
            0,
            None,
            qps=qps_val,
        )

    def size_of_dataflow(self, object: str) -> int | None:
        retries = 10
        while retries > 0:
            result = self.run_query(
                "SELECT size FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE %(name)s;",
                name=object,
                fetch=True,
            )
            if result and len(result) == 1 and result[0][0] is not None:
                return int(result[0][0])
            retries -= 1
            if retries > 0:
                time.sleep(1)
        return None


class ClusterScalingScenario(ABC):
    """Workload base for the strong/weak/envd_cpu sweep families.

    Wrapped by `StrongScalingSweep` / `WeakScalingSweep` /
    `EnvdCpuSweep`, which drive the cluster-size or envd-cpu loop and
    delegate per-point measurement to `run()` here.

    Lifecycle: `setup()` builds load-generator clusters / sources /
    tables, `materialize_views()` lists persist objects to hydrate,
    `run()` measures, `drop()` cleans up.
    """

    # Bump this version in the individual test if it changes in a way that
    # makes comparing results between versions useless.
    VERSION: str = "1.0.0"

    def __init__(self, scale: int | float, replica_size: str | None) -> None:
        self.scale = scale
        self.replica_size = replica_size
        # Cluster names this workload emits in its SQL. Defaults match the
        # historical hard-coded names; the parallel driver overrides them per
        # worker (via the wrapping sweep's `apply_namespace`) so concurrent
        # jobs don't collide on the global cluster namespace.
        self.cluster: str = "c"
        self.lg_cluster: str = "lg"

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def setup(self) -> list[str]: ...

    def drop(self) -> list[str]:
        return []

    def materialize_views(self) -> list[str]:
        """Returns names of the persist objects that setup initializes."""
        return []

    @abstractmethod
    def run(self, runner: ScenarioRunner) -> None: ...


class TpchScenario(ClusterScalingScenario):

    def name(self) -> str:
        return "tpch"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;",
            f"CREATE CLUSTER {self.lg_cluster} SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER {self.lg_cluster} FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return [f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index

        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            setup=[
                "DROP INDEX IF EXISTS lineitem_primary_idx CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE DEFAULT INDEX ON lineitem;",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
            size_of_index="lineitem_primary_idx",
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_fast_path",
            size_of_index="lineitem_primary_idx",
            setup=[],
            query=[
                "SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_fast_path",
            size_of_index="lineitem_primary_idx",
            setup=[],
            query=[
                "SELECT * FROM lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.measure(
            "arrangement_formation",
            "index_restart",
            size_of_index="lineitem_primary_idx",
            setup=[
                "SELECT count(*) > 0 FROM lineitem;",
                f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 0);",
            ],
            query=[
                f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class TpchScenarioMV(ClusterScalingScenario):

    def name(self) -> str:
        return "tpch_mv"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;",
            f"CREATE CLUSTER {self.lg_cluster} SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER {self.lg_cluster} FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return [f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.measure(
            "materialized_view_formation",
            "create_materialize_view",
            setup=[
                "DROP MATERIALIZED VIEW IF EXISTS mv_lineitem CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE MATERIALIZED VIEW mv_lineitem AS SELECT * FROM lineitem;",
                "SELECT count(*) > 0 FROM mv_lineitem;",
            ],
        )

        time.sleep(3)

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_slow_path",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_fast_path",
            setup=[],
            query=[
                "SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_slow_path",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_tax = 123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_fast_path",
            setup=[],
            query=[
                "SELECT * FROM mv_lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.measure(
            "materialized_view_formation",
            "materialized_view_restart",
            setup=[
                "SELECT count(*) > 0 FROM mv_lineitem;",
                f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 0);",
            ],
            query=[
                f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class TpchScenarioQueriesIndexedInputs(ClusterScalingScenario):

    def name(self) -> str:
        return "tpch_queries_indexed_inputs"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;",
            f"CREATE CLUSTER {self.lg_cluster} SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER {self.lg_cluster} FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
            """
            CREATE VIEW revenue (supplier_no, total_revenue) AS
            SELECT
                l_suppkey,
                sum(l_extendedprice * (1 - l_discount))
            FROM
                lineitem
            WHERE
                l_shipdate >= DATE '1996-01-01'
              AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' month
            GROUP BY
                l_suppkey;
            """,
            """
            CREATE VIEW vq01 AS
            SELECT
                l_returnflag,
                l_linestatus,
                sum(l_quantity) AS sum_qty,
                sum(l_extendedprice) AS sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                avg(l_quantity) AS avg_qty,
                avg(l_extendedprice) AS avg_price,
                avg(l_discount) AS avg_disc,
                count(*) AS count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;
            """,
            """
            -- name: Q02
            CREATE VIEW vq02 AS
            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM
                part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND p_size = CAST (15 AS smallint)
              AND p_type LIKE '%%BRASS'
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
              AND ps_supplycost
                = (
                      SELECT
                          min(ps_supplycost)
                      FROM
                          partsupp, supplier, nation, region
                      WHERE
                          p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                  )
            ORDER BY
                s_acctbal DESC, n_name, s_name, p_partkey;
            """,
            """
            -- name: Q03
            CREATE VIEW vq03 AS
            SELECT
                l_orderkey,
                sum(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                customer,
                orders,
                lineitem
            WHERE
                c_mktsegment = 'BUILDING'
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate < DATE '1995-03-15'
              AND l_shipdate > DATE '1995-03-15'
            GROUP BY
                l_orderkey,
                o_orderdate,
                o_shippriority
            ORDER BY
                revenue DESC,
                o_orderdate;
            """,
            """
            -- name: Q04
            CREATE VIEW vq04 AS
            SELECT
                o_orderpriority,
                count(*) AS order_count
            FROM
                orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
              AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' month
              AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem
                WHERE
                    l_orderkey = o_orderkey
                  AND l_commitdate < l_receiptdate
            )
            GROUP BY
                o_orderpriority
            ORDER BY
                o_orderpriority;
            """,
            """
            -- name: Q05
            CREATE VIEW vq05 AS
            SELECT
                n_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            WHERE
                c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND l_suppkey = s_suppkey
              AND c_nationkey = s_nationkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'ASIA'
              AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate < DATE '1995-01-01'
            GROUP BY
                n_name
            ORDER BY
                revenue DESC;
            """,
            """
            -- name: Q06
            CREATE VIEW vq06 AS
            SELECT
                sum(l_extendedprice * l_discount) AS revenue
            FROM
                lineitem
            WHERE
                l_quantity < 24
              AND l_shipdate >= DATE '1994-01-01'
              AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' year
              AND l_discount BETWEEN 0.06 - 0.01 AND 0.07;
            """,
            """
            -- name: Q07
            CREATE VIEW vq07 AS
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) AS revenue
            FROM
                (
                    SELECT
                        n1.n_name AS supp_nation,
                        n2.n_name AS cust_nation,
                        extract(year FROM l_shipdate) AS l_year,
                        l_extendedprice * (1 - l_discount) AS volume
                    FROM
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                    WHERE
                        s_suppkey = l_suppkey
                      AND o_orderkey = l_orderkey
                      AND c_custkey = o_custkey
                      AND s_nationkey = n1.n_nationkey
                      AND c_nationkey = n2.n_nationkey
                      AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                        )
                      AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                ) AS shipping
            GROUP BY
                supp_nation,
                cust_nation,
                l_year
            ORDER BY
                supp_nation,
                cust_nation,
                l_year;
            """,
            """
            -- name: Q08
            CREATE VIEW vq08 AS
            SELECT
                o_year,
                sum(case
                        when nation = 'BRAZIL' then volume
                        else 0
                    end) / sum(volume) AS mkt_share
            FROM
                (
                    SELECT
                        extract(year FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) AS volume,
                        n2.n_name AS nation
                    FROM
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                    WHERE
                        p_partkey = l_partkey
                      AND s_suppkey = l_suppkey
                      AND l_orderkey = o_orderkey
                      AND o_custkey = c_custkey
                      AND c_nationkey = n1.n_nationkey
                      AND n1.n_regionkey = r_regionkey
                      AND r_name = 'AMERICA'
                      AND s_nationkey = n2.n_nationkey
                      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                      AND p_type = 'ECONOMY ANODIZED STEEL'
                ) AS all_nations
            GROUP BY
                o_year
            ORDER BY
                o_year;
            """,
            """
            -- name: Q09
            CREATE VIEW vq09 AS
            SELECT
                nation,
                o_year,
                sum(amount) AS sum_profit
            FROM
                (
                    SELECT
                        n_name AS nation,
                        extract(year FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                    FROM
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                    WHERE
                        s_suppkey = l_suppkey
                      AND ps_suppkey = l_suppkey
                      AND ps_partkey = l_partkey
                      AND p_partkey = l_partkey
                      AND o_orderkey = l_orderkey
                      AND s_nationkey = n_nationkey
                      AND p_name like '%%green%%'
                ) AS profit
            GROUP BY
                nation,
                o_year
            ORDER BY
                nation,
                o_year DESC;
            """,
            """
            -- name: Q10
            CREATE VIEW vq10 AS
            SELECT
                c_custkey,
                c_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM
                customer,
                orders,
                lineitem,
                nation
            WHERE
                c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= DATE '1993-10-01'
              AND o_orderdate < DATE '1994-01-01'
              AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' month
              AND l_returnflag = 'R'
              AND c_nationkey = n_nationkey
            GROUP BY
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            ORDER BY
                revenue DESC;
            """,
            """
            -- name: Q11
            CREATE VIEW vq11 AS
            SELECT
                ps_partkey,
                sum(ps_supplycost * ps_availqty) AS value
            FROM
                partsupp,
                supplier,
                nation
            WHERE
                ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
            GROUP BY
                ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                    SELECT
                        sum(ps_supplycost * ps_availqty) * 0.0001
                    FROM
                        partsupp,
                        supplier,
                        nation
                    WHERE
                        ps_suppkey = s_suppkey
                      AND s_nationkey = n_nationkey
                      AND n_name = 'GERMANY'
                )
            ORDER BY
                value DESC;
            """,
            """
            -- name: Q12
            CREATE VIEW vq12 AS
            SELECT
                l_shipmode,
                sum(case
                        when o_orderpriority = '1-URGENT'
                            or o_orderpriority = '2-HIGH'
                            then 1
                        else 0
                    end) AS high_line_count,
                sum(case
                        when o_orderpriority <> '1-URGENT'
                            AND o_orderpriority <> '2-HIGH'
                            then 1
                        else 0
                    end) AS low_line_count
            FROM
                orders,
                lineitem
            WHERE
                o_orderkey = l_orderkey
              AND l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= DATE '1994-01-01'
              AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' year
            GROUP BY
                l_shipmode
            ORDER BY
                l_shipmode;
            """,
            """
            -- name: Q13
            CREATE VIEW vq13 AS
            SELECT
                c_count,
                count(*) AS custdist
            FROM
                (
                    SELECT
                        c_custkey,
                        count(o_orderkey) c_count -- workaround for no column aliases
                    FROM
                        customer LEFT OUTER JOIN orders ON
                            c_custkey = o_custkey
                                AND o_comment NOT LIKE '%%special%%requests%%'
                    GROUP BY
                        c_custkey
                ) AS c_orders -- (c_custkey, c_count) -- no column aliases yet
            GROUP BY
                c_count
            ORDER BY
                custdist DESC,
                c_count DESC;
            """,
            """
            -- name: Q14
            CREATE VIEW vq14 AS
            SELECT
                100.00 * sum(case
                                 when p_type like 'PROMO%%'
                                     then l_extendedprice * (1 - l_discount)
                                 else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM
                lineitem,
                part
            WHERE
                l_partkey = p_partkey
              AND l_shipdate >= DATE '1995-09-01'
              AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' month;
            """,
            """
            -- name: Q15
            CREATE VIEW vq15 AS
            SELECT
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            FROM
                supplier,
                revenue
            WHERE
                s_suppkey = supplier_no
              AND total_revenue = (
                SELECT
                    max(total_revenue)
                FROM
                    revenue
            )
            ORDER BY
                s_suppkey;
            """,
            """
            -- name: Q16
            CREATE VIEW vq16 AS
            SELECT
                p_brand,
                p_type,
                p_size,
                count(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
                partsupp,
                part
            WHERE
                p_partkey = ps_partkey
              AND p_brand <> 'Brand#45'
              AND p_type NOT LIKE 'MEDIUM POLISHED%%'
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
              AND ps_suppkey NOT IN (
                SELECT
                    s_suppkey
                FROM
                    supplier
                WHERE
                    s_comment like '%%Customer%%Complaints%%'
            )
            GROUP BY
                p_brand,
                p_type,
                p_size
            ORDER BY
                supplier_cnt DESC,
                p_brand,
                p_type,
                p_size;
            """,
            """
            -- name: Q17
            CREATE VIEW vq17 AS
            SELECT
                sum(l_extendedprice) / 7.0 AS avg_yearly
            FROM
                lineitem,
                part
            WHERE
                p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container = 'MED BOX'
              AND l_quantity < (
                SELECT
                    0.2 * avg(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = p_partkey
            );
            """,
            """
            -- name: Q18
            CREATE VIEW vq18 AS
            SELECT
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                sum(l_quantity)
            FROM
                customer,
                orders,
                lineitem
            WHERE
                o_orderkey IN (
                    SELECT
                        l_orderkey
                    FROM
                        lineitem
                    GROUP BY
                        l_orderkey having
                        sum(l_quantity) > 300
                )
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
            GROUP BY
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice
            ORDER BY
                o_totalprice DESC,
                o_orderdate;
            """,
            """
            -- name: Q19
            CREATE VIEW vq19 AS
            SELECT
                sum(l_extendedprice* (1 - l_discount)) AS revenue
            FROM
                lineitem,
                part
            WHERE
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#12'
                        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
               or
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#23'
                        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                        AND l_quantity >= CAST (10 AS smallint) AND l_quantity <= CAST (10 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (10 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
               or
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#34'
                        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                        AND l_quantity >= CAST (20 AS smallint) AND l_quantity <= CAST (20 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (15 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    );
            """,
            """
            -- name: Q20
            CREATE VIEW vq20 AS
                --SELECT
            --    s_name,
            --    s_address
            --FROM
            --    supplier,
            --    nation
            --WHERE
            --    s_suppkey IN (
            --        SELECT
            --            ps_suppkey
            --        FROM
            --            partsupp
            --        WHERE
            --            ps_partkey IN (
            --                SELECT
            --                    p_partkey
            --                FROM
            --                    part
            --                WHERE
            --                    p_name like 'forest%%'
            --            )
            --            AND ps_availqty > (
            --                SELECT
            --                    0.5 * sum(l_quantity)
            --                FROM
            --                    lineitem
            --                WHERE
            --                    l_partkey = ps_partkey
            --                    AND l_suppkey = ps_suppkey
            --                    AND l_shipdate >= DATE '1995-01-01'
            --                    AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
            --            )
            --    )
            --    AND s_nationkey = n_nationkey
            --    AND n_name = 'CANADA'
            --ORDER BY
            --    s_name;
            SELECT
                s_name,
                s_address
            FROM
                supplier,
                nation,
                (
                    SELECT DISTINCT t2.ps_suppkey
                    FROM
                        (
                            SELECT
                                ps_partkey, ps_suppkey, ps_availqty
                            FROM
                                part, partsupp
                            WHERE
                                p_name like 'forest%%'
                              AND ps_partkey = p_partkey
                        ) AS t2
                            ,
                        (
                            SELECT
                                ps_partkey, ps_suppkey, 0.5 * sum(l_quantity) as qty
                            FROM
                                part, partsupp, lineitem
                            WHERE
                                p_name LIKE 'forest%%'
                              AND ps_partkey = p_partkey
                              AND l_partkey = ps_partkey
                              AND l_suppkey = ps_suppkey
                              AND l_shipdate >= DATE '1995-01-01'
                              AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
                            GROUP BY ps_partkey, ps_suppkey
                        ) AS t3
                    WHERE t2.ps_partkey = t3.ps_partkey
                      AND t2.ps_suppkey = t3.ps_suppkey
                      AND t2.ps_availqty > t3.qty
                ) as sj
            WHERE
                s_nationkey = n_nationkey
              AND n_name = 'CANADA'
              AND sj.ps_suppkey = s_suppkey
            ORDER BY
                s_name;
            """,
            """
            -- name: Q21
            CREATE VIEW vq21 AS
            SELECT
                s_name,
                count(*) AS numwait
            FROM
                supplier,
                lineitem l1,
                orders,
                nation
            WHERE
                s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem l2
                WHERE
                    l2.l_orderkey = l1.l_orderkey
                  AND l2.l_suppkey <> l1.l_suppkey
            )
              AND not EXISTS (
                SELECT
                    *
                FROM
                    lineitem l3
                WHERE
                    l3.l_orderkey = l1.l_orderkey
                  AND l3.l_suppkey <> l1.l_suppkey
                  AND l3.l_receiptdate > l3.l_commitdate
            )
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
            GROUP BY
                s_name
            ORDER BY
                numwait DESC,
                s_name;
            """,
            """
            -- name: Q22
            CREATE VIEW vq22 AS
            SELECT
                cntrycode,
                count(*) AS numcust,
                sum(c_acctbal) AS totacctbal
            FROM
                (
                    SELECT
                        substring(c_phone, 1, 2) AS cntrycode, c_acctbal
                    FROM
                        customer
                    WHERE
                        substring(c_phone, 1, 2)
                            IN ('13', '31', '23', '29', '30', '18', '17')
                      AND c_acctbal
                        > (
                              SELECT
                                  avg(c_acctbal)
                              FROM
                                  customer
                              WHERE
                                  c_acctbal > 0.00
                                AND substring(c_phone, 1, 2)
                                  IN (
                                      '13',
                                      '31',
                                      '23',
                                      '29',
                                      '30',
                                      '18',
                                      '17'
                                        )
                          )
                      AND NOT
                        EXISTS(
                        SELECT
                            *
                        FROM
                            orders
                        WHERE
                            o_custkey = c_custkey
                    )
                )
                    AS custsale
            GROUP BY
                cntrycode
            ORDER BY
                cntrycode;
            """,
        ]

    def drop(self) -> list[str]:
        return [f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create indexes on inputs
        runner.measure(
            "arrangement_formation",
            "create_index_inputs",
            setup=["SELECT * FROM t;"],
            query=[
                "CREATE INDEX pk_nation_nationkey ON nation (n_nationkey ASC);",
                "CREATE INDEX fk_nation_regionkey ON nation (n_regionkey ASC);",
                "CREATE INDEX pk_region_regionkey ON region (r_regionkey ASC);",
                "CREATE INDEX pk_part_partkey ON part (p_partkey ASC);",
                "CREATE INDEX pk_supplier_suppkey ON supplier (s_suppkey ASC);",
                "CREATE INDEX fk_supplier_nationkey ON supplier (s_nationkey ASC);",
                "CREATE INDEX pk_partsupp_partkey_suppkey ON partsupp (ps_partkey ASC, ps_suppkey ASC);",
                "CREATE INDEX fk_partsupp_partkey ON partsupp (ps_partkey ASC);",
                "CREATE INDEX fk_partsupp_suppkey ON partsupp (ps_suppkey ASC);",
                "CREATE INDEX pk_customer_custkey ON customer (c_custkey ASC);",
                "CREATE INDEX fk_customer_nationkey ON customer (c_nationkey ASC);",
                "CREATE INDEX pk_orders_orderkey ON orders (o_orderkey ASC);",
                "CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC);",
                "CREATE INDEX pk_lineitem_orderkey_linenumber ON lineitem (l_orderkey ASC, l_linenumber ASC);",
                "CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC);",
                "CREATE INDEX fk_lineitem_partkey ON lineitem (l_partkey ASC);",
                "CREATE INDEX fk_lineitem_suppkey ON lineitem (l_suppkey ASC);",
                "CREATE INDEX fk_lineitem_partsuppkey ON lineitem (l_partkey ASC, l_suppkey ASC);",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
        )

        # Create index for each query
        for i in range(1, 23):
            runner.measure(
                "arrangement_formation",
                f"create_index_vq{i:02}",
                setup=["SELECT * FROM t;"],
                query=[
                    f"DROP INDEX IF EXISTS vq{i:02}_primary_idx CASCADE;",
                    f"CREATE DEFAULT INDEX ON vq{i:02};",
                    f"SELECT count(*) > 0 FROM vq{i:02};",
                ],
                after=[f"DROP INDEX IF EXISTS vq{i:02}_primary_idx CASCADE;"],
                size_of_index=f"vq{i:02}_primary_idx",
                repetitions=3,
            )


class AuctionScenario(ClusterScalingScenario):
    def name(self) -> str:
        return "auction"

    def materialize_views(self) -> list[str]:
        return ["auctions", "bids"]

    def drop(self) -> list[str]:
        return [
            "DROP MATERIALIZED VIEW IF EXISTS auctions CASCADE;",
            "DROP MATERIALIZED VIEW IF EXISTS bids CASCADE;",
            "DROP VIEW IF EXISTS auctions_core CASCADE;",
            "DROP VIEW IF EXISTS random CASCADE;",
            "DROP VIEW IF EXISTS moments CASCADE;",
            "DROP VIEW IF EXISTS seconds CASCADE;",
            "DROP VIEW IF EXISTS minutes CASCADE;",
            "DROP VIEW IF EXISTS hours CASCADE;",
            "DROP VIEW IF EXISTS days CASCADE;",
            "DROP VIEW IF EXISTS years CASCADE;",
            "DROP VIEW IF EXISTS items CASCADE;",
            "DROP TABLE IF EXISTS empty CASCADE;",
            f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;",
        ]

    def setup(self) -> list[str]:
        return [
            f"DROP CLUSTER IF EXISTS {self.lg_cluster} CASCADE;",
            f"CREATE CLUSTER {self.lg_cluster} SIZE '{self.replica_size}';",
            f"SET cluster = '{self.lg_cluster}';",
            "CREATE TABLE empty (e TIMESTAMP);",
            """
            -- Supporting view to translate ids into text.
            CREATE VIEW items (id, item) AS VALUES
                (0, 'Signed Memorabilia'),
                (1, 'City Bar Crawl'),
                (2, 'Best Pizza in Town'),
                (3, 'Gift Basket'),
                (4, 'Custom Art');""",
            f"""
            -- Each year-long interval of interest
            CREATE VIEW years AS
            SELECT *
            FROM generate_series(
                '1970-01-01 00:00:00+00',
                '2099-01-01 00:00:00+00',
                '1 year') year
            WHERE mz_now() BETWEEN year AND year + '1 year' + '{self.scale} day';
            """,
            f"""
            -- Each day-long interval of interest
            CREATE VIEW days AS
            SELECT * FROM (
                SELECT generate_series(year, year + '1 year' - '1 day'::interval, '1 day') as day
                FROM years
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN day AND day + '1 day' + '{self.scale} day';
            """,
            f"""
            -- Each hour-long interval of interest
            CREATE VIEW hours AS
            SELECT * FROM (
                SELECT generate_series(day, day + '1 day' - '1 hour'::interval, '1 hour') as hour
                FROM days
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN hour AND hour + '1 hour' + '{self.scale} day';
            """,
            f"""
            -- Each minute-long interval of interest
            CREATE VIEW minutes AS
            SELECT * FROM (
                SELECT generate_series(hour, hour + '1 hour' - '1 minute'::interval, '1 minute') AS minute
                FROM hours
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN minute AND minute + '1 minute' + '{self.scale} day';
            """,
            f"""
            -- Any second-long interval of interest
            CREATE VIEW seconds AS
            SELECT * FROM (
                SELECT generate_series(minute, minute + '1 minute' - '1 second'::interval, '1 second') as second
                FROM minutes
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN second AND second + '1 second' + '{self.scale} day';
            """,
            # Indexes are important to ensure we expand intervals carefully.
            "CREATE DEFAULT INDEX ON years;",
            "CREATE DEFAULT INDEX ON days;",
            "CREATE DEFAULT INDEX ON hours;",
            "CREATE DEFAULT INDEX ON minutes;",
            "CREATE DEFAULT INDEX ON seconds;",
            f"""
            -- The final view we'll want to use.
            CREATE VIEW moments AS
            SELECT second AS moment FROM seconds
            WHERE mz_now() >= second
              AND mz_now() < second + '{self.scale} day';
            """,
            """
            -- Extract pseudorandom bytes from each moment.
            CREATE VIEW random AS
            SELECT moment, digest(moment::text, 'md5') as random
            FROM moments;
            """,
            """
            -- Present as auction
            CREATE VIEW auctions_core AS
            SELECT
                moment,
                random,
                get_byte(random, 0) +
                get_byte(random, 1) * 256 +
                get_byte(random, 2) * 65536 as id,
                get_byte(random, 3) +
                get_byte(random, 4) * 256 as seller,
                get_byte(random, 5) as item,
                -- Have each auction expire after up to 256 minutes.
                moment + get_byte(random, 6) * '1 minutes'::interval as end_time
            FROM random;
            """,
            """
            -- Refine and materialize auction data.
            CREATE MATERIALIZED VIEW auctions AS
            SELECT auctions_core.id, seller, items.item, end_time
            FROM auctions_core, items
            WHERE auctions_core.item %% 5 = items.id;
            """,
            """
            -- Create and materialize bid data.
            CREATE MATERIALIZED VIEW bids AS
            -- Establish per-bid records and randomness.
            WITH prework AS (
                SELECT
                    id AS auction_id,
                    moment as auction_start,
                    end_time as auction_end,
                    digest(random::text || generate_series(1, get_byte(random, 5))::text, 'md5') as random
                FROM auctions_core
            )
            SELECT
                get_byte(random, 0) +
                get_byte(random, 1) * 256 +
                get_byte(random, 2) * 65536 as id,
                get_byte(random, 3) +
                get_byte(random, 4) * 256 AS buyer,
                auction_id,
                get_byte(random, 5)::numeric AS amount,
                auction_start + get_byte(random, 6) * '1 minutes'::interval as bid_time
            FROM prework;
            """,
        ]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            size_of_index="bids_id_idx",
            setup=[
                "SELECT * FROM t;",
                "DROP INDEX IF EXISTS bids_id_idx CASCADE",
            ],
            query=[
                "CREATE INDEX bids_id_idx ON bids(id);",
                "SELECT count(*) > 0 FROM bids WHERE id = 123123123;",
            ],
        )

        runner.measure(
            "arrangement_formation",
            "create_index_foreign_key",
            size_of_index="bids_id_idx",
            setup=["DROP INDEX IF EXISTS bids_auction_id_idx CASCADE"],
            query=[
                "CREATE INDEX bids_auction_id_idx ON bids(auction_id);",
                "SELECT count(*) > 0 FROM bids WHERE auction_id = 123123123;",
            ],
        )

        time.sleep(3)

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_slow_path",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_fast_path",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT * FROM bids WHERE id = 123412341234",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_slow_path",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM bids WHERE amount = 123123123) SELECT * FROM data, t;",
            ],
            repetitions=3,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_fast_path",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT * FROM bids WHERE amount = 123123123;",
            ],
            repetitions=3,
        )

        # Primitive operators
        runner.measure(
            "primitive_operators",
            "bids_max",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT max(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_max_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                "DECLARE a CURSOR FOR SUBSCRIBE (SELECT max(amount) FROM bids);",
                "FETCH 1 a;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_sum",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT sum(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_sum_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                "DECLARE b CURSOR FOR SUBSCRIBE (SELECT sum(amount) FROM bids);",
                "FETCH 1 b;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_count",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT count(1) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_count_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                "DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(1) FROM bids);",
                "FETCH 1 c;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_basic_list_agg",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT auction_id, list_agg(amount) FROM bids GROUP BY auction_id LIMIT 1;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_basic_list_agg_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                # We need to limit the amount of data we retrieve to avoid
                # stalling the gRPC connection.
                """
                DECLARE d CURSOR FOR SUBSCRIBE (
                    SELECT
                        auction_id, list_agg(amount)
                    FROM
                        bids
                    GROUP BY
                        auction_id
                    HAVING
                        list_length(list_agg(amount)) + auction_id < 10000
                );""",
                "FETCH 1 d;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "join",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT * FROM bids, auctions WHERE bids.auction_id = auctions.id LIMIT 1;",
            ],
            repetitions=3,
        )

        # Composite operators
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                """
                SELECT
                    id, count(amount), max(amount), sum(amount), min(amount)
                FROM bids
                GROUP BY id
                LIMIT 1;
                """,
            ],
            repetitions=3,
        )
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                """
                DECLARE e CURSOR FOR SUBSCRIBE (
                    SELECT
                        id, count(amount), max(amount), sum(amount), min(amount)
                    FROM bids
                    GROUP BY id
                    LIMIT 1
                )
                """,
                "FETCH 1 e;",
                "ROLLBACK;",
                """
                """,
            ],
            repetitions=3,
        )

        runner.measure(
            "composite_operators",
            "join_max",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT auction_id, item, MAX(amount) FROM bids, auctions WHERE bids.auction_id = auctions.id GROUP BY auction_id, item HAVING auction_id + MAX(amount)::int4 < 1000;",
            ],
            repetitions=3,
        )

        # Restart index
        runner.measure(
            "peek_serving",
            "index_restart",
            size_of_index="bids_id_idx",
            setup=[f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 0);"],
            query=[
                f"ALTER CLUSTER {runner.cluster} SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
            setup_delay=2,
        )


class QpsEnvdStrongScalingScenario(ClusterScalingScenario):

    def name(self) -> str:
        return SCENARIO_QPS_ENVD_STRONG_SCALING

    def setup(self) -> list[str]:
        return []

    def run(self, runner: ScenarioRunner) -> None:
        runner.measure_dbbench(
            category="peek_qps",
            name="dbbench_256_conns",
            setup=[
                "create view if not exists gen_view as select generate_series as x from generate_series(1, 10)",
                "create default index on gen_view",
                "select * from gen_view",  # Wait for hydration
            ],
            query=[
                "select * from gen_view",
            ],
            after=[
                "drop view gen_view cascade",
            ],
            duration="40s",
            concurrency=256,
        )

        runner.measure_dbbench(
            category="peek_qps",
            name="dbbench_512_conns",
            setup=[
                "create view if not exists gen_view as select generate_series as x from generate_series(1, 10)",
                "create default index on gen_view",
                "select * from gen_view",  # Wait for hydration
            ],
            query=[
                "select * from gen_view",
            ],
            after=[
                "drop view gen_view cascade",
            ],
            duration="40s",
            concurrency=512,
        )

        # TODO: Add more scenarios as the QPS/CPS work progresses:
        # - different connection counts
        # - distribute queries across more clusters;
        #   see manual test results with multiple clusters here:
        #   https://docs.google.com/presentation/d/1bIyTWaRiyEqBXFxoxpHwWSywztSW1jRw_JP3M-Zj_6A/edit?slide=id.g39de8b7440c_0_86#slide=id.g39de8b7440c_0_86
        # - explicit transactions (which are currently super slow)
        # - (slow-path queries are kinda expected to be slow, so it's not so important to measure them)
        # - I think dbbench uses the "Simple Query Protocol" by default. We might want to also measure the
        #   "Extended Query Protocol" / prepared statements.
        # - Lookups in a large index, especially on a larger replica.
        # - Larger result sets.
        #
        # We'll also want to measure latency, including tail latency.


class CopyFromStdinEnvdStrongScalingScenario(ClusterScalingScenario):
    """Measure COPY FROM STDIN throughput as envd CPU count scales.

    Uses psycopg's COPY protocol to send pre-generated tab-delimited data,
    exercising the parallel decode + persist pipeline in environmentd.

    Note that this doesn't scale well in Cloud at the moment, but scales well
    locally. We might be bound by the incoming postgres connection, S3, or CRDB
    throughput.
    """

    NUM_ROWS = 100_000_000
    NUM_COLS = 4  # (int, text, int, text)
    REPETITIONS = 1  # Pretty slow and resource-intensive

    def name(self) -> str:
        return SCENARIO_COPY_FROM_STDIN_ENVD_STRONG_SCALING

    def setup(self) -> list[str]:
        return []

    # Avoid Python from going OoM
    CHUNK_SIZE = 100_000

    def run(self, runner: ScenarioRunner) -> None:
        # Pre-generate one chunk and reuse it for all writes.
        chunk = "".join(
            f"{i}\thello world\t{i * 2}\tsome text value here\n"
            for i in range(self.CHUNK_SIZE)
        )
        num_chunks = self.NUM_ROWS // self.CHUNK_SIZE

        for repetition in range(self.REPETITIONS):

            def inner() -> None:
                with runner.connection as cur:
                    cur.execute("DROP TABLE IF EXISTS copy_t")
                    cur.execute(
                        "CREATE TABLE copy_t (f1 INTEGER, f2 TEXT, f3 INTEGER, f4 TEXT)"
                    )

                start_time = time.time()
                with runner.connection as cur:
                    with cur.copy("COPY copy_t FROM STDIN") as copy:
                        for _ in range(num_chunks):
                            copy.write(chunk)
                end_time = time.time()

                elapsed = end_time - start_time
                rows_per_sec = self.NUM_ROWS / elapsed
                print(
                    f"    COPY FROM: {self.NUM_ROWS} rows in {elapsed:.2f}s "
                    f"({rows_per_sec:.0f} rows/s)"
                )
                runner.add_result(
                    "copy_from",
                    "copy_from_stdin_1m_rows",
                    repetition,
                    None,
                    time_ms=int(elapsed * 1000),
                    qps=rows_per_sec,
                )

                with runner.connection as cur:
                    cur.execute("DROP TABLE IF EXISTS copy_t")

            runner.connection.retryable(inner)


class SourceIngestionScenario(ClusterScalingScenario):
    def name(self) -> str:
        return "source_ingestion"

    def setup(self) -> list[str]:
        # External setup was done once:
        # Postgres (RDS)
        # create user materialize password '...';
        # create table tbl (customer_id int, region_id int, customer_name text, customer_email text, customer_phone text);
        # \set N 50000000
        # set synchronous_commit = off;
        # insert into tbl
        # select
        #   gs::int as customer_id,
        #   (1 + (random()*999)::int) as region_id,
        #   'Customer ' || gs as customer_name,
        #   'customer' || gs || '@example.com' as customer_email,
        #   lpad((random()*10000000000)::bigint::text, 10, '0') as customer_phone
        # from generate_series(1, :N) as gs;
        # analyze tbl;

        # MySQL (RDS)
        # CREATE USER 'materialize'@'%' IDENTIFIED BY '...';
        # create table tbl (customer_id int, region_id int, customer_name text, customer_email text, customer_phone text);
        # INSERT INTO tbl
        # SELECT
        #   n AS customer_id,
        #   1 + (RAND() * 999) AS region_id,
        #   CONCAT('Customer ', n) AS customer_name,
        #   CONCAT('customer', n, '@example.com') AS customer_email,
        #   LPAD(FLOOR(RAND() * 10000000000), 10, '0') AS customer_phone
        # FROM (
        #   SELECT (a.n + b.n*10 + c.n*100 + d.n*1000 + e.n*10000 + f.n*100000 + g.n*1000000 + h.n*10000000) AS n
        #   FROM (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) f
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) g
        #   CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        #         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) h
        # ) nums
        # WHERE n BETWEEN 1 AND 50000000;

        # Kafka (Confluent)
        # node /usr/local/bin/datagen -f avro -n 50000000 -w 0 -p qa_cluster_spec_sheet -s table.json
        # cat table.json
        # [
        #     {
        #         "_meta": {
        #             "topic": "table",
        #             "key": "customer_id"
        #         },
        #         "customer_id": "iteration.index",
        #         "region_id": "faker.number.int({ min: 1, max: 4 })",
        #         "customer_name": "faker.person.fullName()",
        #         "customer_email": "faker.internet.email()",
        #         "customer_phone": "faker.phone.number()"
        #     }
        # ]

        postgres_hostname = os.environ[
            "QA_CLUSTER_SPEC_SHEET_POSTGRES_HOSTNAME"
        ].replace("%", "%%")
        postgres_password = os.environ[
            "QA_CLUSTER_SPEC_SHEET_POSTGRES_PASSWORD"
        ].replace("%", "%%")
        mysql_hostname = os.environ["QA_CLUSTER_SPEC_SHEET_MYSQL_HOSTNAME"].replace(
            "%", "%%"
        )
        mysql_password = os.environ["QA_CLUSTER_SPEC_SHEET_MYSQL_PASSWORD"].replace(
            "%", "%%"
        )
        kafka_username = os.environ["CONFLUENT_CLOUD_QA_CANARY_KAFKA_USERNAME"].replace(
            "%", "%%"
        )
        kafka_password = os.environ["CONFLUENT_CLOUD_QA_CANARY_KAFKA_PASSWORD"].replace(
            "%", "%%"
        )
        csr_username = os.environ["CONFLUENT_CLOUD_QA_CANARY_CSR_USERNAME"].replace(
            "%", "%%"
        )
        csr_password = os.environ["CONFLUENT_CLOUD_QA_CANARY_CSR_PASSWORD"].replace(
            "%", "%%"
        )
        return [
            "DROP CONNECTION IF EXISTS pg_conn CASCADE;",
            "DROP CONNECTION IF EXISTS mysql_conn CASCADE;",
            "DROP CONNECTION IF EXISTS kafka_conn CASCADE;",
            "DROP CONNECTION IF EXISTS csr_conn CASCADE;",
            f"CREATE SECRET IF NOT EXISTS pgpass AS '{postgres_password}';",
            f"CREATE CONNECTION pg_conn TO postgres (HOST '{postgres_hostname}', PORT 5432, USER materialize, PASSWORD SECRET pgpass, SSL MODE 'require', DATABASE 'postgres');",
            f"CREATE SECRET IF NOT EXISTS mysqlpass AS '{mysql_password}';",
            f"CREATE CONNECTION mysql_conn TO MYSQL (HOST '{mysql_hostname}', PORT 3306, USER 'materialize', PASSWORD SECRET mysqlpass, SSL MODE REQUIRED);",
            f"CREATE SECRET IF NOT EXISTS kafka_username AS '{kafka_username}';",
            f"CREATE SECRET IF NOT EXISTS kafka_password AS '{kafka_password}';",
            "CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET kafka_username, SASL PASSWORD = SECRET kafka_password);",
            f"CREATE SECRET IF NOT EXISTS csr_username AS '{csr_username}';",
            f"CREATE SECRET IF NOT EXISTS csr_password AS '{csr_password}';",
            "CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (URL 'https://psrc-e0919.us-east-2.aws.confluent.cloud', USERNAME = SECRET csr_username, PASSWORD = SECRET csr_password);",
        ]

    def run(self, runner: ScenarioRunner) -> None:
        runner.measure(
            "hydration",
            "postgres",
            setup=["DROP SOURCE IF EXISTS pg_source CASCADE;"],
            query=[
                f"CREATE SOURCE pg_source IN CLUSTER {runner.cluster} FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source') FOR TABLES (tbl AS pg_table);",
                # TODO: Use `CREATE TABLE FROM SOURCE` once supported in prod.
                # "CREATE TABLE pg_table FROM SOURCE qa_cluster_spec_sheet_pg_source (REFERENCE tbl);",
                ("SELECT count(*) FROM pg_table;", [(50000000,)]),
            ],
        )

        runner.measure(
            "hydration",
            "mysql",
            setup=["DROP SOURCE IF EXISTS mysql_source CASCADE;"],
            query=[
                f"CREATE SOURCE mysql_source IN CLUSTER {runner.cluster} FROM MYSQL CONNECTION mysql_conn FOR TABLES (admin.tbl AS mysql_table);",
                # TODO: Use `CREATE TABLE FROM SOURCE` once supported in prod.
                # "CREATE TABLE mysql_table FROM SOURCE mysql_source (REFERENCE admin.tbl);",
                ("SELECT count(*) FROM mysql_table;", [(50000000,)]),
            ],
        )

        runner.measure(
            "hydration",
            "kafka",
            setup=["DROP SOURCE IF EXISTS kafka_table CASCADE;"],
            query=[
                f"CREATE SOURCE kafka_table IN CLUSTER {runner.cluster} FROM KAFKA CONNECTION kafka_conn (TOPIC 'qa_cluster_spec_sheet_table') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE NONE;",
                # TODO: Use `CREATE TABLE FROM SOURCE` once supported in prod.
                # "CREATE TABLE kafka_table FROM SOURCE kafka_source;",
                ("SELECT count(*) FROM kafka_table;", [(50000000,)]),
            ],
        )


class EnvdObjectsScalabilityScenario(ABC):
    """Workload base for the envd_objects_scalability family.

    Wrapped by `EnvdObjectsSweep`, which drives the N-loop and calls
    `init` / `add_objects` / `run` / `teardown`. `runner.scale` is
    updated per step so result rows carry N.
    """

    VERSION: str = "1.0.0"
    REPETITIONS: int = 10

    PAD_SCHEMA: str = "pad_schema"

    def __init__(self) -> None:
        self._current_n = 0

    @abstractmethod
    def name(self) -> str: ...

    def init(self, runner: ScenarioRunner) -> None:
        runner.run_query(f"DROP SCHEMA IF EXISTS {self.PAD_SCHEMA} CASCADE")
        runner.run_query(f"CREATE SCHEMA {self.PAD_SCHEMA}")

    @abstractmethod
    def add_objects(self, runner: ScenarioRunner, target_n: int) -> None: ...

    def teardown(self, runner: ScenarioRunner) -> None:
        _best_effort_drop(
            runner,
            f"DROP SCHEMA IF EXISTS {self.PAD_SCHEMA} CASCADE",
            self.PAD_SCHEMA,
        )

    def run(self, runner: ScenarioRunner) -> None:
        # Measure DDL (CREATE TABLE) latency. Each repetition creates a fresh
        # `m_tmp` table and drops it afterwards; only the CREATE is timed.
        runner.measure(
            "adapter_ddl_latency",
            "create_table",
            setup=["DROP TABLE IF EXISTS m_tmp CASCADE"],
            query=["CREATE TABLE m_tmp (a int)"],
            after=["DROP TABLE m_tmp"],
            repetitions=self.REPETITIONS,
        )

        # Measure simple peek latency against a 1-row table on the fixed
        # measurement cluster `c`.
        runner.measure(
            "adapter_peek_latency",
            "select_one_row",
            setup=[],
            query=["SELECT * FROM t"],
            repetitions=self.REPETITIONS,
        )


def _bulk_run(runner: ScenarioRunner, statements: list[str], log_label: str) -> None:
    """Execute a list of DDL statements one-by-one with progress logging.

    Callers must pass idempotent statements: ``ConnectionHandler.retryable``
    may resend a statement that the server already committed before losing
    the response (e.g. TLS EOF against staging).
    """
    total = len(statements)
    if total == 0:
        return
    print(f"  {log_label}: {total} statements")
    log_every = max(1, total // 10)

    def execute_one(stmt: str) -> None:
        with runner.connection as cur:
            cur.execute(stmt.encode())

    start = time.time()
    for i, stmt in enumerate(statements, 1):
        runner.connection.retryable(lambda s=stmt: execute_one(s))
        if i % log_every == 0 or i == total:
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0
            print(f"  {log_label}: {i}/{total} ({rate:.1f}/s)")


# CSV schemas used by `workflow_default`. The cluster-focused schema is the
# default; envd_qps_scalability uses its own QPS-focused shape;
# envd_objects_scalability reuses CLUSTER_FIELDNAMES verbatim and
# cluster_object_limits extends it with `healthy` and `failure_mode` columns.
CLUSTER_FIELDNAMES: list[str] = [
    "scenario",
    "scenario_version",
    "scale",
    "mode",
    "category",
    "test_name",
    "cluster_size",
    "repetition",
    "size_bytes",
    "time_ms",
]
ENVD_FIELDNAMES: list[str] = [
    "scenario",
    "scenario_version",
    "scale",
    "mode",
    "category",
    "test_name",
    "envd_cpus",
    "repetition",
    "qps",
]


def _make_csv_writer(file: TextIO, fieldnames: list[str]) -> csv.DictWriter:
    writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    return writer


def _recreate_cluster_c(
    runner: ScenarioRunner,
    replica_size: str,
    smoke_test: bool = False,
    skip_if_unavailable_label: str | None = None,
) -> bool:
    """Drop+recreate cluster ``c`` at the given size and SET cluster='c'.

    Returns ``True`` if the cluster is now ready, ``False`` only when
    ``skip_if_unavailable_label`` is set and ``CREATE CLUSTER`` failed with a
    server-side error indicating the size isn't available (e.g. missing from
    ``mz_cluster_replica_sizes``, or allocating it would exceed
    ``max_credit_consumption_rate``). ``OperationalError`` always propagates
    so connection-level failures aren't swallowed.

    ``smoke_test=True`` issues ``SELECT * FROM t`` after the SET as a
    cluster-startup sanity check before the scenario runs (relies on
    `_prepare_probe_table` having already created ``t``).
    """
    cluster = runner.cluster
    runner.run_query(f"DROP CLUSTER IF EXISTS {cluster} CASCADE")
    try:
        runner.run_query(f"CREATE CLUSTER {cluster} SIZE '{replica_size}'")
    except psycopg.errors.DatabaseError as e:
        if skip_if_unavailable_label is None or isinstance(e, OperationalError):
            raise
        print(
            f"^^^ +++ {skip_if_unavailable_label}: cluster size "
            f"'{replica_size}' unavailable on this target "
            f"({type(e).__name__}: {str(e).strip()}); skipping."
        )
        return False
    runner.run_query(f"SET cluster = '{cluster}'")
    # Pin the measurement cluster as the connection default so a mid-measure
    # reconnect resumes on it rather than the bootstrap quickstart cluster.
    runner.connection.set_default_cluster(cluster)
    if smoke_test:
        runner.run_query("SELECT * FROM t")
    return True


def _prepare_probe_table(runner: ScenarioRunner) -> None:
    """(Re)create the tiny one-row table ``t`` used as a probe / smoke-test
    target by the strong / envd_strong_scaling / envd_objects_scalability
    runners."""
    runner.run_query("DROP TABLE IF EXISTS t CASCADE")
    runner.run_query("CREATE TABLE t (a int)")
    runner.run_query("INSERT INTO t VALUES (1)")


def _best_effort_drop(runner: ScenarioRunner, stmt: str, label: str) -> None:
    """Run a DROP statement, swallowing any error as a WARNING.

    Used in teardown paths where a previous failure may have left only
    partial state and we want subsequent cleanup statements to still run.
    """
    try:
        runner.run_query(stmt)
    except Exception as e:
        print(f"WARNING: failed to drop {label}: {e}")


def _extend_incremental(
    runner: ScenarioRunner,
    *,
    current_n: int,
    target_n: int,
    templates_for: Callable[[int], Iterable[str]],
    label: str,
) -> int:
    """Idempotently create objects ``i ∈ (current_n, target_n]`` and return the
    new ``current_n``.

    Statements from ``templates_for`` must be idempotent
    (``CREATE ... IF NOT EXISTS``) so retries in ``_bulk_run`` are safe. Used
    by every scenario that builds a catalog of N objects incrementally
    (envd_objects_scalability_*, cluster_object_limits_*).
    """
    if target_n <= current_n:
        return current_n
    stmts = [s for i in range(current_n + 1, target_n + 1) for s in templates_for(i)]
    _bulk_run(runner, stmts, f"create {label} {current_n + 1}..{target_n}")
    return target_n


def _shrink_incremental(
    runner: ScenarioRunner,
    *,
    current_n: int,
    target_n: int,
    drop_for: Callable[[int], str],
    label: str,
) -> int:
    """Drop objects ``i ∈ (target_n, current_n]`` and return the new
    ``current_n``. Inverse of ``_extend_incremental``."""
    if target_n >= current_n:
        return current_n
    stmts = [drop_for(i) for i in range(target_n + 1, current_n + 1)]
    _bulk_run(runner, stmts, f"drop {label} {target_n + 1}..{current_n}")
    return target_n


@dataclass(frozen=True)
class ScalePoint:
    """A single point in a scenario's sweep.

    Each field is meaningful only for sweeps that vary it; the rest are
    ``None``. The scenario interprets its fields in ``apply()`` to set up
    the world (recreate the cluster, reconfigure envd, add objects). The
    driver doesn't look at any field except ``label``.
    """

    label: str
    # Replica size string, e.g. "100cc". Used in `CREATE CLUSTER c SIZE ...`.
    cluster_size: str | None = None
    # Integer index from `REPLICA_SCALES` (1, 2, 4, ...). Used by weak
    # scaling to multiply the workload's input scale per cluster step.
    replica_scale: int | None = None
    # environmentd CPU cores for the EnvdCpuSweep.
    envd_cpus: int | None = None
    # Number of catalog objects for envd_objects_scalability /
    # cluster_object_limits sweeps.
    n: int | None = None


@dataclass(frozen=True)
class Namespace:
    """Per-worker isolation for a parallel job.

    The database isolates relations / indexes (which are database-scoped);
    the cluster names are globally unique because clusters are *not*
    database-scoped in Materialize. ``quickstart`` is a throwaway cluster the
    worker uses for ad-hoc queries before its measurement cluster ``cluster``
    exists."""

    database: str
    cluster: str
    lg_cluster: str
    quickstart: str
    log_prefix: str


class Scenario(ABC):
    """A benchmark scenario: a sweep over scale points plus per-point
    measurement.

    Concrete subclasses fall into two shapes:
      - Sweep wrappers (`StrongScalingSweep`, `WeakScalingSweep`,
        `EnvdCpuSweep`, `EnvdObjectsSweep`) wrap an inner workload
        (`ClusterScalingScenario` / `EnvdObjectsScalabilityScenario`) and
        delegate per-point measurement to it.
      - `ClusterObjectLimitsScenario` implements the protocol directly
        because its measurement is an adaptive inner walk over N with
        bisection.

    Lifecycle driven by `run_scenario`:
      1. `prepare(runner)` once.
      2. For each `point` in `scale_points(target, max_scale)`:
         a. `apply(runner, point) -> bool`. False = skip this point.
         b. `measure(runner, point)`.
         c. `cleanup_point(runner, point)` in a finally.
      3. `teardown(runner)` once.
    """

    VERSION: str = "1.0.0"

    @abstractmethod
    def name(self) -> str: ...
    @abstractmethod
    def mode(self) -> str: ...
    @abstractmethod
    def stream_key(self) -> str: ...
    @abstractmethod
    def scale_points(
        self, target: "BenchTarget", max_scale: int
    ) -> Iterable[ScalePoint]: ...
    @abstractmethod
    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool: ...
    @abstractmethod
    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None: ...

    def prepare(self, runner: ScenarioRunner) -> None:
        pass

    def cleanup_point(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        pass

    def teardown(self, runner: ScenarioRunner) -> None:
        pass

    # --- Intra-region parallelism hooks (see run_parallel) ---

    def parallel_safe(self) -> bool:
        """Whether this scenario may run concurrently with others in the same
        region. False for scenarios with region-global side effects — e.g.
        `EnvdCpuSweep` reconfigures environmentd's CPU allocation (a region
        restart), and envd-objects / cluster-object-limits push global
        catalog/limit state — which must run in isolation."""
        return False

    def split_by_point(self) -> bool:
        """Whether each scale point may run as its own independent parallel
        job (own database + clusters + data). Only meaningful when
        `parallel_safe()`."""
        return False

    def apply_namespace(self, ns: "Namespace") -> None:
        """Rebind the scenario's cluster names to a per-worker namespace so
        concurrent jobs don't collide on the global cluster namespace. The
        per-worker database isolates relations/indexes. Default: no-op."""
        pass

    def peak_credits(self, points: list[ScalePoint], target: "BenchTarget") -> float:
        """Estimated peak credits/hour this job holds, for the scheduler's
        credit budget. Conservative over-estimates are safe (they just reduce
        concurrency)."""
        return 0.0


class EnvdObjectsScalabilityTablesScenario(EnvdObjectsScalabilityScenario):
    """N empty tables in the catalog. No controller load; pure catalog/adapter."""

    def name(self) -> str:
        return SCENARIO_ENVD_OBJECTS_SCALABILITY_TABLES

    def add_objects(self, runner: ScenarioRunner, target_n: int) -> None:
        self._current_n = _extend_incremental(
            runner,
            current_n=self._current_n,
            target_n=target_n,
            templates_for=lambda i: (
                f"CREATE TABLE IF NOT EXISTS {self.PAD_SCHEMA}.pad_t_{i} (a int, b text)",
            ),
            label="tables",
        )


class EnvdObjectsScalabilityMvsScenario(EnvdObjectsScalabilityScenario):
    """N materialized views in the catalog, spread across pad clusters.

    To bound per-cluster dataflow count, MVs are spread across multiple
    single-replica pad clusters (`pad_c_0`, `pad_c_1`, ...), at most
    `ENVD_OBJECTS_SCALABILITY_MVS_PER_CLUSTER` MVs per cluster.

    Each MV is a trivial transformation of a single 1-row base table, with
    the predicate parameterised on the index `i` so MVs are structurally
    distinct (forcing separate dataflows).

    Each MV reads directly from `base_t` — i.e. one persist source per
    MV — as opposed to a topology where the table feeds one index, and
    many MVs read from that index.
    """

    MVS_PER_CLUSTER: int = ENVD_OBJECTS_SCALABILITY_MVS_PER_CLUSTER
    BASE_TABLE: str = "base_t"

    def __init__(self, pad_replica_size: str) -> None:
        super().__init__()
        self._pad_replica_size = pad_replica_size
        self._pad_clusters_created = 0

    def name(self) -> str:
        return SCENARIO_ENVD_OBJECTS_SCALABILITY_MVS

    def init(self, runner: ScenarioRunner) -> None:
        # Drop any leftover pad clusters from a previous failed run.
        # `%%` escapes the LIKE wildcard from psycopg's `%` placeholder syntax.
        existing = runner.run_query(
            "SELECT name FROM mz_clusters WHERE name LIKE 'pad_c_%%'",
            fetch=True,
        )
        for row in existing or []:
            runner.run_query(f"DROP CLUSTER IF EXISTS {row[0]} CASCADE")

        super().init(runner)
        runner.run_query(
            f"CREATE TABLE {self.PAD_SCHEMA}.{self.BASE_TABLE} (id int, val text)"
        )
        runner.run_query(
            f"INSERT INTO {self.PAD_SCHEMA}.{self.BASE_TABLE} VALUES (1, 'x')"
        )

    def _ensure_pad_cluster(self, runner: ScenarioRunner, cluster_idx: int) -> None:
        while self._pad_clusters_created <= cluster_idx:
            k = self._pad_clusters_created
            print(f"  creating pad cluster pad_c_{k} (size {self._pad_replica_size})")
            runner.run_query(f"DROP CLUSTER IF EXISTS pad_c_{k} CASCADE")
            runner.run_query(
                f"CREATE CLUSTER pad_c_{k} SIZE '{self._pad_replica_size}'"
            )
            self._pad_clusters_created += 1

    def add_objects(self, runner: ScenarioRunner, target_n: int) -> None:
        # Build one pad cluster's worth at a time: create the pad cluster,
        # then fill it up to MVS_PER_CLUSTER MVs before moving on.
        while self._current_n < target_n:
            cluster_idx = self._current_n // self.MVS_PER_CLUSTER
            self._ensure_pad_cluster(runner, cluster_idx)
            cluster_end = min(target_n, (cluster_idx + 1) * self.MVS_PER_CLUSTER)
            self._current_n = _extend_incremental(
                runner,
                current_n=self._current_n,
                target_n=cluster_end,
                templates_for=lambda i, c=cluster_idx: (
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.PAD_SCHEMA}.pad_mv_{i} "
                    f"IN CLUSTER pad_c_{c} "
                    f"AS SELECT id, val FROM {self.PAD_SCHEMA}.{self.BASE_TABLE} "
                    f"WHERE id < {i}",
                ),
                label=f"MVs on pad_c_{cluster_idx}",
            )

    def teardown(self, runner: ScenarioRunner) -> None:
        # Drop the schema first so MVs (catalog entries) are removed; then
        # drop the now-quiescent pad clusters.
        super().teardown(runner)
        for k in range(self._pad_clusters_created):
            _best_effort_drop(
                runner, f"DROP CLUSTER IF EXISTS pad_c_{k} CASCADE", f"pad_c_{k}"
            )
        self._pad_clusters_created = 0


@dataclass(frozen=True)
class ClusterObjectLimitsKind:
    """Per-materialization-type knobs for a ClusterObjectLimitsScenario.

    Each materialization (one "test object") is created from one or more SQL
    statements (indexes need a view + a default index; MVs need a single
    CREATE MATERIALIZED VIEW). Statement templates take ``{schema}``,
    ``{base}``, and ``{i}`` placeholders; the ``WHERE id < {i}`` predicate
    makes each view/MV structurally distinct so we get separate dataflows.
    """

    scenario_name: str
    create_templates: tuple[str, ...]
    drop_template: str
    label: str
    # Join clause linking mz_materialization_lag (aliased `l`) to the
    # cluster name, filtered to cluster `c`. Differs because cluster_id is
    # carried on mz_indexes vs mz_materialized_views.
    lag_filter: str
    # Sibling of `lag_filter` for mz_hydration_statuses (aliased `hs`). Used
    # to count how many of our test objects on `c` are hydrated before we
    # start sampling freshness.
    hydration_filter: str
    # Optional statements run once per cluster-size iteration (after the
    # cluster, schema, and base table exist) to set up extra topology that
    # the per-i `create_templates` then build on top of. Used by the
    # `indexes_from_index` variant to pre-create a single root index that
    # the N test indexes share. Statements take `{schema}` and `{base}`
    # placeholders.
    setup_statements: tuple[str, ...] = ()


CLUSTER_OBJECT_LIMITS_INDEXES_FROM_PERSIST_SOURCES_KIND = ClusterObjectLimitsKind(
    scenario_name=SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_PERSIST_SOURCES,
    # Each (view, default index) pair is one independent index dataflow on
    # `c`, reading directly from `base_t`'s persist shard — i.e. one
    # persist source per test object. DROP VIEW CASCADE removes the
    # dependent default index too. Compare against
    # `cluster_object_limits_indexes_from_index`, which inserts a single
    # root index in front so the N test indexes share one in-memory
    # arrangement instead of N persist sources.
    create_templates=(
        "CREATE VIEW IF NOT EXISTS {schema}.v_{i} AS "
        "SELECT id, val FROM {schema}.{base} WHERE id < {i}",
        "CREATE DEFAULT INDEX IF NOT EXISTS IN CLUSTER c ON {schema}.v_{i}",
    ),
    drop_template="DROP VIEW IF EXISTS {schema}.v_{i} CASCADE",
    label="indexed views",
    lag_filter="""
    JOIN mz_catalog.mz_indexes idx ON l.object_id = idx.id
    JOIN mz_catalog.mz_clusters c ON idx.cluster_id = c.id
    WHERE c.name = 'c'
    """,
    hydration_filter="""
    JOIN mz_catalog.mz_indexes idx ON hs.object_id = idx.id
    JOIN mz_catalog.mz_clusters c ON idx.cluster_id = c.id
    WHERE c.name = 'c'
    """,
)

# Variant with one root index between `base_t` and the N test indexes:
# a single view `v_root` with an index on `c` sits in front of `base_t`,
# and the N test indexes read from `v_root` instead of `base_t`. The
# optimizer imports the root index's arrangement, so the whole topology
# has one persist source regardless of N. That isolates "how many
# compute-only dataflows can a cluster tick" from "how many persist
# sources can it pull in parallel" — the latter dominates
# `cluster_object_limits_indexes_from_persist_sources`. `base_t` is
# still on the data path so frontier ticks propagate through the root
# index into every test index.
#
# No analogous MV variant:
# `cluster_object_limits_mvs_from_persist_sources` already covers the
# persist-sink-breadth axis (each MV is its own sink), so a from-index
# version wouldn't measure something new.
CLUSTER_OBJECT_LIMITS_INDEXES_FROM_INDEX_KIND = ClusterObjectLimitsKind(
    scenario_name=SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_INDEX,
    setup_statements=(
        "CREATE VIEW IF NOT EXISTS {schema}.v_root AS "
        "SELECT id, val FROM {schema}.{base}",
        "CREATE DEFAULT INDEX v_root_primary_idx IN CLUSTER c ON {schema}.v_root",
    ),
    create_templates=(
        "CREATE VIEW IF NOT EXISTS {schema}.v_{i} AS "
        "SELECT id, val FROM {schema}.v_root WHERE id < {i}",
        "CREATE DEFAULT INDEX IF NOT EXISTS IN CLUSTER c ON {schema}.v_{i}",
    ),
    drop_template="DROP VIEW IF EXISTS {schema}.v_{i} CASCADE",
    label="indexed views (from root index)",
    # Exclude the shared root index from the freshness counts: it's
    # topology, not one of the N test objects.
    lag_filter="""
    JOIN mz_catalog.mz_indexes idx ON l.object_id = idx.id
    JOIN mz_catalog.mz_clusters c ON idx.cluster_id = c.id
    WHERE c.name = 'c' AND idx.name <> 'v_root_primary_idx'
    """,
    hydration_filter="""
    JOIN mz_catalog.mz_indexes idx ON hs.object_id = idx.id
    JOIN mz_catalog.mz_clusters c ON idx.cluster_id = c.id
    WHERE c.name = 'c' AND idx.name <> 'v_root_primary_idx'
    """,
)

CLUSTER_OBJECT_LIMITS_MVS_FROM_PERSIST_SOURCES_KIND = ClusterObjectLimitsKind(
    scenario_name=SCENARIO_CLUSTER_OBJECT_LIMITS_MVS_FROM_PERSIST_SOURCES,
    create_templates=(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.mv_{i} IN CLUSTER c "
        "AS SELECT id, val FROM {schema}.{base} WHERE id < {i}",
    ),
    drop_template="DROP MATERIALIZED VIEW IF EXISTS {schema}.mv_{i}",
    label="MVs",
    lag_filter="""
    JOIN mz_catalog.mz_materialized_views mv ON l.object_id = mv.id
    JOIN mz_catalog.mz_clusters c ON mv.cluster_id = c.id
    WHERE c.name = 'c'
    """,
    hydration_filter="""
    JOIN mz_catalog.mz_materialized_views mv ON hs.object_id = mv.id
    JOIN mz_catalog.mz_clusters c ON mv.cluster_id = c.id
    WHERE c.name = 'c'
    """,
)


class ClusterObjectLimitsScenario(Scenario):
    """
    Scenario for cluster_object_limits. Finds the maximum number of idle
    materializations (indexes or materialized views, picked via ``kind``)
    that one cluster can keep fresh.

    Structure:
      - A single one-row base table `base_t` (never updated) lives in
        ``PAD_SCHEMA``. Frontiers advance over wall-clock time, so even with
        no writes the cluster must keep ticking every materialization's
        write_frontier.
      - N structurally-distinct materializations on the measurement cluster
        ``c``, all reading directly from ``base_t`` — i.e. one persist
        source per materialization, as opposed to a topology where
        ``base_t`` feeds one index and the N materializations read from
        that. The materialization type (indexed view vs MV) and the
        lag-query join are carried by ``kind``.
      - The outer sweep is the cluster size: ``apply`` drops+recreates ``c``
        and ``PAD_SCHEMA`` for each size. ``measure`` walks an N-list,
        adding the delta of objects at each step and probing freshness via
        ``mz_internal.mz_materialization_lag``. On the first unhealthy probe
        we stop the walk and bisect the (last_healthy, first_unhealthy]
        interval to narrow the cliff.
    """

    PAD_SCHEMA: str = "pad_schema"
    BASE_TABLE: str = "base_t"
    # Cluster used to probe `mz_materialization_lag`. Built-in catalog server
    # cluster, so the probe does not add load to `c`.
    PROBE_CLUSTER: str = "mz_catalog_server"

    def __init__(
        self,
        kind: ClusterObjectLimitsKind,
        sizes: list[int],
        bisect_steps: int = CLUSTER_OBJECT_LIMITS_BISECT_STEPS,
    ) -> None:
        self._kind = kind
        self._sizes = sizes
        self._bisect_steps = bisect_steps
        self._current_n = 0

    # ----- Scenario protocol -----

    def name(self) -> str:
        return self._kind.scenario_name

    def mode(self) -> str:
        return "cluster_object_limits"

    def stream_key(self) -> str:
        return "cluster_object_limits"

    def scale_points(
        self, target: "BenchTarget", max_scale: int
    ) -> Iterable[ScalePoint]:
        for replica_scale in REPLICA_SCALES:
            if replica_scale > max_scale:
                break
            size = target.replica_size_for_scale(replica_scale)
            yield ScalePoint(
                label=f"replica_size={size}",
                cluster_size=size,
                replica_scale=replica_scale,
            )

    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool:
        assert point.cluster_size is not None
        # (Re)create the test cluster. Skip the iteration if this
        # cluster size isn't available on the target (not in
        # `mz_cluster_replica_sizes`, or exceeds
        # `max_credit_consumption_rate`).
        if not _recreate_cluster_c(
            runner,
            point.cluster_size,
            skip_if_unavailable_label=f"cluster_object_limits {self.name()}",
        ):
            return False
        runner.replica_size = point.cluster_size
        # Reset the pad schema and base table for this cluster-size iteration:
        # each cluster-size run starts from a clean slate. The cluster drop
        # cascades to remove on-cluster objects (indexes/MVs); dropping the
        # schema removes the off-cluster views.
        runner.run_query(f"DROP SCHEMA IF EXISTS {self.PAD_SCHEMA} CASCADE")
        runner.run_query(f"CREATE SCHEMA {self.PAD_SCHEMA}")
        runner.run_query(
            f"CREATE TABLE {self.PAD_SCHEMA}.{self.BASE_TABLE} (id int, val text)"
        )
        runner.run_query(
            f"INSERT INTO {self.PAD_SCHEMA}.{self.BASE_TABLE} VALUES (1, 'x')"
        )
        for stmt in self._kind.setup_statements:
            runner.run_query(stmt.format(schema=self.PAD_SCHEMA, base=self.BASE_TABLE))
        self._current_n = 0
        return True

    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        assert point.cluster_size is not None
        self._walk_and_bisect(runner, point.cluster_size)

    def cleanup_point(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        # Drop the schema (off-cluster views) and the cluster (on-cluster
        # objects cascade) before moving to the next cluster size, to bound
        # catalog growth across iterations.
        _best_effort_drop(
            runner,
            f"DROP SCHEMA IF EXISTS {self.PAD_SCHEMA} CASCADE",
            self.PAD_SCHEMA,
        )
        _best_effort_drop(runner, "DROP CLUSTER IF EXISTS c CASCADE", "cluster c")

    def add_objects(self, runner: ScenarioRunner, target_n: int) -> None:
        self._current_n = _extend_incremental(
            runner,
            current_n=self._current_n,
            target_n=target_n,
            templates_for=lambda i: tuple(
                t.format(schema=self.PAD_SCHEMA, base=self.BASE_TABLE, i=i)
                for t in self._kind.create_templates
            ),
            label=self._kind.label,
        )

    def remove_objects(self, runner: ScenarioRunner, target_n: int) -> None:
        """Drop the test objects above ``target_n`` from cluster ``c``.

        Inverse of ``add_objects``. Needed by the bisection refinement: after
        walking forward past the cliff, we step back down to probe
        intermediate N without rebuilding the catalog from scratch.
        """
        self._current_n = _shrink_incremental(
            runner,
            current_n=self._current_n,
            target_n=target_n,
            drop_for=lambda i: self._kind.drop_template.format(
                schema=self.PAD_SCHEMA, i=i
            ),
            label=self._kind.label,
        )

    def probe_lag_ms(self, runner: ScenarioRunner) -> tuple[int, int, float]:
        """Return (total, reporting, max_local_lag_ms) for our test objects.

        `reporting` is the subset with non-NULL `local_lag`; the caller
        treats unreported objects as not healthy.
        """
        rows = runner.run_query(
            f"""
            SELECT
                count(*),
                count(local_lag),
                COALESCE(
                    max(EXTRACT(EPOCH FROM local_lag) * 1000),
                    0
                )::float8
            FROM mz_internal.mz_materialization_lag l
            {self._kind.lag_filter}
            """,
            fetch=True,
        )
        assert rows is not None and len(rows) == 1
        total, reporting, max_lag_ms = rows[0]
        return int(total), int(reporting), float(max_lag_ms)

    def probe_hydrated(self, runner: ScenarioRunner) -> tuple[int, int]:
        """Return (total, hydrated) for our test objects on cluster ``c``.

        Decoupled from `local_lag` so the freshness loop can wait for
        objects to be hydrated first, then sample steady-state lag.
        """
        rows = runner.run_query(
            f"""
            SELECT
                count(*),
                count(*) FILTER (WHERE hs.hydrated)
            FROM mz_internal.mz_hydration_statuses hs
            {self._kind.hydration_filter}
            """,
            fetch=True,
        )
        assert rows is not None and len(rows) == 1
        total, hydrated = rows[0]
        return int(total), int(hydrated)

    def _hydrate_and_sample(
        self,
        runner: ScenarioRunner,
        target_n: int,
        timeout_s: int,
        samples: int,
        sample_interval_s: float,
        lag_threshold_ms: int,
    ) -> tuple[float, str, int, int]:
        """
        Two-phase probe: wait for hydration, then take steady-state lag
        samples. Returns (max_lag_ms, status, reporting, total), where
        ``status`` is one of:
          - "healthy": hydrated and steady-state lag below threshold
          - "lag": hydrated but steady-state lag above threshold
          - "hydration_timeout": Phase 1 timed out

        Splitting the phases distinguishes "still hydrating" (transient
        high lag is expected) from "hydrated but not keeping up" (the
        steady-state lag check). If hydration times out, the probe is
        unhealthy and Phase 2 doesn't run.

        Probes run on the catalog-server cluster so they don't load `c`;
        the SET is lifted out of the per-probe loop and restored on exit.
        """
        runner.run_query(f"SET cluster = '{self.PROBE_CLUSTER}'")
        try:
            # Phase 1: wait for all N objects to be hydrated.
            deadline = time.time() + timeout_s
            last_total = 0
            last_hydrated = 0
            while time.time() < deadline:
                total, hydrated = self.probe_hydrated(runner)
                last_total, last_hydrated = total, hydrated
                if hydrated >= target_n:
                    break
                time.sleep(1.0)
            else:
                # Hydration didn't complete. Probe lag once for diagnostics
                # so the CSV still carries a meaningful value.
                _, reporting, max_lag_ms = self.probe_lag_ms(runner)
                print(
                    f"    hydration timeout: {last_hydrated}/{target_n} "
                    f"hydrated after {timeout_s}s"
                )
                return max_lag_ms, "hydration_timeout", reporting, last_total

            # Phase 2: steady-state lag sampling.
            max_lag_over_samples = 0.0
            worst_reporting = target_n
            last_total = 0
            for _ in range(samples):
                time.sleep(sample_interval_s)
                total, reporting, max_lag_ms = self.probe_lag_ms(runner)
                last_total = total
                worst_reporting = min(worst_reporting, reporting)
                max_lag_over_samples = max(max_lag_over_samples, max_lag_ms)
            healthy = (
                worst_reporting == target_n and max_lag_over_samples < lag_threshold_ms
            )
            status = "healthy" if healthy else "lag"
            return max_lag_over_samples, status, worst_reporting, last_total
        finally:
            runner.run_query("SET cluster = 'c'")

    def _probe_and_record(
        self,
        runner: ScenarioRunner,
        n: int,
        lag_threshold_ms: int,
        hydration_timeout_s: int,
        first_probe_hydration_timeout_s: int,
        samples: int,
        sample_interval_s: float,
        first_probe: bool = False,
        refinement: bool = False,
    ) -> bool:
        runner.scale = n
        timeout_s = (
            first_probe_hydration_timeout_s if first_probe else hydration_timeout_s
        )
        max_lag_ms, status, reporting, total = self._hydrate_and_sample(
            runner, n, timeout_s, samples, sample_interval_s, lag_threshold_ms
        )
        healthy = status == "healthy"
        capped_lag_ms = min(max_lag_ms, CLUSTER_OBJECT_LIMITS_LAG_CAP_MS)
        print(
            f"    {'(bisect) ' if refinement else ''}"
            f"N={n}: max_local_lag_ms={max_lag_ms:.1f} "
            f"reporting={reporting}/{total} status={status}"
        )
        runner.add_result(
            "freshness",
            "max_local_lag_ms",
            repetition=0,
            size_bytes=None,
            time_ms=int(capped_lag_ms),
            healthy=1 if healthy else 0,
            failure_mode=None if healthy else status,
        )
        return healthy

    def _walk_and_bisect(self, runner: ScenarioRunner, replica_size: str) -> None:
        """Walk the N-list and bisect the cliff for one cluster size.

        Stops walking on the first unhealthy point (still recorded so the
        cliff is visible). If `self._bisect_steps > 0` and a cliff was
        found, bisect (last_healthy, first_unhealthy] that many times,
        adding or dropping objects in place.
        """

        def probe(
            n: int, *, first_probe: bool = False, refinement: bool = False
        ) -> bool:
            return self._probe_and_record(
                runner,
                n,
                CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS,
                CLUSTER_OBJECT_LIMITS_HYDRATION_TIMEOUT_S,
                CLUSTER_OBJECT_LIMITS_FIRST_PROBE_HYDRATION_TIMEOUT_S,
                CLUSTER_OBJECT_LIMITS_SAMPLES,
                CLUSTER_OBJECT_LIMITS_SAMPLE_INTERVAL_S,
                first_probe=first_probe,
                refinement=refinement,
            )

        # Coarse pass: walk the N list, stop at the first unhealthy point.
        last_healthy_n = 0
        first_unhealthy_n: int | None = None
        for i, n in enumerate(self._sizes):
            print(
                f"--- cluster_object_limits {self.name()}: "
                f"size '{replica_size}', building up to N={n}"
            )
            self.add_objects(runner, n)
            print(
                f"--- cluster_object_limits {self.name()}: "
                f"size '{replica_size}', probing freshness at N={n}"
            )
            # Only the very first probe after `_recreate_cluster_c` gets
            # the generous cold-start budget; by the time we walk to
            # larger N or bisect, the replica is warm.
            if probe(n, first_probe=(i == 0)):
                last_healthy_n = n
            else:
                first_unhealthy_n = n
                print(
                    f"    N={n}: unhealthy, stopping N-walk for size "
                    f"'{replica_size}'"
                )
                break

        # Refinement pass: bisect (last_healthy, first_unhealthy] to narrow
        # the cliff. Each step adds or drops objects in place.
        if first_unhealthy_n is not None and self._bisect_steps > 0:
            lo, hi = last_healthy_n, first_unhealthy_n
            for step in range(self._bisect_steps):
                mid = (lo + hi) // 2
                if mid <= lo:
                    # Interval already minimal (hi - lo <= 1).
                    break
                print(
                    f"--- cluster_object_limits {self.name()}: "
                    f"size '{replica_size}', bisect step "
                    f"{step + 1}/{self._bisect_steps} at N={mid} "
                    f"(cliff in ({lo}, {hi}])"
                )
                # Exactly one of these does work; the other is a no-op.
                self.add_objects(runner, mid)
                self.remove_objects(runner, mid)
                if probe(mid, refinement=True):
                    lo = mid
                else:
                    hi = mid
            print(
                f"    bisect done for size '{replica_size}': " f"cliff in ({lo}, {hi}]"
            )


class _StopSweep(Exception):
    """Raised from `Scenario.apply()` to skip the current point AND every
    subsequent point. Use `apply` returning False for skip-this-only."""


class _ClusterSizeSweepBase(Scenario):
    """Common ``scale_points`` for sweeps over `REPLICA_SCALES` cluster sizes."""

    def scale_points(
        self, target: "BenchTarget", max_scale: int
    ) -> Iterable[ScalePoint]:
        for replica_scale in REPLICA_SCALES:
            if replica_scale > max_scale:
                break
            size = target.replica_size_for_scale(replica_scale)
            yield ScalePoint(
                label=f"replica_size={size}",
                cluster_size=size,
                replica_scale=replica_scale,
            )


class StrongScalingSweep(_ClusterSizeSweepBase):
    """Sweep cluster size with fixed dataset. Wraps a `ClusterScalingScenario`
    workload: setup runs once in ``prepare``, the cluster is recreated for
    each size in ``apply``, and ``measure`` defers to ``workload.run``."""

    def __init__(
        self, name: str, workload: ClusterScalingScenario, split_points: bool = True
    ) -> None:
        self._name = name
        self._workload = workload
        self._split_points = split_points

    def name(self) -> str:
        return self._name

    def mode(self) -> str:
        return "strong"

    def stream_key(self) -> str:
        return "cluster"

    def parallel_safe(self) -> bool:
        return True

    def split_by_point(self) -> bool:
        return self._split_points

    def apply_namespace(self, ns: "Namespace") -> None:
        self._workload.cluster = ns.cluster
        self._workload.lg_cluster = ns.lg_cluster

    def peak_credits(self, points: list[ScalePoint], target: "BenchTarget") -> float:
        # lg is fixed at the workload's (scale-1) size for strong scaling; the
        # measurement cluster `c` varies per point. Peak = lg + largest c.
        lg = extract_cluster_size(self._workload.replica_size or "0cc")
        c = max(
            (extract_cluster_size(p.cluster_size) for p in points if p.cluster_size),
            default=0.0,
        )
        return lg + c

    def prepare(self, runner: ScenarioRunner) -> None:
        runner.scale = self._workload.scale
        for q in self._workload.drop():
            runner.run_query(q)
        _prepare_probe_table(runner)
        for q in self._workload.setup():
            runner.run_query(q)
        for n in self._workload.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {n};")

    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool:
        assert point.cluster_size is not None
        _recreate_cluster_c(runner, point.cluster_size, smoke_test=True)
        runner.replica_size = point.cluster_size
        return True

    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        self._workload.run(runner)


class WeakScalingSweep(_ClusterSizeSweepBase):
    """Sweep cluster size AND dataset size in lockstep. The workload's setup
    is rerun at each point with ``scale = initial_scale * replica_scale``."""

    def __init__(
        self, name: str, workload: ClusterScalingScenario, split_points: bool = True
    ) -> None:
        self._name = name
        self._workload = workload
        self._initial_scale = workload.scale
        self._split_points = split_points

    def name(self) -> str:
        return self._name

    def mode(self) -> str:
        return "weak"

    def stream_key(self) -> str:
        return "cluster"

    def parallel_safe(self) -> bool:
        return True

    def split_by_point(self) -> bool:
        return self._split_points

    def apply_namespace(self, ns: "Namespace") -> None:
        self._workload.cluster = ns.cluster
        self._workload.lg_cluster = ns.lg_cluster

    def peak_credits(self, points: list[ScalePoint], target: "BenchTarget") -> float:
        # Both lg and `c` scale with the point for weak scaling. Peak = 2x the
        # largest point's size.
        c = max(
            (extract_cluster_size(p.cluster_size) for p in points if p.cluster_size),
            default=0.0,
        )
        return 2 * c

    def prepare(self, runner: ScenarioRunner) -> None:
        with runner.connection as cur:
            cur.execute("DROP TABLE IF EXISTS t CASCADE;")
            cur.execute("CREATE TABLE t (a int);")
            cur.execute("INSERT INTO t VALUES (1);")

    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool:
        assert point.cluster_size is not None and point.replica_scale is not None
        self._workload.replica_size = point.cluster_size
        self._workload.scale = self._initial_scale * point.replica_scale
        runner.scale = self._workload.scale
        for q in self._workload.drop():
            runner.run_query(q)
        for q in self._workload.setup():
            runner.run_query(q)
        for n in self._workload.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {n};")
        print(f"--- Loading complete; creating cluster with size {point.cluster_size}")
        _recreate_cluster_c(runner, point.cluster_size, smoke_test=True)
        runner.replica_size = point.cluster_size
        return True

    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        self._workload.run(runner)


class EnvdCpuSweep(Scenario):
    """Sweep environmentd's CPU allocation at a fixed compute cluster size.
    Wraps a `ClusterScalingScenario` workload. On Cloud, we reset envd's
    CPU allocation to the default in ``teardown`` regardless of outcome to
    avoid accidentally burning credits."""

    # (So far, I haven't seen a difference between 16 and 32 in manual
    # testing in cloud. When we start seeing a difference, consider
    # extending to 64.)
    ENVD_CPU_SCALES: list[int] = [1, 2, 4, 8, 16, 32]

    def __init__(self, name: str, workload: ClusterScalingScenario) -> None:
        self._name = name
        self._workload = workload
        self._fixed_replica_size: str | None = None

    def name(self) -> str:
        return self._name

    def mode(self) -> str:
        # Matches the historical "strong" mode tag for these scenarios
        # (envd_qps_strong_scaling, copy_from_stdin_envd_strong_scaling).
        return "strong"

    def stream_key(self) -> str:
        return "envd"

    def scale_points(
        self, target: "BenchTarget", max_scale: int
    ) -> Iterable[ScalePoint]:
        for cpus in self.ENVD_CPU_SCALES:
            if cpus > max_scale:
                break
            yield ScalePoint(label=f"envd_cpus={cpus}", envd_cpus=cpus)

    def prepare(self, runner: ScenarioRunner) -> None:
        runner.scale = self._workload.scale
        _prepare_probe_table(runner)
        for q in self._workload.setup():
            runner.run_query(q)
        for n in self._workload.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {n};")
        self._fixed_replica_size = runner.target.replica_size_for_scale(1)

    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool:
        assert point.envd_cpus is not None
        assert self._fixed_replica_size is not None
        try:
            reconfigure_envd_cpus(runner.target, point.envd_cpus, runner)
        except UIError as e:
            print(
                f"WARNING: Failed to reconfigure to {point.envd_cpus} CPUs, "
                f"skipping remaining scale points: {e}"
            )
            raise _StopSweep(str(e)) from e
        fixed_size = self._fixed_replica_size

        def recreate() -> None:
            _recreate_cluster_c(runner, fixed_size, smoke_test=True)

        runner.connection.retryable(recreate)
        # Record envd CPU cores for this step. (We intentionally do not set
        # replica_size, because that would be fixed in this scenario, so
        # there is no meaningful analysis to be done on that.)
        runner.envd_cpus = point.envd_cpus
        return True

    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        self._workload.run(runner)

    def teardown(self, runner: ScenarioRunner) -> None:
        if isinstance(runner.target, CloudTarget):
            print("--- Resetting Cloud environmentd CPUs to the default")
            target = runner.target
            version_args = (
                ["--version", target.version] if target.version is not None else []
            )
            target.composition.run(
                "mz",
                "region",
                "enable",
                "--environmentd-cpu-allocation",
                "2",
                *version_args,
                rm=True,
            )


class EnvdObjectsSweep(Scenario):
    """Sweep catalog object count (N) at a fixed compute cluster size.
    Wraps an `EnvdObjectsScalabilityScenario` workload: catalog is built
    incrementally, so transitioning to the next size adds only the delta."""

    def __init__(
        self,
        name: str,
        workload: EnvdObjectsScalabilityScenario,
        sizes: list[int],
    ) -> None:
        self._name = name
        self._workload = workload
        self._sizes = sizes

    def name(self) -> str:
        return self._name

    def mode(self) -> str:
        return "envd_objects_scalability"

    def stream_key(self) -> str:
        return "envd_objects"

    def scale_points(
        self, target: "BenchTarget", max_scale: int
    ) -> Iterable[ScalePoint]:
        for n in self._sizes:
            yield ScalePoint(label=f"N={n}", n=n)

    def prepare(self, runner: ScenarioRunner) -> None:
        # (Re)create the fixed-size measurement cluster and a tiny one-row
        # table used by the simple-peek measurement.
        measurement_size = runner.target.replica_size_for_scale(1)
        runner.replica_size = measurement_size
        _recreate_cluster_c(runner, measurement_size)
        _prepare_probe_table(runner)
        self._workload.init(runner)

    def apply(self, runner: ScenarioRunner, point: ScalePoint) -> bool:
        assert point.n is not None
        self._workload.add_objects(runner, point.n)
        runner.scale = point.n
        return True

    def measure(self, runner: ScenarioRunner, point: ScalePoint) -> None:
        self._workload.run(runner)

    def teardown(self, runner: ScenarioRunner) -> None:
        self._workload.teardown(runner)


# TODO: We should factor out the below
# `disable_region`, `cloud_disable_enable_and_wait`, `reconfigure_envd_cpus`, `wait_for_envd`
# functions into a separate module. (Similar `disable_region` functions also occur in other tests.)
def disable_region(composition: Composition, hard: bool) -> None:
    print("Shutting down region ...")

    try:
        if hard:
            composition.run("mz", "region", "disable", "--hard", rm=True)
        else:
            composition.run("mz", "region", "disable", rm=True)
    except UIError:
        # Can return: status 404 Not Found
        pass


def cloud_disable_enable_and_wait(
    target: "BenchTarget",
    environmentd_cpu_allocation: int | None = None,
) -> None:
    """
    Soft-disable and then enable the Cloud region, then wait for environmentd readiness.

    The disabling is needed because `mz region enable` does a 0dt rollout. This means that,
    without a `disable`, we'd get an intrusive envd restart some time later after `wait_for_envd`
    already succeeded (with the old envd). Therefore, we do an `mz region disable` first, so
    that `wait_for_envd` can't succeed before the read-only env promotes.

    When `environmentd_cpu_allocation` is provided, it is passed to `mz region enable` via
    `--environmentd-cpu-allocation` to reconfigure environmentd's CPU allocation.
    """
    disable_region(target.composition, hard=False)

    version_args = (
        ["--version", target.version]
        if isinstance(target, CloudTarget) and target.version is not None
        else []
    )

    if environmentd_cpu_allocation is None:
        target.composition.run("mz", "region", "enable", *version_args, rm=True)
    else:
        target.composition.run(
            "mz",
            "region",
            "enable",
            "--environmentd-cpu-allocation",
            str(environmentd_cpu_allocation),
            *version_args,
            rm=True,
        )

    time.sleep(10)

    # The region (and possibly its hostname) was just re-enabled; drop any
    # cached hostname so the next connection re-resolves it.
    if isinstance(target, CloudTarget):
        target.invalidate_hostname()

    assert "materialize.cloud" in target.composition.cloud_hostname()
    wait_for_envd(target)


def reconfigure_envd_cpus(
    target: "BenchTarget", envd_cpus: int, runner: ScenarioRunner
) -> None:
    """
    Reconfigure the number of CPU cores allocated to environmentd for the given target.

    - Docker target: recreate the local `materialized` container with a CPU limit equal to envd_cpus,
      wait for SQL readiness, and force the benchmark connection to reconnect.
    - Cloud target: soft-disable/enable the region with the desired envd CPU allocation, wait for
      SQL readiness, and force the benchmark connection to reconnect.
    """
    if isinstance(target, DockerTarget):
        # For Docker target: restart `materialized` with a CPU limit equal to envd_cpus.
        try:
            # Create a temporary override of the materialized service with updated CPU limits.
            # Keep other defaults consistent with SERVICES.
            overridden = Materialized(
                propagate_crashes=True,
                additional_system_parameter_defaults=MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
                # This is just an upper limit; it won't make a noise if your local machine doesn't have enough cores.
                # If you'd like to avoid going over your machine's core count, you can use `--max-scale`.
                cpu=str(envd_cpus),
            )
            print(f"--- Reconfiguring local environmentd CPUs to {envd_cpus}")
            with target.composition.override(overridden):
                # Recreate the container to apply new limits, but preserve volumes.
                try:
                    target.composition.rm(
                        "materialized", stop=True, destroy_volumes=False
                    )
                except CommandFailureCausedUIError as e:
                    # Ignore only the benign case where the container does not yet exist.
                    if not (e.stderr and "No such container" in e.stderr):
                        raise
                target.composition.up("materialized")
                wait_for_envd(target, timeout_secs=60)
        except Exception as e:
            raise UIError(f"failed to apply Docker CPU override for environmentd: {e}")
    else:
        # Cloud target: reconfigure environmentd CPUs via `mz region`.
        try:
            print(f"--- Reconfiguring Cloud environmentd CPUs to {envd_cpus}")
            cloud_disable_enable_and_wait(target, environmentd_cpu_allocation=envd_cpus)
        except Exception as e:
            raise UIError(
                f"failed to apply Cloud CPU override for environmentd via 'mz region': {e}"
            )

    # Force reconnection to the (potentially restarted) service.
    try:
        runner.connection.connection.close()
    except Exception:
        pass


def wait_for_envd(target: "BenchTarget", timeout_secs: int = 300) -> None:
    """
    Wait until the environmentd SQL endpoint is ready.

    - Cloud: uses cloud hostname:6875, sslmode=require, and prefers the per-run
      app password if available; falls back to MZ_CLI_APP_PASSWORD.
    - Docker: probes SQL readiness via Composition.sql_query
    """
    if isinstance(target, CloudTarget):
        host = target.composition.cloud_hostname()
        user = target.username
        # Prefer the newly created app password when present; fall back to the CLI password.
        password = target.new_app_password or target.app_password or ""
        sslmode = "require"
        print(
            f"Waiting for cloud environmentd at {host}:6875 to come up with username {user} ..."
        )
        _wait_for_pg(
            host=host,
            user=user,
            password=password,
            port=6875,
            query="SELECT 1",
            expected=[(1,)],
            timeout_secs=timeout_secs,
            dbname="materialize",
            sslmode=sslmode,
        )
    else:
        # Docker target: use the composition helper to query the service via the
        # host-mapped port on 127.0.0.1; the container hostname "materialized"
        # is not resolvable from the host network when using psycopg directly.
        print("Waiting for local environmentd (docker) at materialized:6875 ...")
        deadline = time.time() + timeout_secs
        last_err: Exception | None = None
        while time.time() < deadline:
            try:
                target.composition.sql_query("SELECT 1", service="materialized")
                return
            except Exception as e:
                last_err = e
                time.sleep(1)
        raise UIError(
            f"materialized did not accept SQL connections within {timeout_secs}s after restart: {last_err}"
        )


# Parameters we expect to be in effect at workflow start (set by
# `MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS` on Docker; set
# server-side on cloud). Logged so we can verify the environment
# matches what we configured.
SYSTEM_PARAMETERS_TO_LOG: list[str] = [
    "max_tables",
    "max_materialized_views",
    "max_objects_per_schema",
    "max_clusters",
    "max_credit_consumption_rate",
    "memory_limiter_interval",
]


def log_environment_info(target: "BenchTarget") -> None:
    """Print the environment id and key system parameters.

    Best-effort: any error is logged and swallowed so a transient probe
    failure does not abort the workflow.
    """
    print("--- Environment info")
    try:
        conn = target.new_connection()
    except Exception as e:
        print(f"  WARNING: could not open connection to log environment info: {e}")
        return
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT mz_environment_id()")
                row = cur.fetchone()
                env_id = row[0] if row else "<unknown>"
                print(f"  mz_environment_id() = {env_id}")
            except Exception as e:
                print(f"  WARNING: failed to read mz_environment_id(): {e}")
            for param in SYSTEM_PARAMETERS_TO_LOG:
                try:
                    cur.execute(
                        psycopg_sql.SQL("SHOW {}").format(psycopg_sql.Identifier(param))
                    )
                    row = cur.fetchone()
                    val = row[0] if row else "<unknown>"
                    print(f"  {param} = {val}")
                except Exception as e:
                    print(f"  WARNING: failed to SHOW {param}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Intra-region parallelism
#
# The cluster-scaling scenarios are independent and self-contained, so within
# a single region we run them — and, for sweeps that opt in via
# `split_by_point()`, each of their scale points — as concurrent jobs. Each
# job gets its own database (isolating relations / indexes) and its own
# globally-unique cluster names (clusters are not database-scoped), builds its
# own data, and tears everything down at the end.
#
# Concurrency is bounded by a `ResourceGovernor` so we never knowingly exceed
# the region's `max_credit_consumption_rate` / `max_clusters`, plus a hard
# thread cap (`--max-parallelism`). Scenarios with region-global side effects
# (envd CPU reconfiguration, envd-objects, cluster-object-limits) declare
# `parallel_safe() == False` and run serially in isolation.
# ---------------------------------------------------------------------------


class ResourceGovernor:
    """Bounds concurrent jobs by credit-rate and cluster-count budgets.

    A job reserves its estimated peak credits and cluster count before
    creating any clusters and releases them once its clusters are dropped, so
    the scheduler never knowingly exceeds the configured limits. A single
    reservation is capped to the whole budget so an over-large job runs alone
    rather than deadlocking."""

    def __init__(self, credit_budget: float, cluster_budget: int) -> None:
        self._cv = threading.Condition()
        self._credit_budget = max(credit_budget, 0.0)
        self._cluster_budget = max(cluster_budget, 1)
        self._credits_avail = self._credit_budget
        self._clusters_avail = self._cluster_budget

    def acquire(self, credits: float, clusters: int) -> tuple[float, int]:
        credits = min(max(credits, 0.0), self._credit_budget)
        clusters = min(max(clusters, 1), self._cluster_budget)
        with self._cv:
            while not (
                self._credits_avail >= credits and self._clusters_avail >= clusters
            ):
                self._cv.wait()
            self._credits_avail -= credits
            self._clusters_avail -= clusters
        return (credits, clusters)

    def release(self, reservation: tuple[float, int]) -> None:
        credits, clusters = reservation
        with self._cv:
            self._credits_avail += credits
            self._clusters_avail += clusters
            self._cv.notify_all()


def _sanitize_ident(s: str) -> str:
    """Turn an arbitrary label into a safe SQL identifier fragment."""
    out = re.sub(r"[^a-z0-9_]+", "_", s.lower()).strip("_")
    # Bound length so concatenated names stay well within identifier limits.
    return (out or "x")[:48]


def _quickstart_size(target: "BenchTarget") -> str:
    return (
        PARALLEL_QUICKSTART_SIZE_DOCKER
        if isinstance(target, DockerTarget)
        else PARALLEL_QUICKSTART_SIZE_CLOUD
    )


def _make_namespace(scenario_name: str, point_label: str) -> Namespace:
    base = _sanitize_ident(f"{scenario_name}_{point_label}")
    return Namespace(
        database=f"csheet_{base}",
        cluster=f"c_{base}",
        lg_cluster=f"lg_{base}",
        quickstart=f"qs_{base}",
        log_prefix=f"[{scenario_name} {point_label}] ",
    )


@dataclass
class _Job:
    scenario: Scenario
    points: list[ScalePoint]
    namespace: Namespace
    credits: float


def _expand_jobs(
    name: str,
    spec: "ScenarioSpec",
    args: argparse.Namespace,
    target: "BenchTarget",
    max_scale: int,
) -> list[_Job]:
    """Expand a parallel-safe scenario into one or more independent jobs.

    A fresh `Scenario` instance is built per job so concurrent jobs never
    share mutable workload state."""
    base = spec.factory(args, target)
    points = list(base.scale_points(target, max_scale))
    if not points:
        return []
    qs_credits = extract_cluster_size(_quickstart_size(target))
    jobs: list[_Job] = []
    if base.split_by_point():
        for pt in points:
            scenario = spec.factory(args, target)
            jobs.append(
                _Job(
                    scenario=scenario,
                    points=[pt],
                    namespace=_make_namespace(name, pt.label),
                    credits=scenario.peak_credits([pt], target) + qs_credits,
                )
            )
    else:
        jobs.append(
            _Job(
                scenario=base,
                points=points,
                namespace=_make_namespace(name, "all"),
                credits=base.peak_credits(points, target) + qs_credits,
            )
        )
    return jobs


def _bootstrap_namespace(
    conn: ConnectionHandler, ns: Namespace, quickstart_size: str
) -> None:
    with conn as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {ns.database} CASCADE".encode())
        cur.execute(f"CREATE DATABASE {ns.database}".encode())
        cur.execute(f"DROP CLUSTER IF EXISTS {ns.quickstart} CASCADE".encode())
        cur.execute(f"CREATE CLUSTER {ns.quickstart} SIZE '{quickstart_size}'".encode())


def _teardown_namespace_clusters(conn: ConnectionHandler, ns: Namespace) -> None:
    for cluster in (ns.cluster, ns.lg_cluster, ns.quickstart):
        try:
            with conn as cur:
                cur.execute(f"DROP CLUSTER IF EXISTS {cluster} CASCADE".encode())
        except Exception as e:
            print(f"WARNING: failed to drop cluster {cluster}: {e}")


def run_job(
    job: _Job,
    target: "BenchTarget",
    max_scale: int,
    writer: csv.DictWriter,
    write_lock: threading.Lock,
    governor: ResourceGovernor,
    statement_timeout_ms: int | None,
) -> None:
    ns = job.namespace
    reservation = governor.acquire(job.credits, PARALLEL_CLUSTERS_PER_JOB)
    try:
        quickstart_size = _quickstart_size(target)
        # Bootstrap the worker database + quickstart cluster on a short-lived
        # default-database connection.
        boot = ConnectionHandler(
            target.new_connection, statement_timeout_ms=statement_timeout_ms
        )
        try:
            boot.retryable(lambda: _bootstrap_namespace(boot, ns, quickstart_size))
        finally:
            try:
                boot.connection.close()
            except Exception:
                pass
        # Worker connection scoped to the namespace's database and quickstart
        # cluster; the scenario switches to `lg`/`c` as it runs.
        conn = ConnectionHandler(
            target.new_connection,
            dbname=ns.database,
            cluster=ns.quickstart,
            statement_timeout_ms=statement_timeout_ms,
        )
        try:
            run_scenario(
                job.scenario,
                writer,
                conn,
                target,
                max_scale,
                points=job.points,
                namespace=ns,
                write_lock=write_lock,
            )
        finally:
            _teardown_namespace_clusters(conn, ns)
            try:
                conn.connection.close()
            except Exception:
                pass
            # Drop the database from a fresh default-database connection
            # (can't drop the database the worker connection is attached to).
            cleanup = ConnectionHandler(
                target.new_connection, statement_timeout_ms=statement_timeout_ms
            )
            try:
                cleanup.retryable(
                    lambda: cleanup.connection.cursor().execute(
                        f"DROP DATABASE IF EXISTS {ns.database} CASCADE".encode()
                    )
                )
            except Exception as e:
                print(f"WARNING: failed to drop database {ns.database}: {e}")
            finally:
                try:
                    cleanup.connection.close()
                except Exception:
                    pass
    finally:
        governor.release(reservation)


def run_parallel(
    composition: Composition,
    scenario_names: list[str],
    args: argparse.Namespace,
    target: "BenchTarget",
    max_scale: int,
    streams: "dict[str, OpenStream]",
    statement_timeout_ms: int | None,
    run_serial: "Callable[[str], None]",
) -> None:
    """Run the requested scenarios with intra-region parallelism.

    Parallel-safe scenarios are fanned into jobs and run concurrently under a
    `ResourceGovernor`; non-parallel-safe scenarios (region-global side
    effects) run serially first, in isolation. Exceptions are collected and
    the first is re-raised (mirroring `Composition.test_parts`)."""
    parallel_specs: list[tuple[str, ScenarioSpec]] = []
    serial_names: list[str] = []
    for name in scenario_names:
        spec = SCENARIOS_BY_NAME[name]
        if spec.factory(args, target).parallel_safe():
            parallel_specs.append((name, spec))
        else:
            serial_names.append(name)

    exceptions: list[Exception] = []

    # Region-global scenarios run one at a time, before the parallel pool.
    for name in serial_names:
        print(f"--- SCENARIO (serial): {name}")
        try:
            run_serial(name)
        except Exception as e:
            print(f"^^^ +++ serial scenario {name} failed: {e}")
            exceptions.append(e)

    jobs: list[_Job] = []
    for name, spec in parallel_specs:
        jobs.extend(_expand_jobs(name, spec, args, target, max_scale))

    if jobs:
        governor = ResourceGovernor(
            args.parallel_credit_budget, args.parallel_cluster_budget
        )
        write_locks = {key: threading.Lock() for key in streams}
        print(
            f"--- Running {len(jobs)} parallel jobs "
            f"(max_parallelism={args.max_parallelism}, "
            f"credit_budget={args.parallel_credit_budget}, "
            f"cluster_budget={args.parallel_cluster_budget})"
        )
        with ThreadPoolExecutor(max_workers=args.max_parallelism) as executor:
            futures = {
                executor.submit(
                    run_job,
                    job,
                    target,
                    max_scale,
                    streams[job.scenario.stream_key()].writer,
                    write_locks[job.scenario.stream_key()],
                    governor,
                    statement_timeout_ms,
                ): job
                for job in jobs
            }
            for fut in as_completed(futures):
                job = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f"^^^ +++ job {job.namespace.log_prefix}failed: {e}")
                    exceptions.append(e)

    if exceptions:
        if len(exceptions) > 1:
            print(f"Further exceptions were raised:\n{exceptions[1:]}")
        raise exceptions[0]


def workflow_default(composition: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Run the bench workflow by default
    """
    parser.add_argument(
        "--cleanup",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--record",
        default=f"results_{int(time.time())}.csv",
        help="CSV file to store results.",
    )
    parser.add_argument(
        "--analyze",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Analyze results after completing test. Dispatches to cluster-scale or envd-scale focused analyses based on the file suffix: `.cluster.csv` or `.envd.csv`.",
    )
    parser.add_argument(
        "--target",
        default="cloud-production",
        choices=["cloud-production", "cloud-staging", "docker"],
        help="Target to deploy to (default: cloud-production).",
    )
    parser.add_argument(
        "--max-scale",
        type=int,
        default=32,
        help="Maximum scale to test. For QPS scenarios, this directly corresponds to the number of CPU cores given to envd.",
    )
    parser.add_argument(
        "--max-parallelism",
        type=int,
        default=1,
        help=(
            "Maximum number of parallel-safe scenario jobs to run concurrently "
            "within a single region. 1 (default) preserves the serial behavior. "
            ">1 enables intra-region parallelism, additionally bounded by "
            "--parallel-credit-budget and --parallel-cluster-budget. Scenarios "
            "with region-global side effects (envd qps/objects, "
            "cluster-object-limits) always run serially."
        ),
    )
    parser.add_argument(
        "--statement-timeout",
        type=int,
        default=DEFAULT_STATEMENT_TIMEOUT_MS,
        help=(
            "Per-statement server-side timeout in milliseconds applied to all "
            "benchmark connections, so a hung query / crash-looping region "
            "fails in bounded time instead of blocking until the Buildkite "
            f"timeout. Default {DEFAULT_STATEMENT_TIMEOUT_MS} ms. Set to 0 to "
            "disable."
        ),
    )
    parser.add_argument(
        "--parallel-credit-budget",
        type=float,
        default=PARALLEL_LIMIT_SAFETY
        * float(
            MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS[
                "max_credit_consumption_rate"
            ]
        ),
        help=(
            "Total estimated credits/hour the parallel scheduler may reserve "
            "across concurrent jobs. Defaults to "
            f"{PARALLEL_LIMIT_SAFETY:g} x the configured "
            "max_credit_consumption_rate."
        ),
    )
    parser.add_argument(
        "--parallel-cluster-budget",
        type=int,
        default=int(
            PARALLEL_LIMIT_SAFETY
            * float(MATERIALIZED_ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS["max_clusters"])
        ),
        help=(
            "Total clusters the parallel scheduler may reserve across "
            "concurrent jobs (each job holds "
            f"{PARALLEL_CLUSTERS_PER_JOB}). Defaults to "
            f"{PARALLEL_LIMIT_SAFETY:g} x the configured max_clusters."
        ),
    )
    parser.add_argument(
        "--scale-tpch", type=float, default=8, help="TPCH scale factor."
    )
    parser.add_argument(
        "--scale-tpch-queries", type=float, default=4, help="TPCH queries scale factor."
    )
    parser.add_argument(
        "--scale-auction", type=int, default=3, help="Auction scale factor."
    )
    parser.add_argument(
        "--envd-objects-scalability-sizes",
        type=lambda s: [int(x) for x in s.split(",") if x.strip()],
        default=ENVD_OBJECTS_SCALABILITY_SIZES,
        help=(
            "Comma-separated list of catalog object counts to test for "
            "envd_objects_scalability scenarios. Defaults to "
            f"{','.join(str(n) for n in ENVD_OBJECTS_SCALABILITY_SIZES)}."
        ),
    )
    parser.add_argument(
        "--cluster-object-limits-max",
        type=int,
        default=CLUSTER_OBJECT_LIMITS_DEFAULT_MAX,
        help=(
            "Upper bound on N for cluster_object_limits scenarios when no "
            "explicit --cluster-object-limits-sizes is given. Defaults to "
            f"{CLUSTER_OBJECT_LIMITS_DEFAULT_MAX}."
        ),
    )
    parser.add_argument(
        "--cluster-object-limits-sizes",
        type=lambda s: [int(x) for x in s.split(",") if x.strip()],
        default=None,
        help=(
            "Comma-separated list of N (object-count) steps for the "
            "cluster_object_limits scenarios. If omitted, defaults to a "
            "geometric ramp up to 1000 then +1000 increments up to "
            "--cluster-object-limits-max."
        ),
    )
    parser.add_argument(
        "--cluster-object-limits-bisect-steps",
        type=int,
        default=CLUSTER_OBJECT_LIMITS_BISECT_STEPS,
        help=(
            "After the coarse N-walk locates the cliff for a given cluster "
            "size, bisect the (last_healthy, first_unhealthy) interval this "
            "many times to narrow it. Set to 0 to disable refinement. "
            f"Default: {CLUSTER_OBJECT_LIMITS_BISECT_STEPS}."
        ),
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        choices=list(SCENARIOS_BY_NAME) + list(SCENARIO_GROUPS),
        help="Scenarios to run, supports individual scenario names as well as 'all', 'cluster', 'envd_qps_scalability', 'envd_objects_scalability', 'cluster_object_limits'.",
    )

    args = parser.parse_args()

    scenarios: set[str] = set()
    for s in args.scenarios or ["all"]:
        if s in SCENARIO_GROUPS:
            scenarios.update(SCENARIO_GROUPS[s])
        elif s in SCENARIOS_BY_NAME:
            scenarios.add(s)
        else:
            raise ValueError(f"Unknown scenario: {s}")
    print(f"--- Running scenarios: {', '.join(scenarios)}")

    # cluster_object_limits scenarios push the cluster to its idle-object
    # limit; we only run them on staging or local Docker to avoid
    # spill-over effects to real customer environments.
    cluster_object_limits_requested = scenarios & set(
        SCENARIO_GROUPS.get("cluster_object_limits", [])
    )
    if cluster_object_limits_requested and args.target == "cloud-production":
        raise UIError(
            "cluster_object_limits scenarios cannot run against "
            "--target=cloud-production; use --target=cloud-staging or "
            "--target=docker. Requested scenarios: "
            f"{', '.join(sorted(cluster_object_limits_requested))}."
        )

    if args.target == "cloud-production":
        target: BenchTarget = CloudTarget(
            composition, PRODUCTION_USERNAME, PRODUCTION_APP_PASSWORD or ""
        )
        mz = Mz(
            region=PRODUCTION_REGION,
            environment=PRODUCTION_ENVIRONMENT,
            app_password=PRODUCTION_APP_PASSWORD or "",
        )
    elif args.target == "cloud-staging":
        target: BenchTarget = CloudTarget(
            composition,
            STAGING_USERNAME,
            STAGING_APP_PASSWORD or "",
            is_staging=True,
            version=staging_version(),
        )
        mz = Mz(
            region=STAGING_REGION,
            environment=STAGING_ENVIRONMENT,
            app_password=STAGING_APP_PASSWORD or "",
        )
    elif args.target == "docker":
        target = DockerTarget(composition)
        mz = Mz(app_password="")
    else:
        raise ValueError(f"Unknown target: {args.target}")

    with composition.override(mz):
        target_max = target.max_scale()
        max_scale = (
            args.max_scale if target_max is None else min(args.max_scale, target_max)
        )

        if args.cleanup:
            target.cleanup()

        target.initialize()

        log_environment_info(target)

        # Resolve the cluster_object_limits N-list once so each scenario
        # factory can read `args.cluster_object_limits_sizes` directly.
        if args.cluster_object_limits_sizes is None:
            args.cluster_object_limits_sizes = default_cluster_object_limits_sizes(
                args.cluster_object_limits_max
            )

        # Open one CSV per result-stream kind, keyed by `ResultStreamSpec.key`.
        base_name = os.path.splitext(args.record)[0]
        streams: dict[str, OpenStream] = {
            spec.key: _open_stream(spec, base_name) for spec in RESULT_STREAMS
        }

        statement_timeout_ms = args.statement_timeout or None

        def run_serial_scenario(scenario_name: str) -> None:
            conn = ConnectionHandler(
                target.new_connection, statement_timeout_ms=statement_timeout_ms
            )

            # Misc setup cluster for any ad-hoc queries the scenario runs
            # before its own cluster `c` exists.
            size = "50cc" if isinstance(target, CloudTarget) else "scale=1,workers=1"
            with conn as cur:
                cur.execute("DROP CLUSTER IF EXISTS quickstart;")
                cur.execute(f"CREATE CLUSTER quickstart SIZE '{size}';".encode())

            spec = SCENARIOS_BY_NAME[scenario_name]
            scenario = spec.factory(args, target)
            writer = streams[scenario.stream_key()].writer
            print(f"--- SCENARIO: Running {spec.log_label}")
            run_scenario(scenario, writer, conn, target, max_scale)

        def process(scenario_name: str) -> None:
            with composition.test_case(scenario_name):
                run_serial_scenario(scenario_name)

        test_failed = True
        try:
            scenarios_list = buildkite.shard_list(sorted(list(scenarios)), lambda s: s)
            if args.max_parallelism and args.max_parallelism > 1:
                run_parallel(
                    composition,
                    scenarios_list,
                    args,
                    target,
                    max_scale,
                    streams,
                    statement_timeout_ms,
                    run_serial_scenario,
                )
            else:
                composition.test_parts(scenarios_list, process)
            test_failed = False
        finally:
            for stream in streams.values():
                stream.file.close()
            if args.cleanup:
                target.cleanup()

        # Upload, archive, and analyze each result stream uniformly. The
        # cluster_object_limits stream's extra `healthy` / `failure_mode`
        # columns are silently dropped on upload (CSV writer uses
        # extrasaction="ignore"); to recover them, consult the artifact
        # CSV directly.
        for stream in streams.values():
            stream.spec.upload(composition, stream.path, not test_failed)

        assert not test_failed

        if buildkite.is_in_buildkite():
            for stream in streams.values():
                buildkite.upload_artifact(stream.path, cwd=MZ_ROOT, quiet=True)

        if args.analyze:
            for stream in streams.values():
                stream.spec.analyze(stream.path)


def _connection_options(
    cluster: str | None, statement_timeout_ms: int | None
) -> str | None:
    """Build a libpq ``options`` string of ``-c key=val`` server settings.

    Settings passed this way are applied at connection startup and persist
    across reconnects, so a worker's default cluster and statement timeout
    survive a dropped connection. Returns ``None`` when there is nothing to
    set."""
    parts: list[str] = []
    if statement_timeout_ms is not None and statement_timeout_ms > 0:
        parts.append(f"statement_timeout={int(statement_timeout_ms)}")
    if cluster is not None:
        parts.append(f"cluster={cluster}")
    return " ".join(f"-c {p}" for p in parts) if parts else None


class BenchTarget:
    composition: Composition

    @abstractmethod
    def initialize(self) -> None: ...
    @abstractmethod
    def new_connection(
        self,
        *,
        dbname: str | None = None,
        cluster: str | None = None,
        statement_timeout_ms: int | None = None,
    ) -> psycopg.Connection: ...
    @abstractmethod
    def cleanup(self) -> None: ...
    @abstractmethod
    def replica_size_for_scale(self, scale: int) -> str:
        """
        Returns the replica size for a given scale.
        """
        ...

    def max_scale(self) -> int | None:
        """
        Returns the maximum scale for the target, or None if there is no limit.
        """
        return None

    @abstractmethod
    def dbbench_connection_flags(self) -> list[str]:
        """
        Return dbbench connection flags appropriate for this target, excluding the
        workload file path. The result should include driver, host, port, username,
        optional password, database, and params (including sslmode and cluster=c).
        """
        ...


class CloudTarget(BenchTarget):
    def __init__(
        self,
        composition: Composition,
        username: str,
        app_password: str,
        is_staging: bool = False,
        version: str | None = None,
    ) -> None:
        self.composition = composition
        self.username = username
        self.app_password = app_password
        self.new_app_password: str | None = None
        self.is_staging = is_staging
        # Set for staging runs so `mz region enable --version <version>` pins the
        # exact image built for this PR. Must be None for production (production
        # doesn't accept a custom version).
        self.version = version
        assert (version is not None) == is_staging
        # The cloud hostname is stable for the life of a region; resolving it
        # spawns an `mz region show` subprocess, so cache it (and guard the
        # cache for concurrent connects in parallel mode). Invalidated whenever
        # the region is re-enabled (which can change the hostname).
        self._hostname_lock = threading.Lock()
        self._hostname_cache: str | None = None

    def host(self) -> str:
        with self._hostname_lock:
            if self._hostname_cache is None:
                self._hostname_cache = self.composition.cloud_hostname()
            return self._hostname_cache

    def invalidate_hostname(self) -> None:
        with self._hostname_lock:
            self._hostname_cache = None

    def dbbench_connection_flags(self) -> list[str]:
        assert self.new_app_password is not None
        return [
            "-driver",
            "postgres",
            "-host",
            self.composition.cloud_hostname(),
            "-port",
            "6875",
            "-username",
            self.username,
            "-password",
            self.new_app_password,
            "-database",
            "materialize",
            "-params",
            "sslmode=require&cluster=c",
        ]

    def initialize(self) -> None:
        print("Soft-disabling and then enabling region using Mz ...")
        cloud_disable_enable_and_wait(self)

        # Create new app password.
        new_app_password_name = "Materialize CLI (mz) - Cluster Spec Sheet"
        output = self.composition.run(
            "mz",
            "app-password",
            "create",
            new_app_password_name,
            capture=True,
            rm=True,
        )
        self.new_app_password = output.stdout.strip()
        assert (
            isinstance(self.new_app_password, str) and "mzp_" in self.new_app_password
        )

    def new_connection(
        self,
        *,
        dbname: str | None = None,
        cluster: str | None = None,
        statement_timeout_ms: int | None = None,
    ) -> psycopg.Connection:
        assert self.new_app_password is not None
        conn = psycopg.connect(
            host=self.host(),
            port=6875,
            user=self.username,
            password=self.new_app_password,
            dbname=dbname or "materialize",
            sslmode="require",
            options=_connection_options(cluster, statement_timeout_ms),
        )
        conn.autocommit = True
        return conn

    def cleanup(self) -> None:
        disable_region(self.composition, hard=True)

    def replica_size_for_scale(self, scale: int) -> str:
        """
        Returns the replica size for a given scale.
        """
        return f"{scale}00cc"


class DockerTarget(BenchTarget):
    def __init__(self, composition: Composition) -> None:
        self.composition = composition

    def dbbench_connection_flags(self) -> list[str]:
        return [
            "-driver",
            "postgres",
            "-host",
            "materialized",
            "-port",
            "6875",
            "-username",
            "materialize",
            "-database",
            "materialize",
            "-params",
            "sslmode=disable&cluster=c",
        ]

    def initialize(self) -> None:
        print("Starting local Materialize instance ...")
        self.composition.up("materialized")

    def new_connection(
        self,
        *,
        dbname: str | None = None,
        cluster: str | None = None,
        statement_timeout_ms: int | None = None,
    ) -> psycopg.Connection:
        startup_params: dict[str, str] = {}
        if cluster is not None:
            startup_params["cluster"] = cluster
        if statement_timeout_ms is not None:
            startup_params["statement_timeout"] = str(int(statement_timeout_ms))
        return self.composition.sql_connection(
            user="mz_system",
            port=6877,
            database=dbname or "materialize",
            startup_params=startup_params,
        )

    def cleanup(self) -> None:
        print("Stopping local Materialize instance ...")
        self.composition.stop("materialized")

    def replica_size_for_scale(self, scale: int) -> str:
        # 100cc == 2 workers
        return f"scale=1,workers={2*scale}"

    def max_scale(self) -> int | None:
        return 16


def run_scenario(
    scenario: Scenario,
    results_writer: csv.DictWriter,
    connection: ConnectionHandler,
    target: BenchTarget,
    max_scale: int,
    points: list[ScalePoint] | None = None,
    namespace: "Namespace | None" = None,
    write_lock: "threading.Lock | None" = None,
) -> None:
    """Run a `Scenario` end-to-end: prepare, sweep, teardown.

    The scenario decides what to sweep (cluster size, envd CPUs, object
    count) and how to measure each point; this driver just threads the
    lifecycle calls. ``apply`` returning False skips a single point;
    raising `_StopSweep` short-circuits the rest of the sweep.

    ``points`` restricts the sweep to a given subset (used by the parallel
    driver, which fans a scenario's points out into independent jobs).
    ``namespace`` rebinds the scenario + runner to a per-worker database /
    cluster names so concurrent jobs don't collide.
    """
    runner = ScenarioRunner(
        scenario.name(),
        scenario.VERSION,
        0,
        scenario.mode(),
        connection,
        results_writer,
        target=target,
        write_lock=write_lock,
    )
    if namespace is not None:
        scenario.apply_namespace(namespace)
        runner.cluster = namespace.cluster
        runner.lg_cluster = namespace.lg_cluster
        runner.log_prefix = namespace.log_prefix
    if points is None:
        points = list(scenario.scale_points(target, max_scale))
    scenario.prepare(runner)
    try:
        for point in points:
            print(
                f"{runner.log_prefix}--- {scenario.name()} ({scenario.mode()}): {point.label}"
            )
            try:
                proceed = scenario.apply(runner, point)
            except _StopSweep as e:
                print(f"    stopping sweep early: {e}")
                break
            if not proceed:
                continue
            try:
                scenario.measure(runner, point)
            finally:
                scenario.cleanup_point(runner, point)
    finally:
        scenario.teardown(runner)


@dataclass(frozen=True)
class ScenarioSpec:
    """Registry entry for a single scenario.

    ``factory`` builds the `Scenario` instance from CLI args + target.
    ``groups`` lists the ``--scenarios`` group names this scenario belongs to.
    The Scenario itself decides its result stream via ``stream_key()``.
    """

    name: str
    log_label: str
    factory: Callable[[argparse.Namespace, BenchTarget], Scenario]
    groups: tuple[str, ...] = ()


SCENARIOS: list[ScenarioSpec] = [
    ScenarioSpec(
        SCENARIO_TPCH_STRONG,
        "TPC-H Index strong scaling",
        lambda a, t: StrongScalingSweep(
            SCENARIO_TPCH_STRONG,
            TpchScenario(a.scale_tpch, t.replica_size_for_scale(1)),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_TPCH_MV_STRONG,
        "TPC-H Materialized view strong scaling",
        lambda a, t: StrongScalingSweep(
            SCENARIO_TPCH_MV_STRONG,
            TpchScenarioMV(a.scale_tpch, t.replica_size_for_scale(1)),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_TPCH_QUERIES_STRONG,
        "TPC-H Queries strong scaling",
        lambda a, t: StrongScalingSweep(
            SCENARIO_TPCH_QUERIES_STRONG,
            TpchScenarioQueriesIndexedInputs(
                a.scale_tpch_queries, t.replica_size_for_scale(1)
            ),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_TPCH_QUERIES_WEAK,
        "TPC-H Queries weak scaling",
        lambda a, t: WeakScalingSweep(
            SCENARIO_TPCH_QUERIES_WEAK,
            TpchScenarioQueriesIndexedInputs(a.scale_tpch_queries, None),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_AUCTION_STRONG,
        "Auction strong scaling",
        lambda a, t: StrongScalingSweep(
            SCENARIO_AUCTION_STRONG,
            AuctionScenario(a.scale_auction, t.replica_size_for_scale(1)),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_AUCTION_WEAK,
        "Auction weak scaling",
        lambda a, t: WeakScalingSweep(
            SCENARIO_AUCTION_WEAK,
            AuctionScenario(a.scale_auction, None),
        ),
        groups=("cluster", "cluster_compute"),
    ),
    ScenarioSpec(
        SCENARIO_SOURCE_INGESTION_STRONG,
        "Source ingestion scaling",
        lambda a, t: StrongScalingSweep(
            SCENARIO_SOURCE_INGESTION_STRONG,
            SourceIngestionScenario(a.scale_auction, t.replica_size_for_scale(1)),
            # Each point reads the same external Postgres/MySQL/Kafka sources;
            # keep the points serial within a single job rather than hammering
            # those external systems from concurrent jobs.
            split_points=False,
        ),
        groups=("cluster", "source_ingestion"),
    ),
    ScenarioSpec(
        SCENARIO_QPS_ENVD_STRONG_SCALING,
        "QPS envd strong scaling",
        lambda a, t: EnvdCpuSweep(
            SCENARIO_QPS_ENVD_STRONG_SCALING,
            QpsEnvdStrongScalingScenario(1, t.replica_size_for_scale(1)),
        ),
        groups=("envd_qps_scalability",),
    ),
    ScenarioSpec(
        SCENARIO_COPY_FROM_STDIN_ENVD_STRONG_SCALING,
        "COPY FROM STDIN envd strong scaling",
        lambda a, t: EnvdCpuSweep(
            SCENARIO_COPY_FROM_STDIN_ENVD_STRONG_SCALING,
            CopyFromStdinEnvdStrongScalingScenario(1, t.replica_size_for_scale(1)),
        ),
        groups=("envd_qps_scalability",),
    ),
    ScenarioSpec(
        SCENARIO_ENVD_OBJECTS_SCALABILITY_TABLES,
        "envd scalability (tables)",
        lambda a, t: EnvdObjectsSweep(
            SCENARIO_ENVD_OBJECTS_SCALABILITY_TABLES,
            EnvdObjectsScalabilityTablesScenario(),
            sizes=a.envd_objects_scalability_sizes,
        ),
        groups=("envd_objects_scalability",),
    ),
    ScenarioSpec(
        SCENARIO_ENVD_OBJECTS_SCALABILITY_MVS,
        "envd scalability (materialized views)",
        lambda a, t: EnvdObjectsSweep(
            SCENARIO_ENVD_OBJECTS_SCALABILITY_MVS,
            EnvdObjectsScalabilityMvsScenario(
                pad_replica_size=t.replica_size_for_scale(2),
            ),
            sizes=a.envd_objects_scalability_sizes,
        ),
        groups=("envd_objects_scalability",),
    ),
    ScenarioSpec(
        SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_PERSIST_SOURCES,
        "cluster object limits (indexes, from persist sources)",
        lambda a, t: ClusterObjectLimitsScenario(
            CLUSTER_OBJECT_LIMITS_INDEXES_FROM_PERSIST_SOURCES_KIND,
            sizes=a.cluster_object_limits_sizes,
            bisect_steps=a.cluster_object_limits_bisect_steps,
        ),
        groups=("cluster_object_limits",),
    ),
    ScenarioSpec(
        SCENARIO_CLUSTER_OBJECT_LIMITS_INDEXES_FROM_INDEX,
        "cluster object limits (indexes, from root index)",
        lambda a, t: ClusterObjectLimitsScenario(
            CLUSTER_OBJECT_LIMITS_INDEXES_FROM_INDEX_KIND,
            sizes=a.cluster_object_limits_sizes,
            bisect_steps=a.cluster_object_limits_bisect_steps,
        ),
        groups=("cluster_object_limits",),
    ),
    ScenarioSpec(
        SCENARIO_CLUSTER_OBJECT_LIMITS_MVS_FROM_PERSIST_SOURCES,
        "cluster object limits (materialized views, from persist sources)",
        lambda a, t: ClusterObjectLimitsScenario(
            CLUSTER_OBJECT_LIMITS_MVS_FROM_PERSIST_SOURCES_KIND,
            sizes=a.cluster_object_limits_sizes,
            bisect_steps=a.cluster_object_limits_bisect_steps,
        ),
        groups=("cluster_object_limits",),
    ),
]

SCENARIOS_BY_NAME: dict[str, ScenarioSpec] = {s.name: s for s in SCENARIOS}

# `SCENARIO_GROUPS` is the inverse of `ScenarioSpec.groups`, plus an "all"
# group covering every scenario. Used to expand `--scenarios <group>`.
SCENARIO_GROUPS: dict[str, list[str]] = {"all": [s.name for s in SCENARIOS]}
for _spec in SCENARIOS:
    for _g in _spec.groups:
        SCENARIO_GROUPS.setdefault(_g, []).append(_spec.name)
del _spec, _g


def workflow_plot(composition: Composition, parser: WorkflowArgumentParser) -> None:
    """Analyze the results of the workflow.

    Dispatches each result file to the right analyzer by filename suffix
    (`SUFFIX_ANALYZERS`). To plot only one kind, pass an explicit glob:
    ``workflow_plot 'results_*.cluster.csv'``.
    """
    parser.add_argument(
        "files",
        nargs="*",
        default=[f"results_*{suffix}" for suffix, _ in SUFFIX_ANALYZERS],
        type=str,
        help="Glob pattern(s) of result files to plot.",
    )
    args = parser.parse_args()
    for file in itertools.chain(*map(glob.iglob, args.files)):
        file_str = str(file)
        base_name = os.path.basename(file_str)
        for suffix, fn in SUFFIX_ANALYZERS:
            if base_name.endswith(suffix):
                fn(file_str)
                break
        else:
            raise UIError(
                f"Error: Filename '{file_str}' doesn't indicate which "
                "result-stream kind it belongs to. Expected one of these "
                f"suffixes: {', '.join(s for s, _ in SUFFIX_ANALYZERS)}."
            )


def extract_cluster_size(s: str) -> float:
    """Parse a `cluster_size` label (e.g. ``"100cc"``, ``"1600cc"``,
    ``"1C"``, ``"scale=1,workers=2"``) into a numeric credits/hour value
    suitable for ordering and as a continuous x-axis.
    """
    match = re.search(r"(\d+)(?:(cc)|(C))", s)
    if match:
        if match.group(2):  # 'cc' match
            return float(match.group(1)) / 100.0
        elif match.group(3):  # 'C' matches
            return float(match.group(1))
    match = re.search(r"(?:scale=)(\d+)(?:,workers=)(\d+)", s)
    if match:
        # We don't have credits in docker, so approximate it
        # 100cc == 2 workers
        if match.group(1) and match.group(2):
            return float(match.group(1)) * float(match.group(2)) / 2
    raise ValueError(f"Invalid cluster size format: {s}")


def _open_results_for_plotting(
    file: str, label: str
) -> tuple[pd.DataFrame, str] | None:
    """Header + empty-check + plot dir setup shared by every analyzer.

    Returns ``(df, plot_dir)`` or ``None`` if the file is empty / nothing
    further to plot.
    """
    print(f"--- Analyzing {label} results file {file} ...")
    df = pd.read_csv(file)
    if df.empty:
        print(f"^^^ +++ File {file} is empty, skipping")
        return None
    base_name = os.path.basename(file).split(".")[0]
    plot_dir = os.path.join("test", "cluster-spec-sheet", "plots", base_name)
    os.makedirs(plot_dir, exist_ok=True)
    return df, plot_dir


def analyze_cluster_results_file(file: str) -> None:
    loaded = _open_results_for_plotting(file, "cluster")
    if loaded is None:
        return
    df, plot_dir = loaded

    # Cluster replica size as credits/hour
    df["credits_per_h"] = df["cluster_size"].map(extract_cluster_size)
    # Cluster replica size as centi-credits/s
    df["ccredit_per_s"] = df["credits_per_h"] / 3600 * 100
    # Number of timely workers
    df["workers"] = round(df["credits_per_h"] * 1.9375)
    # Throughput in MiB/s
    df["throughput_mb_per_s"] = df["size_bytes"] / df["time_ms"] * 1000 / 1024 / 1024
    # Throughput in MiB/s/worker
    df["throughput_mb_per_s_worker"] = (
        df["size_bytes"] / 1024 / 1024 / df["time_ms"] * 1000 / df["workers"]
    )
    # Throughput in MiB/credit
    df["throughput_mb_per_credit"] = (
        df["size_bytes"] / 1024 / 1024 / df["time_ms"] * 1000 / df["ccredit_per_s"]
    )
    # Cost in centi-credits: ccredit/s * s
    df["credit_time"] = df["ccredit_per_s"] * df["time_ms"] / 1000.0

    df_all = df
    # Plot the results
    for index in (
        df_all[["scenario", "category", "mode"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        benchmark, category, mode = (index.scenario, index.category, index.mode)
        indexes = (
            (df_all["scenario"] == benchmark)
            & (df_all["category"] == category)
            & (df_all["mode"] == mode)
        )
        df = df_all[indexes]
        title = f"{str(benchmark).replace('_', ' ')} - {str(category).replace('_', ' ')} ({mode})"
        slug = f"{benchmark}_{category}_{mode}".replace(" ", "_")

        plot(
            plot_dir,
            df,
            "time_ms",
            f"{title} (time)",
            f"{slug}_time_ms",
            "Time [ms]",
            "Normalized time",
            x="credits_per_h",
        )
        plot(
            plot_dir,
            df,
            "credit_time",
            f"{title} (credits)",
            f"{slug}_credits",
            "Cost [centi-credits]",
            "Normalized cost",
            x="credits_per_h",
        )


def analyze_envd_results_file(file: str) -> None:
    loaded = _open_results_for_plotting(file, "envd")
    if loaded is None:
        return
    df, plot_dir = loaded

    # TODO: this might be need to be modified if we have more than one repetitions in an envd results file.
    for (benchmark, category, mode), sub in df.groupby(
        ["scenario", "category", "mode"]
    ):
        title = f"{str(benchmark).replace('_',' ')} - {str(category).replace('_',' ')} ({mode})"
        slug = f"{benchmark}_{category}_{mode}".replace(" ", "_")
        sub_q = sub[sub["qps"].notna() & (sub["qps"] > 0)]
        if sub_q.empty:
            raise UIError(f"No QPS data found for {title} in {file}")
        plot(
            plot_dir,
            sub_q,
            "qps",
            f"{title} (QPS)",
            f"{slug}_qps",
            "QPS",
            "Normalized QPS",
            x="envd_cpus",
        )


def analyze_envd_objects_scalability_results_file(file: str) -> None:
    """Plot adapter/envd latency vs catalog object count (N).

    Each (scenario, category) pair yields one plot, with `scale` (=N) as the
    x-axis (categorical bar chart, since the values span 1 to the configured
    cap — 30k by default via `--envd-objects-scalability-sizes`). The
    accompanying normalized plot is produced by the shared `plot()` helper.
    """
    loaded = _open_results_for_plotting(file, "envd_objects_scalability")
    if loaded is None:
        return
    df, plot_dir = loaded

    for (benchmark, category, mode), sub in df.groupby(
        ["scenario", "category", "mode"]
    ):
        title = f"{str(benchmark).replace('_',' ')} - {str(category).replace('_',' ')} ({mode})"
        slug = f"{benchmark}_{category}_{mode}".replace(" ", "_")
        sub_t = sub[sub["time_ms"].notna()]
        if sub_t.empty:
            print(f"Warning: No time data for {title} in {file}")
            continue
        plot(
            plot_dir,
            sub_t,
            "time_ms",
            f"{title} (time)",
            f"{slug}_time_ms",
            "Time [ms]",
            "Normalized time",
            x="scale",
        )


def analyze_cluster_object_limits_results_file(file: str) -> None:
    """Plot cluster_object_limits results.

    For each scenario produce two plots:
      - Max-healthy N vs cluster size (one bar per cluster size).
      - Max local lag (ms) vs N, with one series per cluster size — shows the
        cliff where freshness collapses.
    """
    loaded = _open_results_for_plotting(file, "cluster_object_limits")
    if loaded is None:
        return
    df, plot_dir = loaded

    for benchmark, sub in df.groupby("scenario"):
        title_base = str(benchmark).replace("_", " ")
        slug_base = str(benchmark)

        # Max-healthy N per cluster size.
        healthy = sub[sub["healthy"] == 1]
        if healthy.empty:
            print(
                f"Warning: No healthy data points for {benchmark} in {file}; "
                "skipping max-N plot"
            )
        else:
            max_n = healthy.groupby("cluster_size")["scale"].max()
            # Order by numeric cluster size (credits/h) so e.g. 1600cc sits
            # between 800cc and 3200cc rather than between 100cc and 200cc.
            max_n = max_n.reindex(sorted(max_n.index, key=extract_cluster_size))
            df_max_n = max_n.to_frame(name="max_healthy_N")
            ax = df_max_n.plot(
                kind="bar",
                figsize=(10, 6),
                ylabel="Max healthy N",
                title=f"{title_base} — max healthy N per cluster size",
                grid=True,
                legend=False,
            )
            ax.set_xlabel("cluster_size")
            save_plot(
                plot_dir,
                df_max_n,
                f"{title_base} max healthy N",
                f"{slug_base}_max_healthy_n",
            )

        # Lag vs N, one series per cluster size (shows the cliff).
        sub_t = sub[sub["time_ms"].notna()]
        if sub_t.empty:
            print(f"Warning: No lag data for {benchmark} in {file}")
            continue
        pivot = sub_t.pivot_table(
            index="scale",
            columns="cluster_size",
            values="time_ms",
            aggfunc="max",
        ).sort_index()
        filtered = pivot.dropna(axis=1, how="all")
        if filtered.empty:
            continue
        # Order the per-cluster-size series by numeric cluster size so the
        # legend reads small→large rather than alphanumerically.
        filtered = filtered[sorted(filtered.columns, key=extract_cluster_size)]
        ax = filtered.plot(
            kind="line",
            marker="o",
            figsize=(12, 6),
            ylabel=(
                f"Max local lag [ms]"
                f" (capped at {CLUSTER_OBJECT_LIMITS_LAG_CAP_MS} ms)"
            ),
            title=(
                f"{title_base} — freshness lag vs N "
                f"(values >{CLUSTER_OBJECT_LIMITS_LAG_CAP_MS} ms capped; "
                f"healthy threshold {CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS} ms)"
            ),
            grid=True,
        )
        ax.set_xlabel("N (number of materializations on cluster c)")
        ax.axhline(
            CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS,
            color="red",
            linestyle="--",
            linewidth=1,
            label=f"healthy threshold ({CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS} ms)",
        )
        ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
        save_plot(
            plot_dir,
            filtered,
            f"{title_base} lag vs N",
            f"{slug_base}_lag_vs_n",
        )


def save_plot(plot_dir: str, data_frame: pd.DataFrame, title: str, slug: str):
    all_files = []

    base_file_name = os.path.join(plot_dir, slug)
    file_path = f"{base_file_name}.svg"
    plt.savefig(MZ_ROOT / file_path, bbox_inches="tight")
    all_files.append(file_path)
    file_path = f"{base_file_name}.html"
    data_frame.to_html(MZ_ROOT / file_path)
    all_files.append(file_path)
    print(f"+++ Plot for {title}")
    print(data_frame.to_string())

    upload_file(all_files, title)


def upload_file(
    file_paths: list[str],
    title: str,
):
    if buildkite.is_in_buildkite():
        for file_path in file_paths:
            buildkite.upload_artifact(file_path, cwd=MZ_ROOT, quiet=True)
        print(f"+++ Plot for {title}")
        for file_path in file_paths:
            print(
                buildkite.inline_image(
                    f"artifact://{file_path}",
                    f"Plot for {title}",
                )
            )
    else:
        print(f"Saving plots to {file_paths}")


def plot(
    plot_dir: str,
    data: pd.DataFrame,
    value: str,
    title: str,
    slug: str,
    data_label: str,
    normalized_label: str,
    x: str,
):
    df2 = data.pivot_table(
        index=[x],
        columns=["test_name"],
        values=value,
        aggfunc="min",
    ).sort_index(axis=1)
    filtered = df2.dropna(axis=1, how="all")
    if filtered.empty:
        print(f"Warning: No data to plot for {title}")
        return
    filtered.index.name = x
    plot = filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel=data_label,
        logy=False,
        title=f"{title}",
        grid=True,
    )
    plot.set_xlabel(x)
    plot.legend(
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
    )
    save_plot(plot_dir, filtered, title, slug)

    filtered = filtered.div(filtered.iloc[0])
    plot = filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel=normalized_label,
        logy=False,
        title=f"{title}",
        grid=True,
    )
    plot.set_xlabel(x)
    plot.legend(
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
    )
    save_plot(plot_dir, filtered, f"{title} (Normalized)", f"{slug}_normalized")


def upload_cluster_results_to_test_analytics(
    composition: Composition,
    file: str,
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(composition))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            result_entries.append(
                cluster_spec_sheet_result_storage.ClusterSpecSheetResultEntry(
                    scenario=row["scenario"],
                    scenario_version=row["scenario_version"],
                    scale=int(row["scale"]),
                    mode=row["mode"],
                    category=row["category"],
                    test_name=row["test_name"],
                    cluster_size=row["cluster_size"],
                    repetition=int(row["repetition"]),
                    size_bytes=int(row["size_bytes"]) if row["size_bytes"] else None,
                    time_ms=int(row["time_ms"]) if row["time_ms"] else None,
                )
            )

    test_analytics.cluster_spec_sheet_results.add_result(
        framework_version=CLUSTER_SPEC_SHEET_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def upload_environmentd_results_to_test_analytics(
    composition: Composition,
    file: str,
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(composition))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            result_entries.append(
                cluster_spec_sheet_result_storage.ClusterSpecSheetEnvironmentdResultEntry(
                    scenario=row["scenario"],
                    scenario_version=row["scenario_version"],
                    scale=int(row["scale"]),
                    mode=row["mode"],
                    category=row["category"],
                    test_name=row["test_name"],
                    envd_cpus=int(row["envd_cpus"]),
                    repetition=int(row["repetition"]),
                    qps=float(row["qps"]) if row["qps"] else None,
                )
            )

    test_analytics.cluster_spec_sheet_environmentd_results.add_result(
        framework_version=CLUSTER_SPEC_SHEET_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


@dataclass(frozen=True)
class ResultStreamSpec:
    """A result-file kind: suffix, CSV schema, analyzer, and uploader."""

    key: str
    suffix: str
    fieldnames: list[str]
    analyze: Callable[[str], None]
    upload: Callable[[Composition, str, bool], None]


# Order matters for suffix dispatch: longer suffixes first so
# `.cluster_object_limits.csv` isn't shadowed by `.cluster.csv`.
RESULT_STREAMS: list[ResultStreamSpec] = [
    ResultStreamSpec(
        "cluster_object_limits",
        ".cluster_object_limits.csv",
        CLUSTER_FIELDNAMES + ["healthy", "failure_mode"],
        analyze_cluster_object_limits_results_file,
        upload_cluster_results_to_test_analytics,
    ),
    ResultStreamSpec(
        "envd_objects",
        ".envd_objects_scalability.csv",
        CLUSTER_FIELDNAMES,
        analyze_envd_objects_scalability_results_file,
        upload_cluster_results_to_test_analytics,
    ),
    ResultStreamSpec(
        "cluster",
        ".cluster.csv",
        CLUSTER_FIELDNAMES,
        analyze_cluster_results_file,
        upload_cluster_results_to_test_analytics,
    ),
    ResultStreamSpec(
        "envd",
        ".envd.csv",
        ENVD_FIELDNAMES,
        analyze_envd_results_file,
        upload_environmentd_results_to_test_analytics,
    ),
]

RESULT_STREAMS_BY_KEY: dict[str, ResultStreamSpec] = {s.key: s for s in RESULT_STREAMS}

# Back-compat alias for `_plot_files` / `workflow_plot`, which dispatch by
# filename suffix.
SUFFIX_ANALYZERS: list[tuple[str, Callable[[str], None]]] = [
    (s.suffix, s.analyze) for s in RESULT_STREAMS
]


@dataclass
class OpenStream:
    """An opened CSV result file together with its writer and spec."""

    spec: ResultStreamSpec
    path: str
    file: TextIO
    writer: csv.DictWriter


def _open_stream(spec: ResultStreamSpec, base_name: str) -> OpenStream:
    path = f"{base_name}{spec.suffix}"
    file = open(path, "w", newline="")
    writer = _make_csv_writer(file, spec.fieldnames)
    return OpenStream(spec, path, file, writer)
