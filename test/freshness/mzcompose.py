# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Freshness under contention.

A cheap object that is trivial to keep up to date ("the probe") must stay fresh
even while a much more expensive object on the *same cluster* does heavy work
(initial hydration, steady-state ingestion, or rehydration after a restart).

Both the probe and the expensive object live `IN CLUSTER test`, so they share
the same timely/tokio workers. If an expensive dataflow fails to yield while
hydrating a large arrangement, it starves the workers and the probe's write
frontier stops advancing -- its freshness (wallclock lag) spikes. A healthy,
cooperatively-scheduled system keeps the probe fresh throughout.

Matrix dimensions:

  * cluster_sizes  -- start at the smallest (`scale=1,workers=1`); one worker is
    the hardest case for the scheduler, since there is nowhere else for the
    probe's work to run.
  * expensive_ops  -- which operator the expensive object exercises (see
    EXPENSIVE_OPS): index build, reduce, distinct, top-k, join.
  * probe_kind     -- the cheap object kind we monitor (index, materialized-view,
    or a Kafka/PG/MySQL/SQL-Server CDC source/sink). Decoupled from the expensive
    op so we can tell apart compute starvation from persist-write starvation, and
    whether co-located compute starves a *source's* ingestion freshness too.
  * actions        -- what the expensive object does while we watch the probe:
      - steady_state : no expensive object at all (control / calibration).
      - hydration    : build the expensive object from a large snapshot.
      - ingestion    : expensive object exists; append-only input churn.
      - retraction   : expensive object exists; retract+reinsert the hot key's
                       maximum (steady-state churn with retractions). Combine
                       with --skew to make the hot group huge.
      - rehydration  : drop+recreate the replica so everything rehydrates from
                       persist; the probe must recover fresh quickly AND stay
                       fresh, well before the expensive object finishes.

The probe is sampled over a *separate* connection on the default cluster, so the
measurement never competes for the workers under test. We record both:
  - peak lag: `now() - write_frontier` (ms), reported as context but confounded
    by how long the expensive work runs; and
  - max stall: the longest wallclock interval the probe's write_frontier stayed
    frozen -- the duration-independent starvation signal we assert on. (The
    user-facing lag view is `mz_internal.mz_wallclock_global_lag`.)

Empirical finding (see test/freshness/FINDINGS.md): on a single worker, a large
hierarchical (min/max) reduce or top-k hydration starves co-located cheap
objects for many seconds, while accumulable reduces (count/sum), distinct, join,
and a plain index build do not.
"""

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from threading import Event

import psycopg

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.ui import UIError
from materialize.util import PropagatingThread

# Minio + Toxiproxy (workflow_storage) and Kafka (workflow_ingestion) are only
# started by their respective workflows -- they are not deps of the default
# Materialized, so `workflow_default` never brings them up. They are declared
# here so `c.up()` recognizes them.
# Host-reachable Kafka port. The broker advertises two listeners: HOST (for the
# producer running on the host) and PLAINTEXT (for MZ inside the compose net).
# Without the HOST listener a host client gets redirected to `kafka:9092`, which
# it cannot resolve.
KAFKA_HOST_PORT = 30123

SERVICES = [
    Materialized(),
    Minio(setup_materialize=True),
    Toxiproxy(),
    Kafka(
        ports=[f"{KAFKA_HOST_PORT}:{KAFKA_HOST_PORT}"],
        allow_host_ports=True,
        advertised_listeners=[
            f"HOST://127.0.0.1:{KAFKA_HOST_PORT}",
            "PLAINTEXT://kafka:9092",
            # An extra listener whose advertised address is toxiproxy: a client
            # connecting here gets redirected to toxiproxy:9095, so MZ's sink
            # emit path can be routed through the proxy and throttled (the
            # PLAINTEXT path the other workflows use is untouched).
            "TOXI://toxiproxy:9095",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
            "CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT,TOXI:PLAINTEXT",
        ],
    ),
    # CDC upstreams (pg-source / mysql-source / sql-server-source probes).
    # Postgres defaults to wal_level=logical and MySQL to GTID+row binlog, so
    # both are CDC-ready out of the box; SQL Server enables CDC per-table below.
    Postgres(),
    MySql(),
    SqlServer(),
]

# The upstream CDC table each *-source probe replicates (same name everywhere).
CDC_TABLE = "cdc_t"
MYSQL_ROOT_PASSWORD = MySql.DEFAULT_ROOT_PASSWORD
SQL_SERVER_SA_PASSWORD = SqlServer.DEFAULT_SA_PASSWORD

# ---------------------------------------------------------------------------
# Matrix dimensions. Kept as module-level lists/dicts so they are trivial to
# extend; the defaults below are also overridable from the command line.
# ---------------------------------------------------------------------------

# Start at the smallest cluster: a single worker is the hardest case for the
# scheduler, since there is nowhere else for the probe's work to run.
CLUSTER_SIZES = ["scale=1,workers=1"]

# Each expensive op is a single object created `IN CLUSTER test` over the `big`
# table. `index` is the only non-persisted (compute-only) op; the rest are
# materialized views that also write their output to persist.
EXPENSIVE_OPS: dict[str, tuple[str, str]] = {
    "index": (
        "CREATE INDEX expensive_idx IN CLUSTER test ON big (key, id)",
        "expensive_idx",
    ),
    "reduce": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, count(*) AS n, max(id) AS m FROM big GROUP BY key",
        "expensive_mv",
    ),
    # Single-aggregate reduces, to isolate which reduce machinery starves:
    # count/sum are accumulable, max is hierarchical (bucketed min/max),
    # count(DISTINCT) uses the distinct-then-accumulate plan.
    "count": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, count(*) AS n FROM big GROUP BY key",
        "expensive_mv",
    ),
    "sum": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, sum(id) AS s FROM big GROUP BY key",
        "expensive_mv",
    ),
    "max": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, max(id) AS m FROM big GROUP BY key",
        "expensive_mv",
    ),
    "min": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, min(id) AS m FROM big GROUP BY key",
        "expensive_mv",
    ),
    "minmax": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, min(id) AS lo, max(id) AS hi FROM big GROUP BY key",
        "expensive_mv",
    ),
    # avg/stddev are accumulable; array_agg/string_agg are "basic" (collation)
    # reduces that gather all values per group.
    "avg": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, avg(id) AS a FROM big GROUP BY key",
        "expensive_mv",
    ),
    "stddev": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, stddev(id) AS sd FROM big GROUP BY key",
        "expensive_mv",
    ),
    "array_agg": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, array_agg(id) AS arr FROM big GROUP BY key",
        "expensive_mv",
    ),
    "string_agg": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, string_agg(id::text, ',') AS s FROM big GROUP BY key",
        "expensive_mv",
    ),
    "count_distinct": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, count(DISTINCT id) AS d FROM big GROUP BY key",
        "expensive_mv",
    ),
    "distinct": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT DISTINCT key FROM big",
        "expensive_mv",
    ),
    "topk": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, id FROM ("
        "  SELECT key, id, row_number() OVER "
        "    (PARTITION BY key ORDER BY id DESC) AS rn FROM big"
        ") WHERE rn <= 1",
        "expensive_mv",
    ),
    "join": (
        # `id` is unique, so this self-join is 1:1 (bounded output) but still
        # builds two arrangements and a join operator.
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT count(*) AS n FROM big a JOIN big b USING (id)",
        "expensive_mv",
    ),
    # WITH MUTUALLY RECURSIVE: a long fixpoint iteration (tiny data, many
    # rounds). Tests whether the recursive operator yields during its
    # per-timestamp iteration the way the reduce fails to. Independent of `big`.
    "wmr": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "WITH MUTUALLY RECURSIVE it (n bigint) AS ("
        "  SELECT 0 UNION SELECT n + 1 FROM it WHERE n < 50000"
        ") SELECT max(n) AS n FROM it",
        "expensive_mv",
    ),
    # Window functions and FlatMap: do other "process a whole partition / expand
    # a whole row in one step" operators share the reduce/top-k non-yielding
    # shape? Most revealing under --skew (one giant partition).
    "window_lag": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, lag(id) OVER (PARTITION BY key ORDER BY id) AS l FROM big",
        "expensive_mv",
    ),
    "window_sum": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, sum(id) OVER (PARTITION BY key ORDER BY id) AS s FROM big",
        "expensive_mv",
    ),
    "window_rank": (
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, rank() OVER (PARTITION BY key ORDER BY id) AS r FROM big",
        "expensive_mv",
    ),
    "unnest": (
        # FlatMap: expand each row to 3.
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, unnest(ARRAY[id, id + 1, id + 2]) AS u FROM big",
        "expensive_mv",
    ),
    # Completeness ops: confirm which family each lowers to.
    "join_blowup": (
        # Many-to-many join (large intermediate), counted so output is bounded.
        # Join is fuel-limited (LINEAR_JOIN_YIELDING) -> expected fresh.
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT count(*) AS n FROM big a JOIN big b "
        "ON a.key % 20000 = b.key % 20000",
        "expensive_mv",
    ),
    "distinct_on": (
        # DISTINCT ON = top-1 per group -> top-k family (expect window-family
        # starvation on a large partition).
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT DISTINCT ON (key) key, id FROM big ORDER BY key, id DESC",
        "expensive_mv",
    ),
    "except_op": (
        # Set difference -> distinct/negate reduce.
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, id FROM big EXCEPT SELECT key, id FROM big WHERE id % 2 = 0",
        "expensive_mv",
    ),
    "lateral": (
        # Correlated subquery -> decorrelates to join + max reduce (expect to
        # inherit the hierarchical-reduce starvation).
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT a.key, (SELECT max(b.id) FROM big b WHERE b.key = a.key) AS m "
        "FROM big a",
        "expensive_mv",
    ),
}

PROBE_KINDS = [
    "index",
    "materialized-view",
    "kafka-source",
    "kafka-sink",
    "pg-source",
    "mysql-source",
    "sql-server-source",
]
ACTIONS = [
    "steady_state",
    "hydration",
    "ingestion",
    "retraction",
    "big_txn",
    "rehydration",
]

# Actions that mutate the shared `big` table in place (append / retract / bulk
# DML). After one of these runs, `big` must be rebuilt before the next scenario
# that reads it, or the matrix would not be hermetic -- a later scenario would
# see a `big` of a different size/shape and results would depend on the order in
# which the matrix happened to run. (hydration / rehydration only read `big`.)
MUTATING_ACTIONS = {"ingestion", "retraction", "big_txn"}

# Default matrix over the expensive op.
DEFAULT_EXPENSIVE_OPS = ["index", "reduce"]

# ---------------------------------------------------------------------------
# Tunables (defaults; see workflow_default for the matching CLI flags).
# ---------------------------------------------------------------------------

# Rows in the `big` table the expensive object is built over. Sized so that
# hydrating the expensive object on one worker takes clearly longer than the
# freshness threshold -- that gap is what separates "fresh" from "starved".
DEFAULT_ROWS = 5_000_000
# How distinct `key`s `big` has, i.e. how large a keyed arrangement is.
DISTINCT_KEYS = 1_000_000
# A probe lagging more than this is considered "not yet fresh" (warmup/recovery).
DEFAULT_FRESHNESS_THRESHOLD_MS = 5_000
# Assertion threshold: the probe's frontier may not stay frozen longer than this.
DEFAULT_MAX_STALL_MS = 5_000
# Length of the monitored window for steady_state / ingestion / retraction.
DEFAULT_WINDOW_SECONDS = 30
# After a restart, the probe must return to fresh within this budget.
DEFAULT_RECOVERY_BUDGET_MS = 15_000
# Hard timeout for any single hydration.
DEFAULT_HYDRATION_TIMEOUT_S = 900
# Default probe kind.
DEFAULT_PROBE_KIND = "materialized-view"

PROBE_INTERVAL_S = 0.2
WRITER_INTERVAL_S = 0.2

# The write frontier can sit slightly *ahead* of now() when an object is
# perfectly fresh, which would make the raw difference negative; clamp at 0 so
# "lag" is always a non-negative staleness.
LAG_SQL = (
    "SELECT greatest(0, (extract(epoch FROM now()) * 1000)::bigint "
    "                   - write_frontier::text::bigint) AS lag_ms "
    "FROM mz_internal.mz_frontiers WHERE object_id = '{object_id}'"
)

# Same, but also returns the raw write frontier so the monitor can measure how
# long the frontier stayed *frozen* (the duration-independent starvation
# signal), not just the peak lag (which is confounded by how long the expensive
# work runs).
MONITOR_SQL = (
    "SELECT greatest(0, (extract(epoch FROM now()) * 1000)::bigint "
    "                   - write_frontier::text::bigint) AS lag_ms, "
    "       write_frontier::text::bigint AS frontier "
    "FROM mz_internal.mz_frontiers WHERE object_id = '{object_id}'"
)


def _connect(c: Composition) -> psycopg.Connection:
    """A fresh, autocommit connection on the default cluster (not `test`)."""
    return psycopg.connect(
        host="localhost",
        port=c.default_port("materialized"),
        user="materialize",
        dbname="materialize",
        autocommit=True,
    )


class FreshnessMonitor:
    """Background sampler of a probe's lag and write frontier."""

    def __init__(self, c: Composition, object_id: str):
        self.c = c
        self.object_id = object_id
        self._stop = Event()
        # (wallclock, lag_ms, frontier); lag/frontier are None when no replica.
        self.samples: list[tuple[float, int | None, int | None]] = []
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            sql = MONITOR_SQL.format(object_id=self.object_id)
            while not self._stop.is_set():
                cur.execute(sql.encode())
                row = cur.fetchone()
                lag = row[0] if row else None
                frontier = row[1] if row else None
                self.samples.append((time.time(), lag, frontier))
                self._stop.wait(PROBE_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def max_lag(self, since: float = 0.0) -> int | None:
        lags = [lag for t, lag, _ in self.samples if t >= since and lag is not None]
        return max(lags) if lags else None

    def max_stall_ms(self, since: float = 0.0) -> int | None:
        """Longest wallclock interval the write frontier failed to advance.

        This is the duration-independent starvation signal: a perfectly fresh
        probe advances its frontier every oracle tick (~1 s), so a stall well
        beyond that means the workers were busy elsewhere. Unlike peak lag it
        does not grow just because the expensive work ran longer, and it
        captures repeated short stalls in steady state, not only one big one.
        A frozen / absent frontier (None) counts as not advancing.
        """
        samples = [(t, f) for t, _, f in self.samples if t >= since]
        if not samples:
            return None
        last_change_t = samples[0][0]
        last_frontier = samples[0][1]
        worst = 0.0
        for t, f in samples[1:]:
            advanced = f is not None and (last_frontier is None or f > last_frontier)
            if advanced:
                last_frontier = f
                last_change_t = t
            else:
                worst = max(worst, t - last_change_t)
        return int(worst * 1000)


class KafkaProducerThread:
    """Continuously produce to a Kafka topic from the host.

    A steady arrival rate gives the Kafka source a moving target: in a healthy
    system its reclocked write frontier tracks wallclock (~oracle interval
    behind); if the source dataflow is starved, the frontier freezes and the
    ingestion lag grows.
    """

    def __init__(
        self,
        c: Composition,
        topic: str,
        msgs_per_tick: int = 500,
        keyspace: int | None = None,
    ):
        self.c = c
        self.topic = topic
        self.msgs_per_tick = msgs_per_tick
        # None => every message a unique key (append). An int => keys cycle in
        # [0, keyspace), so for an UPSERT source small keyspace = hot updates,
        # large keyspace = state growth.
        self.keyspace = keyspace
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        import confluent_kafka  # type: ignore

        producer = confluent_kafka.Producer(
            {"bootstrap.servers": f"127.0.0.1:{KAFKA_HOST_PORT}"}
        )
        i = 0
        while not self._stop.is_set():
            for _ in range(self.msgs_per_tick):
                key = i if self.keyspace is None else (i % self.keyspace)
                producer.produce(
                    self.topic, key=str(key).encode(), value=str(i).encode()
                )
                i += 1
            producer.flush()
            self._stop.wait(0.1)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def _pg_connect(c: Composition) -> psycopg.Connection:
    """Host connection to the upstream Postgres (the CDC source's origin)."""
    return psycopg.connect(
        host="localhost",
        port=c.default_port("postgres"),
        user="postgres",
        password="postgres",
        dbname="postgres",
        autocommit=True,
    )


class PgCdcWriter:
    """Continuously INSERT into the upstream Postgres table.

    Mirrors KafkaProducerThread but for a Postgres CDC source: each insert is a
    logical-replication event MZ must ingest. A healthy CDC source reclocks LSNs
    onto the wallclock oracle, so its write frontier tracks now().
    """

    def __init__(self, c: Composition, table: str, rows_per_tick: int = 500):
        self.c = c
        self.table = table
        self.rows_per_tick = rows_per_tick
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _pg_connect(self.c)
        try:
            cur = conn.cursor()
            i = 0
            while not self._stop.is_set():
                cur.execute(
                    f"INSERT INTO {self.table} "
                    f"SELECT g.s FROM generate_series("
                    f"{i + 1}, {i + self.rows_per_tick}) AS g(s)".encode()
                )
                i += self.rows_per_tick
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def _ensure_pg_cdc(c: Composition) -> None:
    """Create the upstream PG table + publication (idempotent, top-level)."""
    conn = _pg_connect(c)
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {CDC_TABLE} (val bigint)".encode())
        cur.execute(f"ALTER TABLE {CDC_TABLE} REPLICA IDENTITY FULL".encode())
        cur.execute(b"DROP PUBLICATION IF EXISTS mz_source")
        cur.execute(f"CREATE PUBLICATION mz_source FOR TABLE {CDC_TABLE}".encode())
    finally:
        conn.close()


def _pg_reset_table(c: Composition) -> None:
    """Truncate the upstream table so a fresh source snapshots ~nothing."""
    conn = _pg_connect(c)
    try:
        conn.cursor().execute(f"TRUNCATE {CDC_TABLE}".encode())
    finally:
        conn.close()


def _mysql_connect(c: Composition):
    import pymysql  # type: ignore

    return pymysql.connect(
        host="127.0.0.1",
        port=c.default_port("mysql"),
        user="root",
        password=MYSQL_ROOT_PASSWORD,
        database="public",
        autocommit=True,
    )


class MySqlCdcWriter:
    """Continuously INSERT into the upstream MySQL table (binlog CDC events)."""

    def __init__(self, c: Composition, rows_per_tick: int = 500):
        self.c = c
        self.rows_per_tick = rows_per_tick
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _mysql_connect(self.c)
        try:
            cur = conn.cursor()
            i = 0
            while not self._stop.is_set():
                # MySQL has no generate_series; a small multi-row VALUES list.
                rows = ",".join(f"({i + j})" for j in range(1, self.rows_per_tick + 1))
                cur.execute(f"INSERT INTO {CDC_TABLE} (val) VALUES {rows}")
                i += self.rows_per_tick
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def _ensure_mysql_cdc(c: Composition) -> None:
    """Create the upstream MySQL database + table (idempotent)."""
    import pymysql  # type: ignore

    conn = pymysql.connect(
        host="127.0.0.1",
        port=c.default_port("mysql"),
        user="root",
        password=MYSQL_ROOT_PASSWORD,
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS public")
        cur.execute(f"CREATE TABLE IF NOT EXISTS public.{CDC_TABLE} (val bigint)")
    finally:
        conn.close()


def _mysql_reset_table(c: Composition) -> None:
    conn = _mysql_connect(c)
    try:
        conn.cursor().execute(f"TRUNCATE {CDC_TABLE}")
    finally:
        conn.close()


def _sql_server_exec(c: Composition, sql: str, db: str = "master") -> None:
    """Run a batch via sqlcmd inside the sql-server container (no host driver).

    The tools live at mssql-tools18 on newer images, mssql-tools on older ones;
    pick whichever exists (mirrors the service's own healthcheck).
    """
    script = (
        "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
        '[ -x "$SQLCMD" ] || SQLCMD=/opt/mssql-tools/bin/sqlcmd; '
        f'"$SQLCMD" -C -b -S localhost -U sa -P "{SQL_SERVER_SA_PASSWORD}" '
        f'-d {db} -Q "{sql}"'
    )
    c.exec("sql-server", "bash", "-c", script)


def _ensure_sql_server_cdc(c: Composition) -> None:
    """Create the DB + table and enable CDC (DB-level then table-level).

    ALLOW_SNAPSHOT_ISOLATION is required by MZ's SQL Server source; the SQL
    Agent (enabled by default on the service) runs the CDC capture jobs that
    advance the source's LSN.
    """
    _sql_server_exec(c, "IF DB_ID('cdc_db') IS NULL CREATE DATABASE cdc_db;")
    _sql_server_exec(c, "ALTER DATABASE cdc_db SET ALLOW_SNAPSHOT_ISOLATION ON;")
    _sql_server_exec(c, "EXEC sys.sp_cdc_enable_db;", db="cdc_db")
    _sql_server_exec(
        c,
        f"IF OBJECT_ID('dbo.{CDC_TABLE}') IS NULL "
        f"CREATE TABLE dbo.{CDC_TABLE} (val bigint);",
        db="cdc_db",
    )
    _sql_server_exec(
        c,
        f"IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='{CDC_TABLE}' "
        f"AND is_tracked_by_cdc=1) "
        f"EXEC sys.sp_cdc_enable_table @source_schema='dbo', "
        f"@source_name='{CDC_TABLE}', @role_name=NULL, @supports_net_changes=0;",
        db="cdc_db",
    )


def _sql_server_reset_table(c: Composition) -> None:
    # SQL Server forbids TRUNCATE on a CDC-tracked table, so DELETE instead.
    _sql_server_exec(c, f"DELETE FROM dbo.{CDC_TABLE};", db="cdc_db")


class SqlServerCdcWriter:
    """Continuously INSERT into the upstream SQL Server table via sqlcmd.

    No Python driver is available, so each tick shells into the container. The
    per-call overhead caps the rate, but it keeps a steady CDC stream flowing --
    enough to exercise the source's frontier advancement.
    """

    def __init__(self, c: Composition, rows_per_tick: int = 200):
        self.c = c
        self.rows_per_tick = rows_per_tick
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        i = 0
        while not self._stop.is_set():
            values = ",".join(f"({i + j})" for j in range(1, self.rows_per_tick + 1))
            _sql_server_exec(
                self.c,
                f"INSERT INTO dbo.{CDC_TABLE} (val) VALUES {values};",
                db="cdc_db",
            )
            i += self.rows_per_tick
            self._stop.wait(WRITER_INTERVAL_S)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def _ensure_topic(c: Composition, topic: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore

    admin = AdminClient({"bootstrap.servers": f"127.0.0.1:{KAFKA_HOST_PORT}"})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for fut in futures.values():
        try:
            fut.result()
        except Exception:
            pass  # already exists


def _produce_backlog(c: Composition, topic: str, count: int) -> None:
    """Synchronously produce a backlog of `count` messages, then flush."""
    import confluent_kafka  # type: ignore

    producer = confluent_kafka.Producer(
        {
            "bootstrap.servers": f"127.0.0.1:{KAFKA_HOST_PORT}",
            "queue.buffering.max.messages": 2_000_000,
        }
    )
    for i in range(count):
        producer.produce(topic, key=str(i).encode(), value=str(i).encode())
        if i % 100_000 == 0:
            producer.poll(0)
    producer.flush()


class ProbeWriter:
    """Continuous writes so the probe always has something to refresh.

    rows_per_batch=1 is the light default; a larger batch drives real write
    volume (used by the storage-throughput experiment).
    """

    def __init__(self, c: Composition, table: str, rows_per_batch: int = 1):
        self.c = c
        self.table = table
        self.rows_per_batch = rows_per_batch
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            i = 0
            while not self._stop.is_set():
                if self.rows_per_batch == 1:
                    cur.execute(f"INSERT INTO {self.table} VALUES ({i})".encode())
                    i += 1
                else:
                    cur.execute(
                        f"INSERT INTO {self.table} "
                        f"SELECT g.s FROM generate_series("
                        f"{i + 1}, {i + self.rows_per_batch}) AS g(s)".encode()
                    )
                    i += self.rows_per_batch
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


class BigWriter:
    """Heavy continuous ingestion into the expensive object's input."""

    def __init__(self, c: Composition, rows_per_batch: int):
        self.c = c
        self.rows_per_batch = rows_per_batch
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            # Continue `id` past the existing rows so it stays unique.
            cur.execute(b"SELECT coalesce(max(id), 0) FROM big")
            row = cur.fetchone()
            base = row[0] if row else 0
            while not self._stop.is_set():
                cur.execute(
                    f"INSERT INTO big "
                    f"SELECT (g.s % {DISTINCT_KEYS})::bigint, g.s, repeat('x', 16) "
                    f"FROM generate_series({base + 1}, {base + self.rows_per_batch}) "
                    f"AS g(s)".encode()
                )
                base += self.rows_per_batch
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


@dataclass
class Result:
    size: str
    expensive_op: str
    probe_kind: str
    action: str
    threshold_ms: int
    # Primary, duration-independent signal: longest interval the frontier was
    # frozen. For rehydration this is measured *after* first recovery.
    max_stall_ms: int | None = None
    # Peak lag over the window; reported as context (confounded by duration).
    max_lag_ms: int | None = None
    recovery_ms: int | None = None
    expensive_ms: int | None = None
    passed: bool = False
    detail: str = ""


def _ms(v: int | None) -> str:
    """Format an optional millisecond value for the summary tables."""
    return "-" if v is None else f"{v}ms"


def _object_id(c: Composition, name: str) -> str:
    rows = c.sql_query(
        f"SELECT id FROM mz_objects WHERE name = '{name}' AND id LIKE 'u%'"
    )
    if not rows:
        raise UIError(f"could not find object {name!r}")
    return rows[0][0]


def _lag_ms(c: Composition, object_id: str) -> int | None:
    rows = c.sql_query(LAG_SQL.format(object_id=object_id))
    return rows[0][0] if rows and rows[0][0] is not None else None


def _wait_hydrated(c: Composition, object_id: str, timeout: float) -> float:
    """Block until *object_id* is hydrated on its replica; return elapsed secs."""
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        rows = c.sql_query(
            "SELECT bool_and(hydrated) "
            "FROM mz_internal.mz_compute_hydration_statuses "
            f"WHERE object_id = '{object_id}'"
        )
        if rows and rows[0][0] is True:
            return time.time() - start
        time.sleep(0.5)
    raise UIError(f"object {object_id} did not hydrate within {timeout}s")


def _wait_fresh(
    c: Composition, object_id: str, threshold_ms: int, timeout: float
) -> float:
    """Block until *object_id*'s lag drops below threshold; return elapsed secs."""
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        lag = _lag_ms(c, object_id)
        if lag is not None and lag <= threshold_ms:
            return time.time() - start
        time.sleep(PROBE_INTERVAL_S)
    raise UIError(
        f"object {object_id} did not become fresh (<= {threshold_ms}ms) "
        f"within {timeout}s"
    )


HOT_KEY = 0


def _create_big(c: Composition, rows: int, skew: float = 0.0) -> None:
    """(Re)create and populate the `big` input table the expensive op uses.

    `key` is low-cardinality (DISTINCT_KEYS groups) for reduce/distinct/top-k;
    `id` is unique for a 1:1 join. With `skew` in (0,1], that fraction of rows
    is assigned to the single hot key 0, creating one very large group (the
    worst case for per-update maintenance of a hierarchical reduce).
    """
    hot = max(0, min(100, int(round(skew * 100))))
    key_expr = (
        f"(g.s % {DISTINCT_KEYS})::bigint"
        if hot == 0
        else f"(CASE WHEN g.s % 100 < {hot} THEN {HOT_KEY} "
        f"ELSE (g.s % {DISTINCT_KEYS}) END)::bigint"
    )
    c.sql("DROP TABLE IF EXISTS big CASCADE")
    c.sql("CREATE TABLE big (key bigint, id bigint, payload text)")
    chunk = 500_000
    done = 0
    while done < rows:
        n = min(chunk, rows - done)
        c.sql(
            f"INSERT INTO big "
            f"SELECT {key_expr}, g.s, repeat('x', 16) "
            f"FROM generate_series({done + 1}, {done + n}) AS g(s)",
            print_statement=False,
        )
        done += n


class BigChurner:
    """Steady-state retraction churn on the hot key.

    Each iteration retracts the hot group's current maximum `id` and inserts a
    replacement with a smaller (negative, decreasing) `id`, so the group size
    stays roughly constant but the maximum keeps getting retracted -- forcing a
    hierarchical max/min reduce to re-scan the (large, if skewed) group every
    time. This is the steady-state analogue of the hydration starvation.
    """

    def __init__(self, c: Composition, hot_key: int = HOT_KEY):
        self.c = c
        self.hot_key = hot_key
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            i = 1
            while not self._stop.is_set():
                cur.execute(
                    f"DELETE FROM big WHERE key = {self.hot_key} "
                    f"AND id = (SELECT max(id) FROM big WHERE key = {self.hot_key})".encode()
                )
                cur.execute(
                    f"INSERT INTO big VALUES ({self.hot_key}, {-i}, repeat('x', 16))".encode()
                )
                i += 1
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


# Compute probes (index/MV) hydrate via mz_compute_hydration_statuses and need
# a ProbeWriter feeding probe_src. Ingestion probes (kafka-*) are not compute
# objects (no compute hydration row), so warmup waits on freshness alone.
COMPUTE_PROBE_KINDS = ("index", "materialized-view")
KAFKA_PROBE_KINDS = ("kafka-source", "kafka-sink")
# CDC sources, one per upstream engine.
CDC_PROBE_KINDS = ("pg-source", "mysql-source", "sql-server-source")


def _probe_is_compute(probe_kind: str) -> bool:
    return probe_kind in COMPUTE_PROBE_KINDS


def _probe_is_kafka(probe_kind: str) -> bool:
    return probe_kind in KAFKA_PROBE_KINDS


def _probe_is_cdc(probe_kind: str) -> bool:
    return probe_kind in CDC_PROBE_KINDS


def _make_probe_writer(
    c: Composition, probe_kind: str, topic: str | None, msgs_per_tick: int = 500
):
    """The thing that keeps the probe's input moving for the whole scenario.

    Compute probes -> writes to probe_src; kafka probes -> a Kafka producer on
    `topic`; CDC probes -> INSERTs into the upstream PG/MySQL/SQL-Server table.
    """
    if probe_kind in COMPUTE_PROBE_KINDS:
        return ProbeWriter(c, "probe_src")
    if probe_kind in KAFKA_PROBE_KINDS:
        assert topic is not None
        return KafkaProducerThread(c, topic, msgs_per_tick=msgs_per_tick)
    if probe_kind == "pg-source":
        return PgCdcWriter(c, CDC_TABLE, rows_per_tick=msgs_per_tick)
    if probe_kind == "mysql-source":
        return MySqlCdcWriter(c, rows_per_tick=msgs_per_tick)
    if probe_kind == "sql-server-source":
        return SqlServerCdcWriter(c)
    raise UIError(f"no writer for probe kind {probe_kind!r}")


def _create_probe(
    c: Composition, probe_kind: str, chain_depth: int = 1, topic: str | None = None
) -> str:
    """Create the cheap probe of the given kind; return the id we monitor.

    - index / materialized-view: a trivial compute object over probe_src, kept
      fresh by a ProbeWriter. With chain_depth > 1 the MV probe is a chain of
      pass-through views (probe_src -> pc1 -> ... -> pcN) and we monitor the
      tail, exposing propagation latency rather than a single starved object.
      (NB: a load-generator source is NOT a valid probe -- its frontier is a
      logical offset, not wallclock; see FINDINGS.md -- so CDC sources are used.)
    - kafka-source / kafka-sink: a real Kafka ingestion dataflow on the cluster.
      Kafka reclocks offsets onto the wallclock oracle, so now()-write_frontier
      is real ingestion lag. The producer (the writer for these probes) feeds
      `topic`. kafka-source monitors the ingested table; kafka-sink monitors a
      Kafka sink emitting that table back out. Brings source/sink ingestion
      under the same matrix as compute probes.
    """
    if probe_kind in ("kafka-source", "kafka-sink"):
        if chain_depth != 1:
            raise UIError("chain_depth > 1 requires the materialized-view probe")
        assert topic is not None
        c.sql("DROP SOURCE IF EXISTS ksrc CASCADE")
        c.sql(
            f"CREATE SOURCE ksrc IN CLUSTER test "
            f"FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')"
        )
        c.sql(
            f'CREATE TABLE ksrc_tbl FROM SOURCE ksrc (REFERENCE "{topic}") '
            f"FORMAT BYTES ENVELOPE NONE"
        )
        if probe_kind == "kafka-source":
            return _object_id(c, "ksrc_tbl")
        c.sql(
            f"CREATE SINK ksink IN CLUSTER test FROM ksrc_tbl "
            f"INTO KAFKA CONNECTION kafka_conn (TOPIC '{topic}-sink') "
            f"FORMAT JSON ENVELOPE DEBEZIUM"
        )
        return _object_id(c, "ksink")

    if probe_kind in CDC_PROBE_KINDS:
        if chain_depth != 1:
            raise UIError("chain_depth > 1 requires the materialized-view probe")
        c.sql("DROP SOURCE IF EXISTS cdcsrc CASCADE")
        if probe_kind == "pg-source":
            c.sql(
                "CREATE SOURCE cdcsrc IN CLUSTER test "
                "FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')"
            )
            c.sql(f"CREATE TABLE cdc_tbl FROM SOURCE cdcsrc (REFERENCE {CDC_TABLE})")
        elif probe_kind == "mysql-source":
            c.sql(
                "CREATE SOURCE cdcsrc IN CLUSTER test FROM MYSQL CONNECTION mysql_conn"
            )
            c.sql(
                f"CREATE TABLE cdc_tbl FROM SOURCE cdcsrc (REFERENCE public.{CDC_TABLE})"
            )
        else:  # sql-server-source
            c.sql(
                "CREATE SOURCE cdcsrc IN CLUSTER test FROM SQL SERVER CONNECTION ms_conn"
            )
            c.sql(
                f"CREATE TABLE cdc_tbl FROM SOURCE cdcsrc (REFERENCE dbo.{CDC_TABLE})"
            )
        return _object_id(c, "cdc_tbl")

    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    if probe_kind == "index":
        if chain_depth != 1:
            raise UIError("chain_depth > 1 requires the materialized-view probe")
        c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
        return _object_id(c, "probe_idx")
    elif probe_kind == "materialized-view":
        if chain_depth <= 1:
            c.sql(
                "CREATE MATERIALIZED VIEW probe_mv IN CLUSTER test AS "
                "SELECT count(*) AS n FROM probe_src"
            )
            return _object_id(c, "probe_mv")
        prev = "probe_src"
        for i in range(1, chain_depth + 1):
            c.sql(
                f"CREATE MATERIALIZED VIEW pc{i} IN CLUSTER test AS "
                f"SELECT val FROM {prev}"
            )
            prev = f"pc{i}"
        return _object_id(c, f"pc{chain_depth}")
    else:
        raise UIError(f"unknown probe kind {probe_kind!r}")


def _create_expensive(c: Composition, expensive_op: str) -> str:
    """Create the expensive object for the given op; return its object id."""
    if expensive_op not in EXPENSIVE_OPS:
        raise UIError(f"unknown expensive op {expensive_op!r}")
    create_sql, name = EXPENSIVE_OPS[expensive_op]
    c.sql(create_sql)
    return _object_id(c, name)


def run_scenario(
    c: Composition,
    size: str,
    expensive_op: str,
    probe_kind: str,
    action: str,
    *,
    fresh_threshold_ms: int,
    stall_threshold_ms: int,
    window_seconds: int,
    recovery_budget_ms: int,
    hydration_timeout_s: int,
    chain_depth: int = 1,
    big_txn_rows: int = 2_000_000,
    big_txn_mode: str = "insert",
    kafka_topic: str | None = None,
    msgs_per_tick: int = 500,
    backlog_rows: int = 5_000_000,
) -> Result:
    result = Result(
        size=size,
        expensive_op=expensive_op,
        probe_kind=probe_kind,
        action=action,
        threshold_ms=stall_threshold_ms,
    )

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql(f"CREATE CLUSTER test SIZE = '{size}'")

    # backlog: an ingestion-intrinsic shape (no co-located compute). Pre-load a
    # large backlog, THEN create the source, and measure how long it takes the
    # source to catch up to wallclock (time-to-fresh). Kafka-only.
    if action == "backlog":
        if not _probe_is_kafka(probe_kind):
            raise UIError("backlog action requires a kafka probe")
        assert kafka_topic is not None
        _ensure_topic(c, kafka_topic)
        _produce_backlog(c, kafka_topic, backlog_rows)
        t0 = time.time()
        probe_id = _create_probe(c, probe_kind, topic=kafka_topic)
        monitor = FreshnessMonitor(c, probe_id)
        monitor.start()
        try:
            result.recovery_ms = int(
                _wait_fresh(
                    c, probe_id, fresh_threshold_ms, timeout=hydration_timeout_s
                )
                * 1000
            )
            result.passed = True
        except UIError as e:
            result.detail = str(e)
        finally:
            monitor.stop()
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        # max_lag is meaningless during a Kafka snapshot (the frontier sits at
        # its initial value until the first commit), so leave it unset; the
        # catch-up time is the headline, recorded as recovery_ms. Not a stall
        # bug, so we don't assert on stall -- only that it caught up.
        if not result.passed and not result.detail:
            result.detail = "source never caught up"
        return result

    if _probe_is_kafka(probe_kind):
        assert kafka_topic is not None
        _ensure_topic(c, kafka_topic)
    if _probe_is_cdc(probe_kind):
        # Truncate upstream so the freshly-created source snapshots ~nothing.
        {
            "pg-source": _pg_reset_table,
            "mysql-source": _mysql_reset_table,
            "sql-server-source": _sql_server_reset_table,
        }[probe_kind](c)

    probe_id = _create_probe(c, probe_kind, chain_depth=chain_depth, topic=kafka_topic)

    # The writer keeps the probe's input moving; start it before warmup so a
    # kafka source has data to ingest while becoming fresh.
    writer = _make_probe_writer(c, probe_kind, kafka_topic, msgs_per_tick=msgs_per_tick)
    writer.start()

    # Warm up: don't count the probe's own (tiny) initial hydration. Only
    # compute objects appear in the compute-hydration table.
    if _probe_is_compute(probe_kind):
        _wait_hydrated(c, probe_id, timeout=60)
    _wait_fresh(c, probe_id, fresh_threshold_ms, timeout=120)

    monitor = FreshnessMonitor(c, probe_id)
    recovery_wallclock: float | None = None

    try:
        if action == "steady_state":
            monitor.start()
            time.sleep(window_seconds)

        elif action == "hydration":
            monitor.start()
            start = time.time()
            expensive_id = _create_expensive(c, expensive_op)
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            result.expensive_ms = int((time.time() - start) * 1000)
            # A short tail so we capture any post-hydration settling.
            time.sleep(2)

        elif action == "ingestion":
            expensive_id = _create_expensive(c, expensive_op)
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            monitor.start()
            big_writer = BigWriter(c, rows_per_batch=10_000)
            big_writer.start()
            try:
                time.sleep(window_seconds)
            finally:
                big_writer.stop()

        elif action == "retraction":
            expensive_id = _create_expensive(c, expensive_op)
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            monitor.start()
            churner = BigChurner(c)
            churner.start()
            try:
                time.sleep(window_seconds)
            finally:
                churner.stop()

        elif action == "big_txn":
            expensive_id = _create_expensive(c, expensive_op)
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            monitor.start()
            # One large single-statement DML -> one big incremental diff the
            # expensive operator must absorb in steady state. If maintenance
            # yields per-update this is cheap; if it processes the whole batch in
            # one activation (like bulk hydration) it starves the probe. The mode
            # varies the diff shape (insertion / retraction / mixed); delete and
            # update target the top ids (the current group maxima) to maximally
            # stress a hierarchical reduce.
            max_id = c.sql_query("SELECT coalesce(max(id), 0) FROM big")[0][0]
            if big_txn_mode == "insert":
                c.sql(
                    f"INSERT INTO big "
                    f"SELECT (g.s % {DISTINCT_KEYS})::bigint, g.s, repeat('x', 16) "
                    f"FROM generate_series({max_id + 1}, {max_id + big_txn_rows}) "
                    f"AS g(s)"
                )
            elif big_txn_mode == "delete":
                c.sql(f"DELETE FROM big WHERE id > {max_id - big_txn_rows}")
            elif big_txn_mode == "update":
                c.sql(
                    f"UPDATE big SET id = id - {2 * max_id + 1} "
                    f"WHERE id > {max_id - big_txn_rows}"
                )
            else:
                raise UIError(f"unknown big_txn mode {big_txn_mode!r}")
            time.sleep(window_seconds)

        elif action == "rehydration":
            expensive_id = _create_expensive(c, expensive_op)
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            monitor.start()
            # Drop and recreate the replica -> everything rehydrates from persist.
            c.sql("ALTER CLUSTER test SET (REPLICATION FACTOR 0)")
            time.sleep(1)
            c.sql("ALTER CLUSTER test SET (REPLICATION FACTOR 1)")
            t0 = time.time()
            # The probe must come back fresh quickly...
            result.recovery_ms = int(
                _wait_fresh(
                    c, probe_id, fresh_threshold_ms, timeout=hydration_timeout_s
                )
                * 1000
            )
            recovery_wallclock = time.time()
            # ...well before the expensive object finishes rehydrating, and it
            # must *stay* fresh afterwards (checked via the post-recovery stall).
            _wait_hydrated(c, expensive_id, timeout=hydration_timeout_s)
            result.expensive_ms = int((time.time() - t0) * 1000)
            time.sleep(2)

        else:
            raise UIError(f"unknown action {action!r}")
    finally:
        writer.stop()
        monitor.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    result.max_lag_ms = monitor.max_lag()
    # For rehydration the frontier is legitimately frozen while there is no
    # replica (RF=0); only the stall *after* first recovery is a fairness
    # violation, so measure the stall from the recovery point onwards.
    result.max_stall_ms = monitor.max_stall_ms(since=recovery_wallclock or 0.0)

    # ---- evaluate ----
    if action == "rehydration":
        if result.recovery_ms is None:
            result.detail = "probe never recovered"
        elif result.recovery_ms > recovery_budget_ms:
            result.detail = (
                f"recovery {result.recovery_ms}ms > budget {recovery_budget_ms}ms"
            )
        elif (
            result.expensive_ms is not None
            and result.recovery_ms >= result.expensive_ms
        ):
            result.detail = (
                f"recovery {result.recovery_ms}ms not faster than expensive "
                f"rehydration {result.expensive_ms}ms (probe blocked on it)"
            )
        elif (
            result.max_stall_ms is not None and result.max_stall_ms > stall_threshold_ms
        ):
            result.detail = (
                f"probe recovered but then stalled {result.max_stall_ms}ms "
                f"> threshold {stall_threshold_ms}ms"
            )
        else:
            result.passed = True
    else:
        if result.max_stall_ms is None:
            result.detail = "no freshness samples collected"
        elif result.max_stall_ms > stall_threshold_ms:
            result.detail = (
                f"frontier stalled {result.max_stall_ms}ms "
                f"> threshold {stall_threshold_ms}ms"
            )
        else:
            result.passed = True

    return result


def _print_summary(results: list[Result]) -> None:
    header = (
        f"{'size':<18} {'op':<9} {'probe':<11} {'action':<13} "
        f"{'stall':>9} {'max_lag':>9} {'recovery':>9} {'expensive':>10}  result"
    )
    print("\n" + header)
    print("-" * len(header))
    for r in results:
        status = "PASS" if r.passed else f"FAIL ({r.detail})"
        print(
            f"{r.size:<18} {r.expensive_op:<9} {r.probe_kind:<11} {r.action:<13} "
            f"{_ms(r.max_stall_ms):>9} {_ms(r.max_lag_ms):>9} {_ms(r.recovery_ms):>9} "
            f"{_ms(r.expensive_ms):>10}  {status}"
        )
    print("")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument(
        "--freshness-threshold-ms",
        type=int,
        default=DEFAULT_FRESHNESS_THRESHOLD_MS,
        help="lag below which the probe counts as 'fresh' (warmup/recovery)",
    )
    parser.add_argument(
        "--max-stall-ms",
        type=int,
        default=DEFAULT_MAX_STALL_MS,
        help="assertion threshold on the longest frontier-stall interval",
    )
    parser.add_argument(
        "--skew",
        type=float,
        default=0.0,
        help="fraction (0..1) of `big` rows assigned to a single hot key",
    )
    parser.add_argument(
        "--chain-depth",
        type=int,
        default=1,
        help="probe = chain of N pass-through MVs (materialized-view probe only)",
    )
    parser.add_argument(
        "--big-txn-rows",
        type=int,
        default=2_000_000,
        help="rows touched in one statement for the big_txn action",
    )
    parser.add_argument(
        "--big-txn-mode",
        default="insert",
        help="big_txn diff shape: insert | delete | update",
    )
    parser.add_argument("--window-seconds", type=int, default=DEFAULT_WINDOW_SECONDS)
    parser.add_argument(
        "--recovery-budget-ms", type=int, default=DEFAULT_RECOVERY_BUDGET_MS
    )
    parser.add_argument(
        "--hydration-timeout-s", type=int, default=DEFAULT_HYDRATION_TIMEOUT_S
    )
    parser.add_argument(
        "--cluster-sizes",
        default=";".join(CLUSTER_SIZES),
        help="semicolon-separated cluster sizes (size names contain commas)",
    )
    parser.add_argument(
        "--expensive-ops",
        default=",".join(DEFAULT_EXPENSIVE_OPS),
        help=f"comma-separated expensive ops ({', '.join(EXPENSIVE_OPS)})",
    )
    parser.add_argument(
        "--probe-kinds",
        default=DEFAULT_PROBE_KIND,
        help="comma-separated probe kinds (index, materialized-view)",
    )
    parser.add_argument(
        "--actions",
        default=",".join(ACTIONS),
        help="comma-separated actions ("
        "steady_state, hydration, ingestion, retraction, rehydration)",
    )
    parser.add_argument(
        "--msgs-per-tick",
        type=int,
        default=500,
        help="Kafka messages produced every 100ms (kafka probes); raise to "
        "drive a source past its own ingest throughput",
    )
    parser.add_argument(
        "--backlog-rows",
        type=int,
        default=5_000_000,
        help="messages pre-loaded before the source for the `backlog` action",
    )
    args = parser.parse_args()

    sizes = [s.strip() for s in args.cluster_sizes.split(";") if s.strip()]
    ops = [o.strip() for o in args.expensive_ops.split(",") if o.strip()]
    probe_kinds = [k.strip() for k in args.probe_kinds.split(",") if k.strip()]
    actions = [a.strip() for a in args.actions.split(",") if a.strip()]

    needs_kafka = any(_probe_is_kafka(k) for k in probe_kinds)
    needs_pg = "pg-source" in probe_kinds
    needs_mysql = "mysql-source" in probe_kinds
    needs_sql_server = "sql-server-source" in probe_kinds
    extra_services = (
        (["kafka"] if needs_kafka else [])
        + (["postgres"] if needs_pg else [])
        + (["mysql"] if needs_mysql else [])
        + (["sql-server"] if needs_sql_server else [])
    )
    c.up("materialized", *extra_services)
    # Connections/publications are top-level; they survive the per-scenario
    # `DROP CLUSTER test CASCADE`, so set them up once here.
    if needs_kafka:
        c.sql(
            "CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA "
            "(BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT)"
        )
    if needs_pg:
        _ensure_pg_cdc(c)
        c.sql("CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'")
        c.sql(
            "CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES "
            "(HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgpass)"
        )
    if needs_mysql:
        _ensure_mysql_cdc(c)
        c.sql(f"CREATE SECRET IF NOT EXISTS mysqlpass AS '{MYSQL_ROOT_PASSWORD}'")
        c.sql(
            "CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL "
            "(HOST mysql, USER root, PASSWORD SECRET mysqlpass)"
        )
    if needs_sql_server:
        _ensure_sql_server_cdc(c)
        c.sql(f"CREATE SECRET IF NOT EXISTS mspass AS '{SQL_SERVER_SA_PASSWORD}'")
        c.sql(
            "CREATE CONNECTION IF NOT EXISTS ms_conn TO SQL SERVER "
            "(HOST 'sql-server', PORT 1433, DATABASE cdc_db, USER sa, "
            "PASSWORD SECRET mspass)"
        )

    # `big` is cluster-independent and expensive to build, so we reuse it across
    # read-only scenarios rather than rebuilding every cell. But the mutating
    # actions (see MUTATING_ACTIONS) change it in place, so we rebuild lazily:
    # whenever the previous user dirtied `big`, the next scenario that reads it
    # gets a fresh copy. This keeps the matrix hermetic (order-independent)
    # without paying for a rebuild between consecutive read-only scenarios.
    # Starts dirty so the first non-steady_state scenario triggers a build.
    big_dirty = True

    results: list[Result] = []
    scenario_idx = 0
    for size in sizes:
        for op in ops:
            for probe_kind in probe_kinds:
                for action in actions:
                    scenario_idx += 1
                    if action != "steady_state" and big_dirty:
                        _create_big(c, args.rows, skew=args.skew)
                        big_dirty = False
                    # A mutating action dirties `big`; force a rebuild before the
                    # next scenario that reads it.
                    if action in MUTATING_ACTIONS:
                        big_dirty = True
                    # Per-scenario topic: the cluster (and source) is recreated
                    # each cell, and a fresh source on a shared topic would re-
                    # snapshot a growing backlog, so isolate per scenario.
                    kafka_topic = (
                        f"freshness-ingest-{scenario_idx}"
                        if _probe_is_kafka(probe_kind)
                        else None
                    )
                    print(
                        f"\n=== freshness: size={size} op={op} "
                        f"probe={probe_kind} action={action} ==="
                    )
                    try:
                        results.append(
                            run_scenario(
                                c,
                                size,
                                op,
                                probe_kind,
                                action,
                                fresh_threshold_ms=args.freshness_threshold_ms,
                                stall_threshold_ms=args.max_stall_ms,
                                window_seconds=args.window_seconds,
                                recovery_budget_ms=args.recovery_budget_ms,
                                hydration_timeout_s=args.hydration_timeout_s,
                                chain_depth=args.chain_depth,
                                big_txn_rows=args.big_txn_rows,
                                big_txn_mode=args.big_txn_mode,
                                kafka_topic=kafka_topic,
                                msgs_per_tick=args.msgs_per_tick,
                                backlog_rows=args.backlog_rows,
                            )
                        )
                    except Exception as e:
                        results.append(
                            Result(
                                size=size,
                                expensive_op=op,
                                probe_kind=probe_kind,
                                action=action,
                                threshold_ms=args.max_stall_ms,
                                detail=f"error: {e}",
                            )
                        )

    _print_summary(results)

    failures = [r for r in results if not r.passed]
    if failures:
        raise UIError(
            f"{len(failures)}/{len(results)} freshness scenarios failed: "
            + "; ".join(
                f"{r.size}/{r.expensive_op}/{r.probe_kind}/{r.action}: {r.detail}"
                for r in failures
            )
        )


def _toxiproxy(c: Composition, method: str, path: str, body: dict | None = None):
    """Drive the toxiproxy admin API over its mapped host port."""
    url = f"http://localhost:{c.default_port('toxiproxy')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        # DELETE of a missing proxy/toxic is fine (idempotent setup/teardown).
        if method == "DELETE" and e.code in (404, 409):
            return None
        raise


def workflow_storage(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Storage-bound freshness: inject persist *blob* latency (not compute) and
    watch the probes lag.

    persist routes blob through toxiproxy -> minio; consensus stays on the
    metadata store, so this isolates blob-store slowness. An index probe
    (compute-only output) and a materialized-view probe (also writes its output
    to persist) are monitored together as a discriminator: blob latency is
    global (table commits are blob-gated too), so both lag, but the MV -- which
    has an extra persist write on its output -- should lag at least as much.
    A dose-response over latencies shows the harness detects storage-bound
    staleness, a fundamentally different shape from compute starvation.
    """
    parser.add_argument(
        "--latencies-ms",
        default="0,150,500",
        help="comma-separated blob round-trip latencies to sweep",
    )
    parser.add_argument("--window-seconds", type=int, default=15)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument(
        "--heavy-rows-per-batch",
        type=int,
        default=0,
        help="if >0, drive heavy write volume and make the MV probe a "
        "write-heavy pass-through (tests storage *throughput*, not latency)",
    )
    args = parser.parse_args()
    latencies = [int(x) for x in args.latencies_ms.split(",") if x.strip()]
    heavy = args.heavy_rows_per_batch

    with c.override(
        Materialized(external_blob_store="toxiproxy", sanity_restart=False),
    ):
        c.up("minio", "toxiproxy")
        # The proxy must exist before materialized starts, since persist points
        # its blob URL at toxiproxy:9000.
        _toxiproxy(c, "DELETE", "/proxies/persist")
        _toxiproxy(
            c,
            "POST",
            "/proxies",
            {
                "name": "persist",
                "listen": "0.0.0.0:9000",
                "upstream": "minio:9000",
                "enabled": True,
            },
        )
        c.up("materialized")

        c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
        c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
        c.sql("CREATE TABLE probe_src (val bigint)")
        c.sql("INSERT INTO probe_src VALUES (0)")
        c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
        # A write-heavy MV (pass-through, output grows with the input) writes a
        # lot to persist; the count(*) MV barely writes. The index never writes
        # output to persist, so it isolates the input-commit lag from the MV's
        # own output-write lag.
        mv_sql = (
            "SELECT val FROM probe_src"
            if heavy > 0
            else "SELECT count(*) AS n FROM probe_src"
        )
        c.sql(f"CREATE MATERIALIZED VIEW probe_mv IN CLUSTER test AS {mv_sql}")
        idx_id = _object_id(c, "probe_idx")
        mv_id = _object_id(c, "probe_mv")
        _wait_hydrated(c, idx_id, timeout=120)
        _wait_hydrated(c, mv_id, timeout=120)

        rows: list[tuple[int, int | None, int | None, int | None, int | None]] = []
        writer = ProbeWriter(c, "probe_src", rows_per_batch=max(1, heavy))
        writer.start()
        try:
            for lat in latencies:
                _toxiproxy(c, "DELETE", "/proxies/persist/toxics/lat_up")
                _toxiproxy(c, "DELETE", "/proxies/persist/toxics/lat_down")
                if lat > 0:
                    for name, stream in (
                        ("lat_up", "upstream"),
                        ("lat_down", "downstream"),
                    ):
                        _toxiproxy(
                            c,
                            "POST",
                            "/proxies/persist/toxics",
                            {
                                "name": name,
                                "type": "latency",
                                "stream": stream,
                                "attributes": {"latency": lat},
                            },
                        )
                print(f"\n=== storage: blob latency = {lat}ms ===")
                time.sleep(3)  # let the toxic take effect and any backlog settle
                mon_idx = FreshnessMonitor(c, idx_id)
                mon_mv = FreshnessMonitor(c, mv_id)
                mon_idx.start()
                mon_mv.start()
                time.sleep(args.window_seconds)
                mon_idx.stop()
                mon_mv.stop()
                rows.append(
                    (
                        lat,
                        mon_idx.max_stall_ms(),
                        mon_idx.max_lag(),
                        mon_mv.max_stall_ms(),
                        mon_mv.max_lag(),
                    )
                )
        finally:
            writer.stop()
            _toxiproxy(c, "DELETE", "/proxies/persist/toxics/lat_up")
            _toxiproxy(c, "DELETE", "/proxies/persist/toxics/lat_down")
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    header = (
        f"{'blob_latency':>12} {'idx_stall':>10} {'idx_lag':>9} "
        f"{'mv_stall':>10} {'mv_lag':>9}"
    )
    print("\n" + header)
    print("-" * len(header))

    for lat, idx_stall, idx_lag, mv_stall, mv_lag in rows:
        print(
            f"{str(lat) + 'ms':>12} {_ms(idx_stall):>10} {_ms(idx_lag):>9} "
            f"{_ms(mv_stall):>10} {_ms(mv_lag):>9}"
        )
    print("")


def workflow_ingestion(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Source/sink ingestion freshness under co-located compute contention.

    A Kafka source reclocks Kafka offsets onto the wallclock timestamp oracle,
    so `now() - write_frontier` of the ingested collection is a *real* ingestion
    lag (unlike a load-generator source, whose frontier is a logical offset --
    see FINDINGS.md). A host producer streams to a topic; we monitor:
      - the ingested table's frontier (source ingestion freshness), and
      - a Kafka sink's frontier (sink emission freshness),
    first in steady state, then while a co-located expensive `max` MV hydrates
    on the same cluster. The question is whether bulk compute work starves the
    source/sink ingestion dataflows the same way it starves indexes/MVs.
    """
    parser.add_argument("--expensive-op", default="max")
    parser.add_argument("--rows", type=int, default=3_000_000)
    parser.add_argument("--window-seconds", type=int, default=15)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument(
        "--msgs-per-tick",
        type=int,
        default=500,
        help="Kafka messages produced every 100ms",
    )
    args = parser.parse_args()
    topic = "freshness-ingest"

    c.up("kafka", "materialized")
    _ensure_topic(c, topic)
    producer = KafkaProducerThread(c, topic, msgs_per_tick=args.msgs_per_tick)
    producer.start()
    try:
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
        c.sql(
            "CREATE CONNECTION kafka_conn TO KAFKA "
            "(BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT)"
        )
        c.sql(
            f"CREATE SOURCE ksrc IN CLUSTER test "
            f"FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')"
        )
        # The ingested collection (the table-from-source) holds the data and
        # carries the reclocked, wallclock-domain write frontier we monitor.
        c.sql(
            f'CREATE TABLE ksrc_tbl FROM SOURCE ksrc (REFERENCE "{topic}") '
            f"FORMAT BYTES ENVELOPE NONE"
        )
        # A sink emitting the (steadily-updated) ingested data back to Kafka.
        c.sql(
            "CREATE SINK ksink IN CLUSTER test FROM ksrc_tbl "
            "INTO KAFKA CONNECTION kafka_conn (TOPIC 'freshness-sink-out') "
            "FORMAT JSON ENVELOPE DEBEZIUM"
        )
        src_id = _object_id(c, "ksrc_tbl")
        sink_id = _object_id(c, "ksink")

        # A co-located compute reference (index over a tiny written table). It
        # shares the cluster's workers, so under the expensive op it is expected
        # to STALL -- the contrast that shows source/sink ingestion rides a
        # different (storage/persist-async) path than timely-worker compute.
        c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
        c.sql("CREATE TABLE probe_src (val bigint)")
        c.sql("INSERT INTO probe_src VALUES (0)")
        c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
        idx_id = _object_id(c, "probe_idx")
        idx_writer = ProbeWriter(c, "probe_src")
        idx_writer.start()

        # Warm up: wait until all are tracking wallclock (fresh).
        _wait_fresh(c, src_id, args.max_stall_ms, timeout=120)
        _wait_fresh(c, sink_id, args.max_stall_ms, timeout=120)
        _wait_fresh(c, idx_id, args.max_stall_ms, timeout=120)

        results = []
        for phase_name in ("steady_state", "hydration"):
            mon_src = FreshnessMonitor(c, src_id)
            mon_sink = FreshnessMonitor(c, sink_id)
            mon_idx = FreshnessMonitor(c, idx_id)
            mon_src.start()
            mon_sink.start()
            mon_idx.start()
            expensive_ms: int | None = None
            if phase_name == "hydration":
                # Build a large `big` table and an expensive co-located MV.
                _create_big(c, args.rows)
                start = time.time()
                expensive_id = _create_expensive(c, args.expensive_op)
                _wait_hydrated(c, expensive_id, timeout=900)
                expensive_ms = int((time.time() - start) * 1000)
                time.sleep(2)
            else:
                time.sleep(args.window_seconds)
            mon_src.stop()
            mon_sink.stop()
            mon_idx.stop()
            results.append(
                (
                    phase_name,
                    mon_src.max_stall_ms(),
                    mon_sink.max_stall_ms(),
                    mon_idx.max_stall_ms(),
                    expensive_ms,
                )
            )
    finally:
        idx_writer.stop()
        producer.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    header = (
        f"{'phase':<13} {'src_stall':>10} {'sink_stall':>10} "
        f"{'idx_stall (ref)':>16} {'expensive':>10}  result"
    )
    print("\n" + header)
    print("-" * len(header))

    failures = []
    for phase_name, ss, ks, ixs, exp in results:
        # Assertion is on source/sink ingestion only; the index is a reference
        # that is *expected* to stall (and demonstrates the contrast).
        worst = max(x for x in (ss, ks) if x is not None)
        ok = worst <= args.max_stall_ms
        if not ok:
            failures.append(phase_name)
        print(
            f"{phase_name:<13} {_ms(ss):>10} {_ms(ks):>10} {_ms(ixs):>16} "
            f"{_ms(exp):>10}  {'PASS' if ok else 'FAIL'}"
        )
    print("")
    if failures:
        raise UIError(f"ingestion freshness stalled in phases: {failures}")


# Port toxiproxy listens on (inside the compose net) to proxy MZ -> Postgres.
PG_PROXY_PORT = 5432


_PG_TOXICS = ("lat_up", "lat_down", "bw_down")


def _clear_pg_toxics(c: Composition) -> None:
    for name in _PG_TOXICS:
        _toxiproxy(c, "DELETE", f"/proxies/pgproxy/toxics/{name}")


def workflow_upstream(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Upstream-link throttling: degrade MZ's path to the CDC source.

    MZ's Postgres connection is routed through toxiproxy (MZ -> toxiproxy ->
    postgres); the host writer inserts *directly* into Postgres, so only MZ's
    ingestion link is degraded. Ingestion-intrinsic (no co-located compute), the
    one freshness vector that depends on the upstream link, not the cluster.

    --mode latency   : sweep added round-trip latency (the link is slow).
    --mode bandwidth : sweep a downstream byte-rate cap (the link is narrow);
                       if throughput < arrival rate the source falls behind
                       unboundedly, unlike latency.
    --mode flap      : repeatedly sever + restore the link (disconnect), and
                       measure whether the source recovers freshness afterward.
    """
    parser.add_argument("--mode", default="latency", help="latency | bandwidth | flap")
    parser.add_argument(
        "--latencies-ms",
        default="0,250,1000",
        help="latency mode: one-way latencies added to the MZ<->PG link",
    )
    parser.add_argument(
        "--rates-kbps",
        default="0,500,100,20",
        help="bandwidth mode: downstream rate caps in KB/s (0 = unlimited)",
    )
    parser.add_argument("--flap-cycles", type=int, default=3)
    parser.add_argument("--flap-down-s", type=int, default=5)
    parser.add_argument("--flap-up-s", type=int, default=8)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--msgs-per-tick", type=int, default=200)
    args = parser.parse_args()

    c.up("postgres", "toxiproxy", "materialized")
    _ensure_pg_cdc(c)
    # Proxy MZ -> Postgres through toxiproxy so we can degrade that link only.
    _toxiproxy(c, "DELETE", "/proxies/pgproxy")
    _toxiproxy(
        c,
        "POST",
        "/proxies",
        {
            "name": "pgproxy",
            "listen": f"0.0.0.0:{PG_PROXY_PORT}",
            "upstream": "postgres:5432",
            "enabled": True,
        },
    )
    c.sql("CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'")
    c.sql(
        f"CREATE CONNECTION IF NOT EXISTS pg_via_proxy TO POSTGRES "
        f"(HOST toxiproxy, PORT {PG_PROXY_PORT}, DATABASE postgres, "
        f"USER postgres, PASSWORD SECRET pgpass)"
    )

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    _pg_reset_table(c)
    c.sql(
        "CREATE SOURCE cdcsrc IN CLUSTER test "
        "FROM POSTGRES CONNECTION pg_via_proxy (PUBLICATION 'mz_source')"
    )
    c.sql(f"CREATE TABLE cdc_tbl FROM SOURCE cdcsrc (REFERENCE {CDC_TABLE})")
    src_id = _object_id(c, "cdc_tbl")

    writer = PgCdcWriter(c, CDC_TABLE, rows_per_tick=args.msgs_per_tick)
    writer.start()
    # Establish freshness on a clean link before degrading it.
    _wait_fresh(c, src_id, args.max_stall_ms, timeout=120)

    try:
        if args.mode == "flap":
            mon = FreshnessMonitor(c, src_id)
            mon.start()
            for i in range(args.flap_cycles):
                print(f"\n=== upstream flap cycle {i + 1}: sever link ===")
                _toxiproxy(c, "POST", "/proxies/pgproxy", {"enabled": False})
                time.sleep(args.flap_down_s)
                print("    restore link")
                _toxiproxy(c, "POST", "/proxies/pgproxy", {"enabled": True})
                time.sleep(args.flap_up_s)
            mon.stop()
            # After the last restore, how long until fresh again?
            recovery_ms = int(
                _wait_fresh(c, src_id, args.max_stall_ms, timeout=120) * 1000
            )
            print(f"\n{'flap':>14} {'max_stall':>10} {'max_lag':>9} {'recovery':>9}")
            print("-" * 45)
            print(
                f"{f'{args.flap_cycles}x':>14} {_ms(mon.max_stall_ms()):>10} "
                f"{_ms(mon.max_lag()):>9} {_ms(recovery_ms):>9}"
            )
            print(
                "\nrecovery < down-interval => source resumed from its "
                "replication slot and caught up (no permanent staleness)."
            )
        else:
            rows: list[tuple[int, int | None, int | None]] = []
            steps = (
                [int(x) for x in args.latencies_ms.split(",") if x.strip()]
                if args.mode == "latency"
                else [int(x) for x in args.rates_kbps.split(",") if x.strip()]
            )
            for step in steps:
                _clear_pg_toxics(c)
                if args.mode == "latency" and step > 0:
                    for name, stream in (
                        ("lat_up", "upstream"),
                        ("lat_down", "downstream"),
                    ):
                        _toxiproxy(
                            c,
                            "POST",
                            "/proxies/pgproxy/toxics",
                            {
                                "name": name,
                                "type": "latency",
                                "stream": stream,
                                "attributes": {"latency": step},
                            },
                        )
                    label = f"latency {step}ms/way"
                elif args.mode == "bandwidth" and step > 0:
                    _toxiproxy(
                        c,
                        "POST",
                        "/proxies/pgproxy/toxics",
                        {
                            "name": "bw_down",
                            "type": "bandwidth",
                            "stream": "downstream",
                            "attributes": {"rate": step},
                        },
                    )
                    label = f"rate {step}KB/s"
                else:
                    label = "unlimited" if args.mode == "bandwidth" else "latency 0ms"
                print(f"\n=== upstream {args.mode}: {label} ===")
                time.sleep(3)  # let the toxic take effect
                mon = FreshnessMonitor(c, src_id)
                mon.start()
                time.sleep(args.window_seconds)
                mon.stop()
                rows.append((step, mon.max_stall_ms(), mon.max_lag()))

            col = "link_latency" if args.mode == "latency" else "rate_kbps"
            unit = "ms" if args.mode == "latency" else "KB/s"
            header = f"{col:>12} {'src_stall':>10} {'src_lag':>9}"
            print("\n" + header)
            print("-" * len(header))
            for step, stall, lag in rows:
                print(f"{f'{step}{unit}':>12} {_ms(stall):>10} {_ms(lag):>9}")
            print("")
    finally:
        writer.stop()
        _clear_pg_toxics(c)
        _toxiproxy(c, "POST", "/proxies/pgproxy", {"enabled": True})
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")


def workflow_xcluster(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Cross-cluster 'noisy neighbor': does compute starvation on one cluster
    leak into a *different* cluster's freshness through a shared input?

    Topology: an upstream MV `shared` on cluster `up` (fed by probe_src). A cheap
    consumer MV `consumer` on a SEPARATE cluster `down` reads `shared`. We run the
    expensive `max` hydration on `down` (same cluster as the consumer -> expected
    to starve it, the within-cluster control) OR on `up` (different cluster from
    the consumer -> the cross-cluster question). The consumer's freshness depends
    on `shared`'s persist output; if starving `up` stalls `shared`'s write
    frontier, the consumer on `down` goes stale too -- cross-cluster contagion.
    """
    parser.add_argument(
        "--starve-on",
        default="up,down",
        help="comma-separated clusters to place the expensive op on (up|down)",
    )
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--window-seconds", type=int, default=12)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()
    starve_on = [s.strip() for s in args.starve_on.split(",") if s.strip()]

    c.up("materialized")
    _create_big(c, args.rows)

    results: list[tuple[str, int | None, int | None, int | None]] = []
    for where in starve_on:
        print(f"\n=== xcluster: expensive op on '{where}' ===")
        for cl in ("up", "down"):
            c.sql(f"DROP CLUSTER IF EXISTS {cl} CASCADE")
            c.sql(f"CREATE CLUSTER {cl} SIZE = 'scale=1,workers=1'")
        c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
        c.sql("CREATE TABLE probe_src (val bigint)")
        c.sql("INSERT INTO probe_src VALUES (0)")
        # Upstream MV on `up`, consumer MV on `down` reading the upstream.
        c.sql(
            "CREATE MATERIALIZED VIEW shared IN CLUSTER up AS "
            "SELECT val, val + 1 AS v2 FROM probe_src"
        )
        c.sql(
            "CREATE MATERIALIZED VIEW consumer IN CLUSTER down AS "
            "SELECT count(*) AS n FROM shared"
        )
        shared_id = _object_id(c, "shared")
        consumer_id = _object_id(c, "consumer")
        _wait_hydrated(c, shared_id, timeout=120)
        _wait_hydrated(c, consumer_id, timeout=120)
        _wait_fresh(c, consumer_id, args.max_stall_ms, timeout=60)

        mon_shared = FreshnessMonitor(c, shared_id)
        mon_consumer = FreshnessMonitor(c, consumer_id)
        writer = ProbeWriter(c, "probe_src")
        mon_shared.start()
        mon_consumer.start()
        writer.start()
        try:
            c.sql(
                f"CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER {where} AS "
                f"SELECT key, max(id) AS m FROM big GROUP BY key"
            )
            _wait_hydrated(
                c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s
            )
            time.sleep(2)
        finally:
            writer.stop()
            mon_shared.stop()
            mon_consumer.stop()
            for cl in ("up", "down"):
                c.sql(f"DROP CLUSTER IF EXISTS {cl} CASCADE")
        results.append(
            (
                where,
                mon_shared.max_stall_ms(),
                mon_consumer.max_stall_ms(),
                mon_consumer.max_lag(),
            )
        )

    header = (
        f"{'starve_on':<10} {'shared_stall(up)':>17} "
        f"{'consumer_stall(down)':>21} {'consumer_lag':>13}  note"
    )
    print("\n" + header)
    print("-" * len(header))

    for where, ss, cs, cl in results:
        note = "within-cluster control" if where == "down" else "CROSS-CLUSTER question"
        print(f"{where:<10} {_ms(ss):>17} {_ms(cs):>21} {_ms(cl):>13}  {note}")
    print(
        "\nIf 'up' starves the down-cluster consumer, compute starvation leaks "
        "across clusters via the shared input's stalled write frontier."
    )


def _timed_select(c: Composition, isolation: str, query: str) -> float:
    """Run *query* under *isolation* on a fresh connection; return seconds."""
    conn = _connect(c)
    try:
        cur = conn.cursor()
        cur.execute(f"SET transaction_isolation = '{isolation}'".encode())
        start = time.time()
        cur.execute(query.encode())
        cur.fetchall()
        return time.time() - start
    finally:
        conn.close()


def workflow_rtr(c: Composition, parser: WorkflowArgumentParser) -> None:
    """User-facing amplification: the freshness stall as *hung SELECTs*.

    The freshness lag we measure is exactly how stale a `serializable` read is.
    A `strict serializable` read instead *waits* for the object's write frontier
    to reach real time -- so when a co-located hierarchical reduce starves the
    cluster, a strict-serializable SELECT against the cheap probe blocks for ~the
    stall, while a serializable SELECT returns (stale) immediately. We fire both,
    repeatedly, while the expensive `max` hydrates, and report the worst latency.
    """
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    _create_big(c, args.rows)

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql(
        "CREATE MATERIALIZED VIEW probe_mv IN CLUSTER test AS "
        "SELECT count(*) AS n FROM probe_src"
    )
    probe_id = _object_id(c, "probe_mv")
    _wait_hydrated(c, probe_id, timeout=60)

    writer = ProbeWriter(c, "probe_src")
    writer.start()
    worst = {"serializable": 0.0, "strict serializable": 0.0}
    try:
        # Create the expensive op; it hydrates asynchronously. While it does,
        # fire reads under both isolations against the cheap probe.
        c.sql(
            "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
            "SELECT key, max(id) AS m FROM big GROUP BY key"
        )
        expensive_id = _object_id(c, "expensive_mv")
        start = time.time()
        while True:
            for iso in ("serializable", "strict serializable"):
                dt = _timed_select(c, iso, "SELECT n FROM probe_mv")
                worst[iso] = max(worst[iso], dt)
            rows = c.sql_query(
                "SELECT bool_and(hydrated) FROM "
                "mz_internal.mz_compute_hydration_statuses "
                f"WHERE object_id = '{expensive_id}'"
            )
            if rows and rows[0][0] is True:
                break
            if time.time() - start > args.hydration_timeout_s:
                raise UIError("expensive op did not hydrate")
        expensive_ms = int((time.time() - start) * 1000)
    finally:
        writer.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    print(f"\nexpensive `max` hydration took {expensive_ms}ms")
    print(f"{'isolation':<22} {'worst SELECT latency':>22}")
    print("-" * 46)
    for iso in ("serializable", "strict serializable"):
        print(f"{iso:<22} {str(int(worst[iso] * 1000)) + 'ms':>22}")
    print(
        "\nA strict-serializable read blocks ~the stall while a serializable read "
        "returns stale immediately: the compute stall surfaces to users as hung "
        "queries, not just lag."
    )


def _wait_all_hydrated(c: Composition, names: list[str], timeout: float) -> float:
    """Wait until every named compute object on cluster `test` is hydrated."""
    ids = [_object_id(c, n) for n in names]
    in_list = ",".join(f"'{i}'" for i in ids)
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        rows = c.sql_query(
            "SELECT count(*) FROM mz_internal.mz_compute_hydration_statuses "
            f"WHERE object_id IN ({in_list}) AND hydrated"
        )
        if rows and rows[0][0] == len(ids):
            return time.time() - start
        time.sleep(0.5)
    raise UIError(f"only some of {len(ids)} objects hydrated within {timeout}s")


def workflow_manyobjects(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Scheduling fairness at fleet scale: do MANY individually-cheap dataflows
    collectively starve a co-located cheap probe?

    Customers run hundreds of MVs on one cluster. Each `count`-reduce over `big`
    keeps the probe fresh on its own (see the operator taxonomy); the question is
    whether N of them hydrating at once collectively monopolize the worker and
    starve a trivial index probe -- a fairness failure distinct from the single
    non-yielding hierarchical reduce.
    """
    parser.add_argument("--fan-count", type=int, default=50)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    _create_big(c, args.rows)

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
    probe_id = _object_id(c, "probe_idx")
    _wait_hydrated(c, probe_id, timeout=60)
    _wait_fresh(c, probe_id, args.max_stall_ms, timeout=60)

    monitor = FreshnessMonitor(c, probe_id)
    writer = ProbeWriter(c, "probe_src")
    monitor.start()
    writer.start()
    try:
        # Fire N individually-cheap (accumulable) reduce MVs all at once. Each is
        # offset by `lvl` so they are distinct dataflows, not deduplicated.
        names = [f"fan_{i}" for i in range(args.fan_count)]
        for i, n in enumerate(names):
            c.sql(
                f"CREATE MATERIALIZED VIEW {n} IN CLUSTER test AS "
                f"SELECT key, count(*) + {i} AS c FROM big GROUP BY key"
            )
        start = time.time()
        _wait_all_hydrated(c, names, timeout=args.hydration_timeout_s)
        fleet_ms = int((time.time() - start) * 1000)
        time.sleep(2)
    finally:
        writer.stop()
        monitor.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    stall = monitor.max_stall_ms()
    ok = stall is not None and stall <= args.max_stall_ms
    print(
        f"\n{args.fan_count} accumulable MVs hydrated together in {fleet_ms}ms; "
        f"probe max stall {stall}ms -> {'PASS' if ok else 'FAIL'}"
    )
    print(
        "PASS => the worker round-robins a fleet of cheap dataflows fairly; the "
        "probe stays fresh. FAIL => collective fleet starvation (distinct from "
        "the single hierarchical-reduce bug)."
    )


def workflow_upsert(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Upsert-source state: does maintaining a large/hot upsert key-state slow
    the source's own ingestion freshness?

    An UPSERT Kafka source keeps per-key state (last value). We sweep the key
    space at a fixed high produce rate: a small keyspace = many updates to few
    keys (hot), a huge keyspace = unbounded state growth. We watch whether the
    source's write frontier keeps up with wallclock as the state shape varies.
    """
    parser.add_argument(
        "--keyspaces",
        default="1000,1000000,unbounded",
        help="comma-separated upsert key spaces (int, or 'unbounded')",
    )
    parser.add_argument("--msgs-per-tick", type=int, default=2000)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    args = parser.parse_args()
    keyspaces = [s.strip() for s in args.keyspaces.split(",") if s.strip()]

    c.up("kafka", "materialized")
    c.sql(
        "CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA "
        "(BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT)"
    )

    rows: list[tuple[str, int | None, int | None]] = []
    for ks in keyspaces:
        topic = f"freshness-upsert-{ks}"
        _ensure_topic(c, topic)
        keyspace = None if ks == "unbounded" else int(ks)
        # Pre-seed a little so the upsert snapshot is non-empty, then stream.
        producer = KafkaProducerThread(
            c, topic, msgs_per_tick=args.msgs_per_tick, keyspace=keyspace
        )
        producer.start()
        try:
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")
            c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
            c.sql(
                f"CREATE SOURCE usrc IN CLUSTER test "
                f"FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')"
            )
            c.sql(
                f'CREATE TABLE u_tbl FROM SOURCE usrc (REFERENCE "{topic}") '
                f"KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE UPSERT"
            )
            src_id = _object_id(c, "u_tbl")
            _wait_fresh(c, src_id, args.max_stall_ms, timeout=120)
            mon = FreshnessMonitor(c, src_id)
            mon.start()
            print(f"\n=== upsert: keyspace={ks} @ {args.msgs_per_tick * 10}/s ===")
            time.sleep(args.window_seconds)
            mon.stop()
            rows.append((ks, mon.max_stall_ms(), mon.max_lag()))
        finally:
            producer.stop()
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    header = f"{'keyspace':>12} {'src_stall':>10} {'src_lag':>9}"
    print("\n" + header)
    print("-" * len(header))

    for ks, stall, lag in rows:
        print(f"{ks:>12} {_ms(stall):>10} {_ms(lag):>9}")
    print("")


class _RetainChurner:
    """Sliding-window churn: insert a fresh batch and delete the oldest each
    tick, so the live set stays bounded but history accumulates (the cost a
    RETAIN HISTORY window pays)."""

    def __init__(self, c: Composition, table: str, batch: int = 5000, window: int = 10):
        self.c = c
        self.table = table
        self.batch = batch
        self.window = window
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            gen = 0
            while not self._stop.is_set():
                cur.execute(
                    f"INSERT INTO {self.table} SELECT {gen}, g.s "
                    f"FROM generate_series(1, {self.batch}) AS g(s)".encode()
                )
                if gen >= self.window:
                    cur.execute(
                        f"DELETE FROM {self.table} WHERE gen = {gen - self.window}".encode()
                    )
                gen += 1
                self._stop.wait(WRITER_INTERVAL_S)
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def workflow_retain(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Read-side: does a large RETAIN HISTORY window degrade write-frontier
    freshness under heavy churn?

    RETAIN HISTORY holds the read frontier (`since`) back so historical / AS OF
    reads work, which keeps more versions in the arrangement. The question is
    whether that retained history slows the object's *write*-frontier freshness
    (the live edge). We churn a passthrough MV's input hard and sweep the
    retention window, monitoring both frontiers.
    """
    parser.add_argument(
        "--retentions", default="default,60s,600s", help="comma-separated windows"
    )
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    args = parser.parse_args()
    retentions = [s.strip() for s in args.retentions.split(",") if s.strip()]

    c.up("materialized")
    rows: list[tuple[str, int | None, int | None, int | None]] = []
    for ret in retentions:
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
        c.sql("DROP TABLE IF EXISTS rsrc CASCADE")
        c.sql("CREATE TABLE rsrc (gen bigint, val bigint)")
        with_clause = "" if ret == "default" else f" WITH (RETAIN HISTORY FOR '{ret}')"
        c.sql(
            f"CREATE MATERIALIZED VIEW rv IN CLUSTER test{with_clause} AS "
            f"SELECT gen, val FROM rsrc"
        )
        rv_id = _object_id(c, "rv")
        _wait_hydrated(c, rv_id, timeout=60)

        churner = _RetainChurner(c, "rsrc")
        churner.start()
        try:
            _wait_fresh(c, rv_id, args.max_stall_ms, timeout=60)
            mon = FreshnessMonitor(c, rv_id)
            mon.start()
            print(f"\n=== retain: RETAIN HISTORY {ret} under churn ===")
            time.sleep(args.window_seconds)
            mon.stop()
            # read_frontier (since) lag: how far back the readable edge sits.
            rf = c.sql_query(
                "SELECT (extract(epoch FROM now()) * 1000)::bigint "
                "- read_frontier::text::bigint FROM mz_internal.mz_frontiers "
                f"WHERE object_id = '{rv_id}'"
            )
            read_lag = rf[0][0] if rf and rf[0][0] is not None else None
            rows.append((ret, mon.max_stall_ms(), mon.max_lag(), read_lag))
        finally:
            churner.stop()
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    header = f"{'retention':>10} {'write_stall':>12} {'write_lag':>10} {'read_lag':>10}"
    print("\n" + header)
    print("-" * len(header))

    for ret, stall, lag, rlag in rows:
        print(f"{ret:>10} {_ms(stall):>12} {_ms(lag):>10} {_ms(rlag):>10}")
    print(
        "\nFlat write_stall across retention => RETAIN HISTORY does not degrade "
        "live freshness (it holds the read frontier back, as designed). read_lag "
        "tracks the retention window."
    )


SINK_PROXY_PORT = 9095


def workflow_sink(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Sink backpressure: when a Kafka sink can't emit fast enough, does it lag
    only itself, or does it stall its *input* MV's freshness too?

    MZ's sink emit path is routed through toxiproxy (MZ -> toxiproxy -> kafka via
    the TOXI advertised listener) and the produce stream is bandwidth-capped. We
    monitor both the sink's frontier and the input MV's frontier under a heavy
    write load. If MZ does not backpressure inputs, the input MV stays fresh
    while only the sink lags (the sink's read hold blocks input *compaction*, not
    input *freshness*).
    """
    parser.add_argument(
        "--rates-kbps", default="0,200,20", help="produce-path rate caps in KB/s"
    )
    parser.add_argument("--msgs-per-tick", type=int, default=2000)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    args = parser.parse_args()
    rates = [int(x) for x in args.rates_kbps.split(",") if x.strip()]

    c.up("kafka", "toxiproxy", "materialized")
    _toxiproxy(c, "DELETE", "/proxies/ksink")
    _toxiproxy(
        c,
        "POST",
        "/proxies",
        {
            "name": "ksink",
            "listen": f"0.0.0.0:{SINK_PROXY_PORT}",
            "upstream": f"kafka:{SINK_PROXY_PORT}",
            "enabled": True,
        },
    )
    c.sql(
        f"CREATE CONNECTION IF NOT EXISTS kafka_throttled TO KAFKA "
        f"(BROKER 'toxiproxy:{SINK_PROXY_PORT}', SECURITY PROTOCOL PLAINTEXT)"
    )

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS sink_src CASCADE")
    c.sql("CREATE TABLE sink_src (val bigint)")
    c.sql("INSERT INTO sink_src VALUES (0)")
    c.sql("CREATE MATERIALIZED VIEW m IN CLUSTER test AS SELECT val FROM sink_src")
    c.sql(
        "CREATE SINK s IN CLUSTER test FROM m "
        "INTO KAFKA CONNECTION kafka_throttled (TOPIC 'freshness-sink-bp') "
        "FORMAT JSON ENVELOPE DEBEZIUM"
    )
    m_id = _object_id(c, "m")
    sink_id = _object_id(c, "s")
    _wait_hydrated(c, m_id, timeout=60)
    _wait_fresh(c, sink_id, args.max_stall_ms, timeout=120)

    # A heavy writer so the sink has real volume to emit.
    writer = ProbeWriter(c, "sink_src", rows_per_batch=args.msgs_per_tick)
    writer.start()
    rows: list[tuple[int, int | None, int | None]] = []
    try:
        for rate in rates:
            _toxiproxy(c, "DELETE", "/proxies/ksink/toxics/bw_up")
            if rate > 0:
                _toxiproxy(
                    c,
                    "POST",
                    "/proxies/ksink/toxics",
                    {
                        "name": "bw_up",
                        "type": "bandwidth",
                        "stream": "upstream",
                        "attributes": {"rate": rate},
                    },
                )
            print(f"\n=== sink: produce-path cap = {rate}KB/s ===")
            time.sleep(3)
            mon_m = FreshnessMonitor(c, m_id)
            mon_s = FreshnessMonitor(c, sink_id)
            mon_m.start()
            mon_s.start()
            time.sleep(args.window_seconds)
            mon_m.stop()
            mon_s.stop()
            rows.append((rate, mon_m.max_lag(), mon_s.max_lag()))
    finally:
        writer.stop()
        _toxiproxy(c, "DELETE", "/proxies/ksink/toxics/bw_up")
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    header = f"{'sink_rate':>10} {'input_mv_lag':>13} {'sink_lag':>10}"
    print("\n" + header)
    print("-" * len(header))

    for rate, ml, sl in rows:
        print(f"{f'{rate}KB/s':>10} {_ms(ml):>13} {_ms(sl):>10}")
    print(
        "\ninput_mv_lag flat while sink_lag grows => sink backpressure does NOT "
        "stall its input's freshness; only the sink itself falls behind."
    )


def workflow_xchain(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Cross-cluster contagion DEPTH: does the stall cascade multiple hops?

    A 3-cluster chain c0 -> c1 -> c2: MV m0 on c0 (from a table), m1 on c1 reads
    m0, m2 on c2 reads m1. We place the expensive `max` on one cluster and watch
    all three frontiers. Starving the HEAD (c0) should stall m1 and m2 two hops
    away; starving the MIDDLE (c1) should leave m0 (upstream) fresh but stall m2.
    """
    parser.add_argument("--starve-on", default="c0,c1", help="which cluster: c0|c1|c2")
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()
    starve = [s.strip() for s in args.starve_on.split(",") if s.strip()]

    c.up("materialized")
    _create_big(c, args.rows)

    results: list[tuple[str, int | None, int | None, int | None]] = []
    for where in starve:
        print(f"\n=== xchain: expensive op on '{where}' ===")
        for cl in ("c0", "c1", "c2"):
            c.sql(f"DROP CLUSTER IF EXISTS {cl} CASCADE")
            c.sql(f"CREATE CLUSTER {cl} SIZE = 'scale=1,workers=1'")
        c.sql("DROP TABLE IF EXISTS chain_src CASCADE")
        c.sql("CREATE TABLE chain_src (val bigint)")
        c.sql("INSERT INTO chain_src VALUES (0)")
        c.sql(
            "CREATE MATERIALIZED VIEW m0 IN CLUSTER c0 AS "
            "SELECT val, val + 1 AS v FROM chain_src"
        )
        c.sql(
            "CREATE MATERIALIZED VIEW m1 IN CLUSTER c1 AS SELECT count(*) AS n FROM m0"
        )
        c.sql("CREATE MATERIALIZED VIEW m2 IN CLUSTER c2 AS SELECT n + 1 AS n FROM m1")
        ids = {m: _object_id(c, m) for m in ("m0", "m1", "m2")}
        for oid in ids.values():
            _wait_hydrated(c, oid, timeout=120)
        _wait_fresh(c, ids["m2"], args.max_stall_ms, timeout=60)

        mons = {m: FreshnessMonitor(c, oid) for m, oid in ids.items()}
        writer = ProbeWriter(c, "chain_src")
        for mon in mons.values():
            mon.start()
        writer.start()
        try:
            c.sql(
                f"CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER {where} AS "
                f"SELECT key, max(id) AS m FROM big GROUP BY key"
            )
            _wait_hydrated(
                c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s
            )
            time.sleep(2)
        finally:
            writer.stop()
            for mon in mons.values():
                mon.stop()
            for cl in ("c0", "c1", "c2"):
                c.sql(f"DROP CLUSTER IF EXISTS {cl} CASCADE")
        results.append(
            (
                where,
                mons["m0"].max_stall_ms(),
                mons["m1"].max_stall_ms(),
                mons["m2"].max_stall_ms(),
            )
        )

    header = (
        f"{'starve_on':<10} {'m0_stall(c0)':>14} {'m1_stall(c1)':>14} "
        f"{'m2_stall(c2)':>14}"
    )
    print("\n" + header)
    print("-" * len(header))

    for where, s0, s1, s2 in results:
        print(f"{where:<10} {_ms(s0):>14} {_ms(s1):>14} {_ms(s2):>14}")
    print(
        "\nStarving c0 stalling m2 (2 hops away) => contagion cascades the full "
        "dependency chain; only objects upstream of the starved cluster stay fresh."
    )


def workflow_subscribe(c: Composition, parser: WorkflowArgumentParser) -> None:
    """SUBSCRIBE progress latency under contention -- the streaming-read analog
    of the strict-serializable hang.

    A SUBSCRIBE ... WITH (PROGRESS) emits progress timestamps; the gap between
    wallclock and the latest progress is what a streaming consumer experiences as
    staleness. We hold a SUBSCRIBE open on the cheap probe while the co-located
    `max` hydrates, and report the largest gap between consecutive progress
    messages (how long the stream went silent).
    """
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    _create_big(c, args.rows)
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql(
        "CREATE MATERIALIZED VIEW probe_mv IN CLUSTER test AS "
        "SELECT count(*) AS n FROM probe_src"
    )
    _wait_hydrated(c, _object_id(c, "probe_mv"), timeout=60)

    # Background thread holds a SUBSCRIBE open and records progress-message gaps.
    stop = Event()
    state: dict = {"max_gap_ms": 0, "last": None}

    def _subscribe() -> None:
        conn = _connect(c)
        try:
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("DECLARE cur CURSOR FOR SUBSCRIBE probe_mv WITH (PROGRESS)")
            while not stop.is_set():
                cur.execute("FETCH ALL cur")
                now = time.time()
                if cur.rowcount and cur.rowcount > 0:
                    cur.fetchall()
                    if state["last"] is not None:
                        gap = (now - state["last"]) * 1000
                        state["max_gap_ms"] = max(state["max_gap_ms"], int(gap))
                    state["last"] = now
                stop.wait(0.1)
        finally:
            conn.close()

    writer = ProbeWriter(c, "probe_src")
    sub = PropagatingThread(target=_subscribe)
    writer.start()
    sub.start()
    time.sleep(3)  # let SUBSCRIBE warm up
    state["max_gap_ms"] = 0  # reset after warmup
    try:
        c.sql(
            "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
            "SELECT key, max(id) AS m FROM big GROUP BY key"
        )
        start = time.time()
        _wait_hydrated(
            c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s
        )
        expensive_ms = int((time.time() - start) * 1000)
        time.sleep(2)
    finally:
        stop.set()
        sub.join()
        writer.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    print(f"\nexpensive `max` hydration: {expensive_ms}ms")
    print(f"largest SUBSCRIBE progress gap on the probe: {state['max_gap_ms']}ms")
    print(
        "\nA multi-second progress gap => a streaming SUBSCRIBE consumer sees the "
        "probe go silent for ~the stall, the streaming analog of the hung read."
    )


def workflow_recovery(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Recovery / 0dt: many starving MVs rehydrating at once (a restart or
    blue-green deploy). Does the probe's recovery wedge far past a single
    object's rehydration when N hierarchical reduces rehydrate together?

    Sweeps the count of co-located `max` MVs; for each, hydrates them, drops the
    replica (REPLICATION FACTOR 0 -> 1) so everything rehydrates from persist at
    once, and measures how long the cheap probe takes to come back fresh.
    """
    parser.add_argument("--counts", default="1,5,20", help="comma-separated MV counts")
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()
    counts = [int(x) for x in args.counts.split(",") if x.strip()]

    c.up("materialized")
    _create_big(c, args.rows)

    rows: list[tuple[int, int, int | None, int]] = []
    for n in counts:
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
        c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
        c.sql("CREATE TABLE probe_src (val bigint)")
        c.sql("INSERT INTO probe_src VALUES (0)")
        c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
        probe_id = _object_id(c, "probe_idx")
        names = [f"rec_{i}" for i in range(n)]
        for i, nm in enumerate(names):
            c.sql(
                f"CREATE MATERIALIZED VIEW {nm} IN CLUSTER test AS "
                f"SELECT key, max(id) + {i} AS m FROM big GROUP BY key"
            )
        _wait_hydrated(c, probe_id, timeout=120)
        _wait_all_hydrated(c, names, timeout=args.hydration_timeout_s)
        _wait_fresh(c, probe_id, args.max_stall_ms, timeout=60)

        monitor = FreshnessMonitor(c, probe_id)
        writer = ProbeWriter(c, "probe_src")
        monitor.start()
        writer.start()
        try:
            print(f"\n=== recovery: {n} max MVs rehydrating together ===")
            c.sql("ALTER CLUSTER test SET (REPLICATION FACTOR 0)")
            time.sleep(1)
            c.sql("ALTER CLUSTER test SET (REPLICATION FACTOR 1)")
            t0 = time.time()
            recovery_ms = int(
                _wait_fresh(
                    c, probe_id, args.max_stall_ms, timeout=args.hydration_timeout_s
                )
                * 1000
            )
            recovery_wallclock = time.time()
            _wait_all_hydrated(c, names, timeout=args.hydration_timeout_s)
            fleet_ms = int((time.time() - t0) * 1000)
            # The real question: does the probe STAY fresh during the (long)
            # fleet rehydration, or stall again as each reduce rehydrates?
            post_stall = monitor.max_stall_ms(since=recovery_wallclock)
        finally:
            writer.stop()
            monitor.stop()
            c.sql("DROP CLUSTER IF EXISTS test CASCADE")
        rows.append((n, recovery_ms, post_stall, fleet_ms))

    header = (
        f"{'n_mvs':>6} {'first_recovery':>15} {'post_rec_stall':>15} "
        f"{'fleet_rehydrate':>16}"
    )
    print("\n" + header)
    print("-" * len(header))

    for n, rec, post, fleet in rows:
        print(f"{n:>6} {_ms(rec):>15} {_ms(post):>15} {_ms(fleet):>16}")
    print(
        "\nIf post_rec_stall grows with the number of co-rehydrating reduces, a "
        "restart/0dt with many hierarchical MVs delays everyone's first-fresh far "
        "beyond a single object."
    )


def workflow_memory(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Memory-pressure-induced staleness: does a co-located arrangement held near
    the cluster's memory limit stall a cheap probe's freshness?

    MZ is Rust (no managed GC), so memory pressure tends to manifest as a hard
    cgroup OOM kill (-> replica restart -> rehydration, covered by `recovery`),
    not a gradual slowdown. We build a large index (rows x payload) to a
    high-but-safe fraction of the 4 GiB limit, hold it, and check whether the
    probe stays fresh under sustained memory pressure short of OOM.
    """
    parser.add_argument("--rows", type=int, default=6_000_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    c.sql("DROP TABLE IF EXISTS mem CASCADE")
    c.sql("CREATE TABLE mem (key bigint, payload text)")
    chunk = 500_000
    done = 0
    while done < args.rows:
        nrows = min(chunk, args.rows - done)
        c.sql(
            f"INSERT INTO mem SELECT g.s, repeat('x', {args.payload_bytes}) "
            f"FROM generate_series({done + 1}, {done + nrows}) AS g(s)",
            print_statement=False,
        )
        done += nrows
    approx_gb = args.rows * args.payload_bytes / 1e9

    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
    probe_id = _object_id(c, "probe_idx")
    _wait_hydrated(c, probe_id, timeout=60)

    monitor = FreshnessMonitor(c, probe_id)
    writer = ProbeWriter(c, "probe_src")
    monitor.start()
    writer.start()
    oom = False
    try:
        # The large arrangement (~approx_gb held in the cluster's memory).
        c.sql("CREATE INDEX mem_idx IN CLUSTER test ON mem (key, payload)")
        _wait_hydrated(c, _object_id(c, "mem_idx"), timeout=args.hydration_timeout_s)
        time.sleep(args.window_seconds)  # hold under pressure, watch the probe
    except Exception as e:
        oom = "exited" in str(e).lower() or "unhealthy" in str(e).lower()
        if not oom:
            raise
    finally:
        writer.stop()
        monitor.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    stall = monitor.max_stall_ms()
    print(f"\nlarge arrangement ~{approx_gb:.1f}GB on a 4GiB cluster; oom={oom}")
    print(f"probe max stall under memory pressure: {stall}ms")
    print(
        "\nFresh under sustained pressure => memory pressure does not gradually "
        "stall freshness; it surfaces as a hard OOM (-> rehydration, see "
        "`recovery`), not creeping lag."
    )


class _FloodThread:
    """Hammers the coordinator: rapid DDL (catalog txns) and/or peeks (each takes
    a timestamp from the oracle)."""

    def __init__(self, c: Composition, tid: int, mode: str):
        self.c = c
        self.tid = tid
        self.mode = mode
        self._stop = Event()
        self._thread = PropagatingThread(target=self._run)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn = _connect(self.c)
        try:
            cur = conn.cursor()
            i = 0
            while not self._stop.is_set():
                try:
                    if self.mode in ("ddl", "both"):
                        t = f"flood_{self.tid}_{i}"
                        cur.execute(f"CREATE TABLE {t} (a int)".encode())
                        cur.execute(f"DROP TABLE {t}".encode())
                    if self.mode in ("peek", "both"):
                        cur.execute(b"SELECT * FROM oracle_idle")
                        cur.fetchall()
                except Exception:
                    pass  # transient coordinator errors under load are expected
                i += 1
        finally:
            conn.close()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def workflow_oracle(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Coordinator / timestamp-oracle contention: a GLOBAL freshness shape.

    Unlike compute starvation (per-cluster), the coordinator and timestamp oracle
    are shared by the whole environment: they advance table frontiers and hand
    out query timestamps. We hammer them (DDL flood and/or peek flood) and watch
    an *idle* materialized view (no writes, no co-located compute) -- its write
    frontier advances purely via coordinator/oracle table-advancement, so if it
    freezes, frontier advancement has stalled environment-wide, independent of
    any cluster. A second idle MV on the default cluster confirms it is global.
    """
    parser.add_argument("--mode", default="ddl", help="ddl | peek | both")
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    args = parser.parse_args()

    c.up("materialized")
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS oracle_src CASCADE")
    c.sql("CREATE TABLE oracle_src (val bigint)")
    c.sql("INSERT INTO oracle_src VALUES (0)")
    # Idle MVs (never written after creation) on `test` and on the default
    # cluster -- both track wallclock purely via coordinator advancement.
    c.sql(
        "CREATE MATERIALIZED VIEW oracle_idle IN CLUSTER test AS "
        "SELECT count(*) AS n FROM oracle_src"
    )
    c.sql(
        "CREATE MATERIALIZED VIEW oracle_idle_default IN CLUSTER quickstart AS "
        "SELECT count(*) + 1 AS n FROM oracle_src"
    )
    test_id = _object_id(c, "oracle_idle")
    default_id = _object_id(c, "oracle_idle_default")
    _wait_hydrated(c, test_id, timeout=60)
    _wait_fresh(c, test_id, args.max_stall_ms, timeout=60)
    _wait_fresh(c, default_id, args.max_stall_ms, timeout=60)

    mon_test = FreshnessMonitor(c, test_id)
    mon_default = FreshnessMonitor(c, default_id)
    mon_test.start()
    mon_default.start()
    time.sleep(4)  # quiet baseline
    baseline_test = mon_test.max_stall_ms()

    floods = [_FloodThread(c, t, args.mode) for t in range(args.concurrency)]
    flood_start = time.time()
    for f in floods:
        f.start()
    try:
        print(f"\n=== oracle: {args.concurrency}x {args.mode} flood ===")
        time.sleep(args.window_seconds)
    finally:
        for f in floods:
            f.stop()
        mon_test.stop()
        mon_default.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    test_stall = mon_test.max_stall_ms(since=flood_start)
    default_stall = mon_default.max_stall_ms(since=flood_start)

    print(f"\n{'object':<26} {'baseline_stall':>15} {'under_flood_stall':>18}")
    print("-" * 61)
    print(
        f"{'idle MV (test cluster)':<26} {_ms(baseline_test):>15} {_ms(test_stall):>18}"
    )
    print(f"{'idle MV (default cluster)':<26} {'-':>15} {_ms(default_stall):>18}")
    print(
        "\nIf BOTH idle MVs stall under the flood (no co-located compute, no "
        "writes), coordinator/oracle contention freezes frontier advancement "
        "environment-wide -- a global freshness shape distinct from per-cluster "
        "operator starvation."
    )


def workflow_temporal(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Temporal filter (`mz_now()`) feeding a hierarchical reduce: does the
    synchronized retraction when a window expires starve co-located objects?

    A common customer pattern is a sliding window `WHERE mz_now() <= ts + INTERVAL
    ...` feeding an aggregate. When a large batch of rows shares an expiry, they
    all retract at the same wallclock instant -- one big diff into the reduce, on
    a wallclock schedule. Given BUG 1 (a large diff starves the hierarchical
    reduce), this should produce a *periodic, automatic* stall with no user
    action. We load a big batch all expiring together, let it settle, then watch
    a co-located probe across the expiry instant.
    """
    parser.add_argument("--rows", type=int, default=3_000_000)
    parser.add_argument("--expire-in-s", type=int, default=15)
    parser.add_argument("--max-stall-ms", type=int, default=DEFAULT_MAX_STALL_MS)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS events CASCADE")
    # expiry is epoch-ms; mz_now() is compared against it directly.
    c.sql("CREATE TABLE events (key bigint, id bigint, expiry numeric)")
    # A hierarchical reduce behind a temporal filter: rows vanish from the
    # window (and thus retract from the max) once mz_now() passes `expiry`.
    c.sql(
        "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
        "SELECT key, max(id) AS m FROM events WHERE mz_now() < expiry GROUP BY key"
    )
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
    probe_id = _object_id(c, "probe_idx")
    _wait_hydrated(c, probe_id, timeout=60)

    # All rows share one expiry instant `expire-in-s` from now -> one synchronized
    # retraction when mz_now() passes it.
    now_ms = c.sql_query("SELECT (extract(epoch FROM now()) * 1000)::bigint")[0][0]
    expiry = now_ms + args.expire_in_s * 1000
    chunk = 500_000
    done = 0
    while done < args.rows:
        nrows = min(chunk, args.rows - done)
        c.sql(
            f"INSERT INTO events SELECT (g.s % {DISTINCT_KEYS})::bigint, g.s, "
            f"{expiry}::numeric FROM generate_series({done + 1}, {done + nrows}) "
            f"AS g(s)",
            print_statement=False,
        )
        done += nrows
    _wait_hydrated(c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s)
    _wait_fresh(c, probe_id, args.max_stall_ms, timeout=120)

    monitor = FreshnessMonitor(c, probe_id)
    writer = ProbeWriter(c, "probe_src")
    monitor.start()
    writer.start()
    try:
        # Sit across the expiry instant; the whole window retracts at once.
        wait_s = max(0, (expiry - now_ms) / 1000) + 8
        print(
            f"\n=== temporal: {args.rows} rows expiring together in ~{args.expire_in_s}s ==="
        )
        time.sleep(wait_s)
    finally:
        writer.stop()
        monitor.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    stall = monitor.max_stall_ms()
    ok = stall is not None and stall <= args.max_stall_ms
    print(
        f"\nprobe max stall across the temporal expiry: {stall}ms -> {'PASS' if ok else 'FAIL'}"
    )
    print(
        "\nFAIL => a sliding-window (`mz_now()`) expiry feeding a hierarchical "
        "reduce produces a periodic, automatic freshness stall with no user "
        "action -- BUG 1 triggered on a wallclock schedule."
    )


def workflow_validate(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Two meta-checks during a reduce-hydration stall:

    (#4 correctness) Is the stall purely a *liveness* problem, or does the system
        ever serve data inconsistent with its reported frontier? We read
        `count(*)` from the probe MV and from its source table *in one
        serializable transaction* (same read timestamp) -- they must always be
        equal, even when stale. And after settling we check the expensive MV's
        result against a fresh recomputation from `big`.
    (#5 observability) Does the user-facing lag view reflect the stall? We sample
        our direct `now()-write_frontier` and `mz_internal.mz_wallclock_global_lag`
        for the probe at the same time and compare -- if the view stays ~0 while
        the frontier is frozen, customers are blind to the bug.
    """
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    _create_big(c, args.rows)
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql(
        "CREATE MATERIALIZED VIEW probe_mv IN CLUSTER test AS "
        "SELECT count(*) AS n FROM probe_src"
    )
    probe_id = _object_id(c, "probe_mv")
    _wait_hydrated(c, probe_id, timeout=60)

    stop = Event()
    state = {
        "max_view_lag": 0,
        "view_reported": False,
        "consistency_violation": None,
    }

    # (#5) The AUTHORITATIVE direct stall comes from the lightweight 200 ms
    # FreshnessMonitor; a separate light loop samples the user-facing lag view
    # at the same cadence and does the (heavier) serializable consistency check
    # only every ~2 s so it cannot throttle the view sampling.
    mon = FreshnessMonitor(c, probe_id)

    def _watch() -> None:
        conn = _connect(c)
        cur = conn.cursor()
        i = 0
        while not stop.is_set():
            try:
                cur.execute(
                    "SELECT extract(epoch FROM lag)*1000 FROM "
                    f"mz_internal.mz_wallclock_global_lag WHERE object_id = '{probe_id}'".encode()
                )
                vr = cur.fetchone()
                if vr and vr[0] is not None:
                    state["view_reported"] = True
                    state["max_view_lag"] = max(state["max_view_lag"], int(vr[0]))
                if i % 10 == 0:  # (#4) consistency check, throttled
                    cur.execute(b"BEGIN")
                    cur.execute(b"SET LOCAL transaction_isolation = 'serializable'")
                    cur.execute(b"SELECT n FROM probe_mv")
                    mv_row = cur.fetchone()
                    cur.execute(b"SELECT count(*) FROM probe_src")
                    tbl_row = cur.fetchone()
                    cur.execute(b"COMMIT")
                    if (
                        mv_row is not None
                        and tbl_row is not None
                        and mv_row[0] != tbl_row[0]
                        and state["consistency_violation"] is None
                    ):
                        state["consistency_violation"] = (mv_row[0], tbl_row[0])
            except Exception:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
            i += 1
            stop.wait(0.2)
        conn.close()

    writer = ProbeWriter(c, "probe_src")
    watcher = PropagatingThread(target=_watch)
    writer.start()
    mon.start()
    watcher.start()
    try:
        c.sql(
            "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
            "SELECT key, max(id) AS m FROM big GROUP BY key"
        )
        _wait_hydrated(
            c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s
        )
        time.sleep(2)
    finally:
        stop.set()
        watcher.join()
        mon.stop()
        writer.stop()

    # (#4) result correctness: expensive MV vs a fresh recomputation from `big`.
    mv_chk = c.sql_query("SELECT count(*), coalesce(sum(m), 0) FROM expensive_mv")[0]
    gt_chk = c.sql_query(
        "SELECT count(*), coalesce(sum(m), 0) FROM "
        "(SELECT key, max(id) AS m FROM big GROUP BY key) t"
    )[0]
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    result_ok = tuple(mv_chk) == tuple(gt_chk)
    consistent = state["consistency_violation"] is None
    print("\n--- #4 correctness ---")
    print(
        f"  MV-vs-table consistency during stall: {'OK' if consistent else 'VIOLATION ' + str(state['consistency_violation'])}"
    )
    print(
        f"  expensive MV result vs ground truth: mv={tuple(mv_chk)} gt={tuple(gt_chk)} -> {'OK' if result_ok else 'WRONG'}"
    )
    print("--- #5 observability ---")
    print(f"  our direct frontier stall:        {mon.max_stall_ms()}ms")
    print(
        f"  mz_wallclock_global_lag (view):   {state['max_view_lag']}ms (reported={state['view_reported']})"
    )
    print(
        "\nConsistency OK + result OK => the bug is purely LIVENESS (stale, not "
        "wrong). If the view lag << our frontier stall, the user-facing freshness "
        "metric under-reports the stall (customers are partly blind)."
    )


def workflow_combined(c: Composition, parser: WorkflowArgumentParser) -> None:
    """#6 Emergent combined-workload contention: run a realistic mix on one
    cluster simultaneously -- continuous ingest, a peek flood, AND the expensive
    reduce hydration -- and compare the probe's stall to the hydration-alone
    baseline. Tests whether the combination is worse than its parts.
    """
    parser.add_argument("--rows", type=int, default=3_000_000)
    parser.add_argument("--peekers", type=int, default=8)
    parser.add_argument("--hydration-timeout-s", type=int, default=900)
    args = parser.parse_args()

    c.up("materialized")
    _create_big(c, args.rows)
    c.sql("DROP CLUSTER IF EXISTS test CASCADE")
    c.sql("CREATE CLUSTER test SIZE = 'scale=1,workers=1'")
    c.sql("DROP TABLE IF EXISTS probe_src CASCADE")
    c.sql("CREATE TABLE probe_src (val bigint)")
    c.sql("INSERT INTO probe_src VALUES (0)")
    c.sql("CREATE INDEX probe_idx IN CLUSTER test ON probe_src (val)")
    # A second table+MV on the same cluster, continuously ingested (extra load).
    c.sql("DROP TABLE IF EXISTS churn CASCADE")
    c.sql("CREATE TABLE churn (val bigint)")
    c.sql(
        "CREATE MATERIALIZED VIEW churn_mv IN CLUSTER test AS SELECT count(*) AS n FROM churn"
    )
    probe_id = _object_id(c, "probe_idx")
    _wait_hydrated(c, probe_id, timeout=60)
    _wait_fresh(c, probe_id, DEFAULT_MAX_STALL_MS, timeout=60)

    monitor = FreshnessMonitor(c, probe_id)
    writer = ProbeWriter(c, "probe_src")
    churn_writer = ProbeWriter(c, "churn", rows_per_batch=5000)
    # Peek flood against objects on the test cluster (real query load).
    stop = Event()

    def _peek() -> None:
        conn = _connect(c)
        cur = conn.cursor()
        while not stop.is_set():
            try:
                cur.execute("SELECT n FROM churn_mv")
                cur.fetchall()
            except Exception:
                pass
        conn.close()

    peekers = [PropagatingThread(target=_peek) for _ in range(args.peekers)]
    monitor.start()
    writer.start()
    churn_writer.start()
    for p in peekers:
        p.start()
    try:
        print(f"\n=== combined: ingest + {args.peekers} peekers + reduce hydration ===")
        c.sql(
            "CREATE MATERIALIZED VIEW expensive_mv IN CLUSTER test AS "
            "SELECT key, max(id) AS m FROM big GROUP BY key"
        )
        _wait_hydrated(
            c, _object_id(c, "expensive_mv"), timeout=args.hydration_timeout_s
        )
        time.sleep(2)
    finally:
        stop.set()
        for p in peekers:
            p.join()
        churn_writer.stop()
        writer.stop()
        monitor.stop()
        c.sql("DROP CLUSTER IF EXISTS test CASCADE")

    print(
        f"\nprobe max stall under combined load: {monitor.max_stall_ms()}ms"
        "\n(compare to the hydration-alone baseline ~18s at 3M rows: combined "
        ">> baseline would indicate emergent contention; ~= baseline means the "
        "single non-yielding reduce already dominates.)"
    )
