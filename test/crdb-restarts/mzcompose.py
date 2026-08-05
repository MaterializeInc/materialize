# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Disrupt Cockroach and verify that Materialize recovers from it.
"""

import threading
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from textwrap import dedent

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as ServiceDef
from materialize.mzcompose.service import ServiceHealthcheck
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.util import selected_by_name

CRDB_NODE_COUNT = 4
TESTDRIVE_TIMEOUT = (
    "80s"  # We expect any CRDB disruption to not disrupt Mz for more than this timeout
)

COCKROACH_HEALTHCHECK_DISABLED = ServiceHealthcheck(
    test="/bin/true",
    interval="1s",
    start_period="30s",
)

INIT_SCRIPT = dedent("""
    # This source will persist throughout the CRDB rolling restart
    > DROP CLUSTER IF EXISTS s_old_cluster CASCADE;
    > CREATE CLUSTER s_old_cluster SIZE = 'scale=4,workers=4';
    > CREATE SOURCE s_old IN CLUSTER s_old_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');
    > CREATE TABLE s_old_tbl FROM SOURCE s_old;

    > SELECT COUNT(*) > 1 FROM s_old_tbl;
    true

    # This source is recreated periodically
    > DROP CLUSTER IF EXISTS s_new_cluster CASCADE;
    > CREATE CLUSTER s_new_cluster SIZE = 'scale=4,workers=4';
    > CREATE SOURCE s_new IN CLUSTER s_new_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');
    > CREATE TABLE s_new_tbl FROM SOURCE s_new;

    > SELECT COUNT(*) > 1 FROM s_new_tbl;
    true
    """)

VALIDATE_SCRIPT = dedent("""
    > SELECT COUNT(*) > 1 FROM s_old_tbl;
    true

    # This source is recreated periodically
    > DROP SOURCE s_new CASCADE;
    > CREATE SOURCE s_new IN CLUSTER s_new_cluster FROM LOAD GENERATOR COUNTER (TICK INTERVAL '0.1s');
    > CREATE TABLE s_new_tbl FROM SOURCE s_new;

    > SELECT COUNT(*) > 1 FROM s_new_tbl;
    true
    """)


ALL_COCKROACH_NODES = ",".join(
    [f"cockroach{id}:26257" for id in range(CRDB_NODE_COUNT)]
)

SERVICES = [
    Testdrive(default_timeout=TESTDRIVE_TIMEOUT, no_reset=True),
    # TCP round-robin load balancer over the CRDB nodes, standing in for the
    # cloud load balancer. Connecting through it is what spreads a connection
    # pool across nodes; connecting to the shared `cockroach` DNS alias does
    # not (the client picks one resolved address and sticks with it).
    ServiceDef(
        name="crdb-lb",
        config={
            "image": "haproxy:2.9",
            "ports": [26257],
            "volumes": ["./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro"],
            "networks": {"default": {"aliases": ["crdb-lb"]}},
        },
    ),
    Materialized(
        # Consensus runs against CockroachDB here, so
        # `persist_pg_consensus_read_committed` must stay off (the CRDB_*
        # queries are only linearizable under SERIALIZABLE). Signalling the
        # backend lets the service force the flag off.
        metadata_store="cockroach",
        depends_on=[f"cockroach{id}" for id in range(CRDB_NODE_COUNT)],
        options=[
            "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
            "--timestamp-oracle-url=postgres://root@cockroach:26257?options=--search_path=tsoracle",
        ],
    ),
    *[
        Cockroach(
            setup_materialize=True,
            name=f"cockroach{id}",
            command=[
                "start",
                "--insecure",
                f"--store=cockroach{id}",
                "--listen-addr=0.0.0.0:26257",
                f"--advertise-addr=cockroach{id}:26257",
                "--http-addr=0.0.0.0:8080",
                f"--join={ALL_COCKROACH_NODES}",
            ],
            healthcheck=COCKROACH_HEALTHCHECK_DISABLED,
        )
        for id in range(CRDB_NODE_COUNT)
    ],
]


@dataclass
class CrdbDisruption:
    name: str
    disruption: Callable


DISRUPTIONS = [
    # Unfortunately this disruption is too aggressive and causes CRDB to enter in a state
    # where it is no longer able to service queries, with either no error or errors about
    # 'lost quorum' or 'encountered poisoned latch'
    #
    # Most likely the test kills and restarts the nodes too fast for CRDB to handle, even though
    # the nodes are taken out in succession one by one and never in parallel.
    #
    # CrdbDisruption(
    #    name="sigkill",
    #    disruption=lambda c, id: c.kill(f"cockroach{id}"),
    # ),
    CrdbDisruption(
        name="sigterm",
        disruption=lambda c, id: c.kill(f"cockroach{id}", signal="SIGTERM"),
    ),
    CrdbDisruption(
        name="drain",
        disruption=lambda c, id: c.exec(
            # Execute the 'drain' command on a different node from the one that we are draining
            #
            # Draining may sometimes time out, but we continue with the restart in case this happens,
            # as a real life CRDB upgrade procedure will most likely also ignore such a timeout.
            f"cockroach{(id % 2) + 1}",
            "cockroach",
            "node",
            "drain",
            str(id + 1),
            "--insecure",
            check=False,
        ),
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Perform rolling restarts on a CRDB cluster with CRDB_NODE_COUNT nodes and
    confirm that Mz does not hang for longer than the expected."""
    parser.add_argument("disruptions", nargs="*", default=[d.name for d in DISRUPTIONS])

    args = parser.parse_args()

    for d in selected_by_name(args.disruptions, DISRUPTIONS):
        run_disruption(c, d)


def bootstrap_crdb_cluster(c: Composition) -> None:
    c.up(*[f"cockroach{id}" for id in range(CRDB_NODE_COUNT)])

    c.exec("cockroach0", "cockroach", "init", "--insecure", "--host=localhost:26257")

    for query in [
        "SET CLUSTER SETTING sql.stats.forecasts.enabled = false",
        "CREATE SCHEMA IF NOT EXISTS consensus",
        "CREATE SCHEMA IF NOT EXISTS storage",
        "CREATE SCHEMA IF NOT EXISTS adapter",
        "CREATE SCHEMA IF NOT EXISTS tsoracle",
    ]:
        c.exec("cockroach0", "cockroach", "sql", "--insecure", "-e", query)


def run_disruption(c: Composition, d: CrdbDisruption) -> None:
    print(f"--- Running Disruption {d.name} ...")
    c.down(destroy_volumes=True, sanity_restart_mz=False)

    bootstrap_crdb_cluster(c)

    c.up("materialized", Service("testdrive", idle=True))

    # We expect the testdrive fragment to complete within Testdrive's default_timeout
    # This will indicate that Mz has not hung for a prolonged period of time
    # as a result of the disruption we just introduced
    c.testdrive(input=INIT_SCRIPT)

    # Messing with cockroach node #0 borks the cluster permanently, so we start from node #1
    for id in range(1, CRDB_NODE_COUNT):
        d.disruption(c, id)

        # Restart the node we just disrupted so that we can safely disrupt another node
        try:
            # Node may have died already, so we eat any docker-compose exceptions
            c.kill(f"cockroach{id}")
        except UIError:
            pass
        c.up(f"cockroach{id}")

        # Confirm things continue to work after CRDB is back to full complement
        c.testdrive(input=VALIDATE_SCRIPT)


class MetricsScraper(threading.Thread):
    """Polls environmentd's internal metrics endpoint and records the persist
    consensus connection pool gauges/counters as (time, name, value) samples."""

    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.port = port
        self.samples: list[tuple[float, str, float]] = []
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                body = (
                    urllib.request.urlopen(
                        f"http://localhost:{self.port}/metrics", timeout=2
                    )
                    .read()
                    .decode()
                )
                now = time.time()
                for line in body.splitlines():
                    if line.startswith("mz_persist_postgres_connpool_"):
                        name, _, value = line.rpartition(" ")
                        try:
                            self.samples.append((now, name, float(value)))
                        except ValueError:
                            pass
            except Exception:
                # environmentd may be briefly unreachable; keep polling.
                pass
            self.stop_event.wait(0.5)

    def series(self, name: str) -> list[tuple[float, float]]:
        return [(t, v) for (t, n, v) in self.samples if n == name]


class InsertLoad(threading.Thread):
    """Round-robin single-row inserts across the workload tables, each on its
    own connection, to keep steady write (and thus consensus) traffic going."""

    def __init__(self, port: int, tables: int, thread_id: int, num_threads: int):
        super().__init__(daemon=True)
        self.port = port
        self.tables = tables
        self.thread_id = thread_id
        self.num_threads = num_threads
        self.stop_event = threading.Event()
        self.errors = 0
        self.inserts = 0

    def run(self) -> None:
        import psycopg

        conn = None
        i = self.thread_id
        while not self.stop_event.is_set():
            try:
                if conn is None:
                    conn = psycopg.connect(
                        host="localhost",
                        port=self.port,
                        user="materialize",
                        dbname="materialize",
                    )
                    conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO pool_t{i % self.tables} VALUES (1)".encode()
                    )
                self.inserts += 1
                i += self.num_threads
            except Exception:
                self.errors += 1
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                conn = None
                self.stop_event.wait(0.5)


def summarize_window(
    scraper: MetricsScraper, label: str, start: float, end: float
) -> dict[str, float]:
    def window(name: str) -> list[float]:
        return [v for (t, v) in scraper.series(name) if start <= t <= end]

    def rate(name: str) -> float:
        pts = [(t, v) for (t, v) in scraper.series(name) if start <= t <= end]
        if len(pts) < 2 or pts[-1][0] == pts[0][0]:
            return 0.0
        return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0])

    waiting = window("mz_persist_postgres_connpool_waiting")
    summary = {
        "max_waiting": max(waiting, default=0.0),
        "created_per_s": rate("mz_persist_postgres_connpool_connections_created"),
        "acquire_ms_per_acquire": (
            1000.0
            * rate("mz_persist_postgres_connpool_acquire_seconds")
            / max(rate("mz_persist_postgres_connpool_acquires"), 0.001)
        ),
    }
    print(
        f"--- [{label}] max_waiting={summary['max_waiting']:.0f} "
        f"created/s={summary['created_per_s']:.2f} "
        f"mean_acquire_ms={summary['acquire_ms_per_acquire']:.2f}"
    )
    return summary


def print_session_distribution(c: Composition) -> None:
    """Show how SQL sessions are spread across CRDB nodes, to confirm the
    load balancer is distributing the connection pool."""
    c.exec(
        "cockroach0",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "SELECT node_id, count(*) FROM crdb_internal.cluster_sessions GROUP BY 1 ORDER BY 1",
        check=False,
    )


def workflow_pool_exhaustion(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Reproduce persist consensus connection pool exhaustion during a CRDB
    rolling restart, and measure whether pool pre-warming mitigates it.

    Rolls the CRDB cluster node by node (drain, SIGTERM, restart) under a
    steady multi-table insert load and reports connection pool queueing from
    environmentd's metrics for each drain window."""
    parser.add_argument("--tables", type=int, default=256)
    parser.add_argument("--insert-threads", type=int, default=16)
    parser.add_argument(
        "--pool-max-size",
        type=int,
        default=20,
        help="scaled-down consensus pool cap to make exhaustion reachable locally",
    )
    parser.add_argument(
        "--min-idle",
        type=int,
        default=0,
        help="persist consensus pool pre-warm floor (0 = disabled)",
    )
    parser.add_argument("--settle-secs", type=int, default=45)
    args = parser.parse_args()

    c.down(destroy_volumes=True, sanity_restart_mz=False)
    bootstrap_crdb_cluster(c)

    sysparams = {
        "persist_consensus_connection_pool_max_size": str(args.pool_max_size),
        "max_tables": str(args.tables + 100),
    }
    if args.min_idle > 0:
        sysparams["persist_consensus_connection_pool_min_idle"] = str(args.min_idle)

    with c.override(
        Materialized(
            metadata_store="cockroach",
            depends_on=["crdb-lb"]
            + [f"cockroach{id}" for id in range(CRDB_NODE_COUNT)],
            options=[
                "--persist-consensus-url=postgres://root@crdb-lb:26257?options=--search_path=consensus",
                "--timestamp-oracle-url=postgres://root@crdb-lb:26257?options=--search_path=tsoracle",
            ],
            additional_system_parameter_defaults=sysparams,
        )
    ):
        c.up("crdb-lb", "materialized")

        print(f"--- Creating {args.tables} workload tables ...")
        for i in range(args.tables):
            c.sql(f"CREATE TABLE pool_t{i} (a int)")

        sql_port = c.default_port("materialized")
        metrics_port = c.port("materialized", 6878)

        scraper = MetricsScraper(metrics_port)
        scraper.start()
        loaders = [
            InsertLoad(sql_port, args.tables, i, args.insert_threads)
            for i in range(args.insert_threads)
        ]
        for l in loaders:
            l.start()

        # Let the workload reach steady state and capture a baseline window.
        time.sleep(args.settle_secs)
        baseline_end = time.time()
        summarize_window(
            scraper, "baseline", baseline_end - args.settle_secs, baseline_end
        )
        print_session_distribution(c)

        drain_summaries = []
        # Node #0 stays untouched (see run_disruption: disrupting it borks the
        # cluster), matching the production pattern of rolling a subset of
        # nodes while the LB address keeps resolving.
        for id in range(1, CRDB_NODE_COUNT):
            window_start = time.time()
            print(f"--- Draining and restarting cockroach{id} ...")
            c.exec(
                f"cockroach{(id % 2) + 1}",
                "cockroach",
                "node",
                "drain",
                str(id + 1),
                "--insecure",
                check=False,
            )
            try:
                c.kill(f"cockroach{id}", signal="SIGTERM")
            except UIError:
                pass
            c.up(f"cockroach{id}")
            time.sleep(args.settle_secs)
            window_end = time.time()
            drain_summaries.append(
                summarize_window(
                    scraper, f"drain-cockroach{id}", window_start, window_end
                )
            )

        for l in loaders:
            l.stop_event.set()
        scraper.stop_event.set()
        for l in loaders:
            l.join(timeout=5)
        scraper.join(timeout=5)

        total_inserts = sum(l.inserts for l in loaders)
        total_errors = sum(l.errors for l in loaders)
        print(f"--- Load summary: {total_inserts} inserts, {total_errors} errors")
        max_waiting = max(s["max_waiting"] for s in drain_summaries)
        print(f"--- RESULT: max waiting across drain windows: {max_waiting:.0f}")
