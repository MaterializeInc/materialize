# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""DDL measurements on persistent connections, without retries or implicit transactions."""

import math
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier, Lock

import psycopg
from psycopg import sql


def ddl_statements(
    kind: str, schema: str, cluster: str, name: str, *, index_table: str = "base"
) -> tuple:
    """Return a single-object CREATE/DROP pair. Indexes use the fixture's base table."""
    obj = sql.Identifier(schema, name)
    if kind == "table":
        create = sql.SQL("CREATE TABLE {} (a int, b text)").format(obj)
    elif kind == "view":
        create = sql.SQL("CREATE VIEW {} AS SELECT a, b FROM {} WHERE a > 0").format(
            obj, sql.Identifier(schema, "base")
        )
    elif kind == "index":
        create = sql.SQL("CREATE INDEX {} IN CLUSTER {} ON {} (a)").format(
            sql.Identifier(name),
            sql.Identifier(cluster),
            sql.Identifier(schema, index_table),
        )
    else:
        raise ValueError(f"Unknown DDL object kind: {kind}")
    return create, sql.SQL("DROP {} {}").format(sql.SQL(kind.upper()), obj)


class DdlMeasurement:
    """Own connections for one concurrency point. The caller owns the fixture schema."""

    def __init__(
        self,
        new_connection: Callable[[], psycopg.Connection],
        concurrency: int,
        schema: str,
        cluster: str,
        emit: Callable[[dict], None],
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        self.concurrency = concurrency
        self.schema = schema
        self.cluster = cluster
        self.emit = emit
        self._resources = ExitStack()
        try:
            self.connections = []
            for _ in range(concurrency):
                conn = new_connection()
                self._resources.callback(conn.close)
                conn.autocommit = True
                conn.prepare_threshold = None
                conn.execute("SET statement_timeout = '120s'")
                self.connections.append(conn)
            self.executor = self._resources.enter_context(
                ThreadPoolExecutor(max_workers=concurrency)
            )
        except BaseException:
            self._resources.close()
            raise

    def close(self) -> None:
        self._resources.close()

    def run(
        self,
        kind: str,
        phase: str,
        *,
        slots: int = 1,
        duration: float = 0,
    ) -> dict:
        """Measure a bounded CREATE/DROP batch or sustained CREATE+DROP cycles.

        Batch size is concurrency times slots. A cycle phase drains in-flight
        statements after its deadline, includes them in elapsed time, and ends
        with no probe objects. CREATE batches must be paired with DROP batches.
        SQL acknowledgement is measured, not hydration or resource reclamation.
        Index clients require fixture tables named index_base_<worker>, and one
        slot each. Multiple indexes on one table can depend on each other.
        """
        if phase not in ("create", "drop", "cycle") or slots < 1:
            raise ValueError("invalid DDL phase")
        if phase == "cycle" and duration <= 0:
            raise ValueError("cycle duration must be positive")
        if kind == "index" and slots != 1:
            raise ValueError("index measurements require one slot per client")
        queries = [
            [
                ddl_statements(
                    kind,
                    self.schema,
                    self.cluster,
                    f"probe_{w}_{s}",
                    index_table=f"index_base_{w}",
                )
                for s in range(slots)
            ]
            for w in range(self.concurrency)
        ]
        clock: dict[str, float] = {}

        def start() -> None:
            clock["start_unix"] = time.time()
            clock["start_monotonic"] = time.perf_counter()

        barrier = Barrier(self.concurrency, action=start)

        def worker(w: int) -> list[dict]:
            conn = self.connections[w]
            samples = []
            barrier.wait()
            while True:
                for create, drop in queries[w]:
                    statements = (
                        [("create", create), ("drop", drop)]
                        if phase == "cycle"
                        else [(phase, create if phase == "create" else drop)]
                    )
                    for operation, statement in statements:
                        started = time.perf_counter()
                        sample = {
                            "worker": w,
                            "operation": operation,
                            "start_offset_seconds": started - clock["start_monotonic"],
                        }
                        try:
                            conn.execute(statement)
                        except psycopg.Error as error:
                            # No reconnect or retry: a lost response has an indeterminate outcome.
                            sample["error"] = type(error).__name__
                            sample["sqlstate"] = error.sqlstate
                        sample["latency_seconds"] = time.perf_counter() - started
                        samples.append(sample)
                        if "error" in sample:
                            return samples
                if (
                    phase != "cycle"
                    or time.perf_counter() - clock["start_monotonic"] >= duration
                ):
                    return samples

        self.emit({"event": "phase_start", "kind": kind, "phase": phase})
        results = list(self.executor.map(worker, range(self.concurrency)))
        elapsed = time.perf_counter() - clock["start_monotonic"]
        samples = [sample for worker_samples in results for sample in worker_samples]
        errors = sum("error" in sample for sample in samples)
        result = {
            "event": "phase_end",
            "kind": kind,
            "phase": phase,
            "concurrency": self.concurrency,
            "slots": slots,
            "start_unix": clock["start_unix"],
            "end_unix": time.time(),
            "elapsed_seconds": elapsed,
            "requested_duration_seconds": duration,
            "successful_statements": len(samples) - errors,
            "errors": errors,
            "statements_per_second": (len(samples) - errors) / elapsed,
            "samples": samples,
        }
        self.emit(result)
        if errors:
            raise RuntimeError(
                f"DDL phase failed with {errors} errors, see raw samples"
            )
        return result


def responsiveness_duration(value: str | float) -> float:
    """Zero disables responsiveness measurements. Otherwise require finite seconds."""
    duration = float(value)
    if not math.isfinite(duration) or duration < 0:
        raise ValueError("responsiveness duration must be finite and nonnegative")
    return duration


def responsiveness_conditions(concurrencies: list[int]) -> list[tuple[str, int]]:
    return [("probes_only", 0)] + [
        (condition, concurrency)
        for concurrency in concurrencies
        if concurrency in (1, 8)
        for condition in ("view_load", "view_load_replan")
    ]


def fixed_rate_probe(
    execute: Callable[[], None],
    emit: Callable[[dict], None],
    *,
    start: float,
    duration: float,
    hz: int,
    clock: Callable[[], float] = time.perf_counter,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Offer fixed slots with one outstanding request and no catch-up queue."""
    busy_until = start
    errors = 0
    slot = 0
    while slot / hz < duration:
        scheduled = start + slot / hz
        sample: dict = {"slot": slot, "scheduled_offset_seconds": slot / hz}
        now = clock()
        if scheduled < busy_until or now >= scheduled + 1 / hz:
            sample.update(status="missed", observed_offset_seconds=now - start)
        else:
            sleep(max(0, scheduled - now))
            dispatched = clock()
            if dispatched >= min(scheduled + 1 / hz, start + duration):
                sample.update(
                    status="missed", observed_offset_seconds=dispatched - start
                )
            else:
                sample.update(status="ok", dispatch_offset_seconds=dispatched - start)
                try:
                    execute()
                except Exception as error:
                    errors += 1
                    sample.update(
                        status="error",
                        error=type(error).__name__,
                        sqlstate=getattr(error, "sqlstate", None),
                    )
                completed = clock()
                busy_until = completed
                sample.update(
                    completion_offset_seconds=completed - start,
                    service_latency_seconds=completed - dispatched,
                    scheduled_latency_seconds=completed - scheduled,
                )
        emit(sample)
        slot += 1
    if errors:
        raise RuntimeError(
            f"responsiveness probe failed with {errors} errors, see raw samples"
        )


def responsiveness_fixture_statements(schema: str, cluster: str) -> list:
    table = sql.Identifier(schema, "read_table")
    return [
        sql.SQL("CREATE TABLE {} (a int)").format(table),
        sql.SQL("INSERT INTO {} SELECT generate_series(1, 1024)").format(table),
        sql.SQL("CREATE DEFAULT INDEX {} IN CLUSTER {} ON {}").format(
            sql.Identifier("read_primary_idx"), sql.Identifier(cluster), table
        ),
        sql.SQL("CREATE VIEW {} AS SELECT a FROM {}").format(
            sql.Identifier(schema, "replan_view"), table
        ),
    ]


def run_responsiveness(
    new_connection: Callable[[], psycopg.Connection],
    schema: str,
    cluster: str,
    duration: float,
    condition: str,
    concurrency: int,
    emit: Callable[[dict], None],
) -> None:
    """Run an isolated condition, draining all requests before closing connections.

    Caller owns schema cleanup, including indeterminate DDL outcomes. All events
    use a distinct namespace so DDL profile and summary consumers ignore them.
    """
    if responsiveness_duration(duration) == 0:
        raise ValueError("responsiveness condition requires a positive duration")
    if (condition, concurrency) not in responsiveness_conditions([1, 8]):
        raise ValueError("invalid responsiveness condition")
    lock = Lock()

    def record(event: dict) -> None:
        with lock:
            emit({**event, "condition": condition, "concurrency": concurrency})

    def ddl_event(event: dict) -> None:
        record({**event, "event": "responsiveness_ddl_" + event["event"]})

    table = sql.Identifier(schema, "read_table")
    # peek_sequencing::try_peek only admits SHOW objects/columns, and the
    # frontend read-then-write path only admits DML. SHOW CREATE reaches Execute.
    show = sql.SQL("SHOW CREATE TABLE {}").format(table)
    select = sql.SQL("SELECT count(*), sum(a) FROM {}").format(table)
    # explain_replan_view awaits settle_view_commit on the coordinator. This
    # condition measures that joining limitation, not general nonblocking DDL.
    replan = sql.SQL("EXPLAIN OPTIMIZED PLAN FOR REPLAN VIEW {}").format(
        sql.Identifier(schema, "replan_view")
    )
    with ExitStack() as resources:
        probes = []
        for name, query, hz in [
            ("show_create", show, 20),
            ("indexed_select", select, 20),
        ] + ([("replan", replan, 5)] if condition == "view_load_replan" else []):
            conn = new_connection()
            resources.callback(conn.close)
            conn.autocommit = True
            conn.prepare_threshold = None
            conn.execute(sql.SQL("SET cluster = {}").format(sql.Identifier(cluster)))
            conn.execute("SET statement_timeout = '10s'")
            expected = conn.execute(query).fetchall()
            if name == "indexed_select":
                if expected != [(1024, 524800)]:
                    raise RuntimeError(f"incorrect read fixture result: {expected}")
                plan = conn.execute(
                    sql.SQL("EXPLAIN PHYSICAL PLAN ") + query
                ).fetchall()
                if "read_primary_idx" not in str(plan):
                    raise RuntimeError(f"read probe did not use fixture index: {plan}")
            elif name == "show_create":
                if (
                    len(expected) != 1
                    or len(expected[0]) != 2
                    or "CREATE TABLE" not in expected[0][1]
                ):
                    raise RuntimeError(f"incorrect SHOW CREATE result: {expected}")
            elif not expected or not all(row and row[0] for row in expected):
                raise RuntimeError("empty replan result")

            def execute(conn=conn, query=query, expected=expected):
                if conn.execute(query).fetchall() != expected:
                    raise RuntimeError("responsiveness result changed")

            probes.append((name, execute, hz))
        driver = None
        if concurrency:
            driver = DdlMeasurement(
                new_connection, concurrency, schema, cluster, ddl_event
            )
            resources.callback(driver.close)
        clock: dict = {}

        def start() -> None:
            clock.update(start_unix=time.time(), start_monotonic=time.perf_counter())
            record(
                {
                    "event": "responsiveness_start",
                    **clock,
                    "requested_duration_seconds": duration,
                    "sample_clock": "client perf_counter, offsets from start_monotonic",
                    "ddl_sample_clock": "client perf_counter, wall anchor DDL start_unix",
                    "autocommit": True,
                    "prepare_threshold": None,
                    "probe_statement_timeout_seconds": 10,
                    "ddl_statement_timeout_seconds": 120,
                    "probe_rates_hz": {name: hz for name, _, hz in probes},
                }
            )

        barrier = Barrier(len(probes) + bool(driver), action=start)

        def probe(name, execute, hz):
            barrier.wait()
            fixed_rate_probe(
                execute,
                lambda sample: record(
                    {"event": "responsiveness_sample", "probe": name, **sample}
                ),
                start=clock["start_monotonic"],
                duration=duration,
                hz=hz,
            )

        def ddl():
            barrier.wait()
            assert driver is not None
            driver.run("view", "cycle", duration=duration)

        failed = True
        try:
            with ThreadPoolExecutor(max_workers=barrier.parties) as executor:
                futures = [executor.submit(probe, *p) for p in probes]
                if driver:
                    futures.append(executor.submit(ddl))
                for future in futures:
                    future.result()
            failed = False
        finally:
            record(
                {
                    "event": "responsiveness_end",
                    **clock,
                    "end_unix": time.time(),
                    "end_monotonic": time.perf_counter(),
                    "failed": failed,
                }
            )
