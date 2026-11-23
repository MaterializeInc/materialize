# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""DDL measurements on persistent connections, without retries or implicit transactions."""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier
from typing import Callable

import psycopg
from psycopg import sql


def ddl_statements(kind: str, schema: str, cluster: str, name: str) -> tuple:
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
            sql.Identifier(name), sql.Identifier(cluster), sql.Identifier(schema, "base")
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
        """
        if phase not in ("create", "drop", "cycle") or slots < 1:
            raise ValueError("invalid DDL phase")
        if phase == "cycle" and duration <= 0:
            raise ValueError("cycle duration must be positive")
        queries = [
            [
                ddl_statements(kind, self.schema, self.cluster, f"probe_{w}_{s}")
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
            raise RuntimeError(f"DDL phase failed with {errors} errors, see raw samples")
        return result
