# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Initial data creation functions for workload replay.
"""

from __future__ import annotations

import asyncio
import os
import random
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import Any

import psycopg
from psycopg.sql import SQL, Identifier

from materialize.mzcompose.composition import Composition
from materialize.util import PropagatingThread
from materialize.workload_replay.column import Column
from materialize.workload_replay.config import SEED_RANGE
from materialize.workload_replay.ingest import (
    delivery_report,
    get_kafka_objects,
    ingest,
    ingest_webhook,
)
from materialize.workload_replay.util import (
    get_kafka_topic,
    get_mysql_reference_db_table,
    get_postgres_reference_db_schema_table,
)

_NUM_WORKERS = min(os.cpu_count() or 4, 16)
_CHUNK_ROWS = 100_000


# Subprocess workers (run in forked children, must use only picklable args)
def _copy_chunk(
    conn_params: dict[str, Any],
    table_fqn: list[str],
    column_dicts: list[dict[str, Any]],
    num_rows: int,
    rng_seed: int,
) -> int:
    """Generate random data and COPY to Postgres or Materialize."""
    rng = random.Random(rng_seed)
    columns = [
        Column(c["name"], c["type"], c["nullable"], c["default"], c.get("data_shape"))
        for c in column_dicts
    ]
    col_names = [c.name for c in columns]

    conn = psycopg.connect(**conn_params)
    conn.autocommit = True

    table_ident = SQL(".").join(map(Identifier, table_fqn))
    copy_stmt = SQL("COPY {} ({}) FROM STDIN").format(
        table_ident,
        SQL(", ").join(map(Identifier, col_names)),
    )

    batch_size = 10000
    with conn.cursor() as cur:
        for start in range(0, num_rows, batch_size):
            batch_rows = min(batch_size, num_rows - start)
            with cur.copy(copy_stmt) as copy:
                for _ in range(batch_rows):
                    row = [c.value(rng, in_query=False) for c in columns]
                    copy.write_row(row)

    conn.close()
    return num_rows


def _kafka_chunk(
    kafka_port: int,
    sr_port: int,
    topic: str,
    debezium: bool,
    column_dicts: list[dict[str, Any]],
    num_rows: int,
    rng_seed: int,
) -> int:
    """Generate random data and produce to Kafka."""
    rng = random.Random(rng_seed)
    columns = [
        Column(c["name"], c["type"], c["nullable"], c["default"], c.get("data_shape"))
        for c in column_dicts
    ]

    producer, serializer, key_serializer, sctx, ksctx, col_names = get_kafka_objects(
        topic,
        tuple(columns),
        debezium,
        sr_port,
        kafka_port,
    )

    now_ms = int(time.time() * 1000)
    source_struct: dict[str, Any] | None = None
    if debezium:
        source_struct = {
            "version": "0",
            "connector": "mysql",
            "name": "materialize-generator",
            "ts_ms": now_ms,
            "snapshot": None,
            "db": "db",
            "sequence": None,
            "table": topic.split(".")[-1],
            "server_id": 0,
            "gtid": None,
            "file": "binlog.000001",
            "pos": 0,
            "row": 0,
            "thread": None,
            "query": None,
        }

    producer.poll(0)
    for _ in range(num_rows):
        row = [col.kafka_value(rng) for col in columns]
        while True:
            try:
                if debezium:
                    after_value = dict(zip(col_names, row))
                    envelope_value = {
                        "before": None,
                        "after": after_value,
                        "source": source_struct,
                        "op": "c",
                        "ts_ms": now_ms,
                        "transaction": None,
                    }
                    producer.produce(
                        topic=topic,
                        key=key_serializer(after_value, ksctx),
                        value=serializer(envelope_value, sctx),
                        on_delivery=delivery_report,
                    )
                else:
                    key_dict = {col_names[0]: row[0]}
                    value_dict = dict(zip(col_names[1:], row[1:]))
                    producer.produce(
                        topic=topic,
                        key=key_serializer(key_dict, ksctx),
                        value=serializer(value_dict, sctx),
                        on_delivery=delivery_report,
                    )
                break
            except BufferError:
                producer.poll(0.01)
        producer.poll(0)

    producer.flush()
    return num_rows


def _mysql_chunk(
    conn_params: dict[str, Any],
    table: str,
    column_dicts: list[dict[str, Any]],
    num_rows: int,
    rng_seed: int,
) -> int:
    """Generate random data and INSERT into MySQL."""
    import pymysql

    rng = random.Random(rng_seed)
    columns = [
        Column(c["name"], c["type"], c["nullable"], c["default"], c.get("data_shape"))
        for c in column_dicts
    ]

    conn = pymysql.connect(**conn_params)

    batch_size = 10000
    for start in range(0, num_rows, batch_size):
        batch_rows = min(batch_size, num_rows - start)
        rows_sql = []
        for _ in range(batch_rows):
            row = [col.value(rng) for col in columns]
            rows_sql.append("(" + ", ".join(row) + ")")
        stmt = f"INSERT INTO {table} VALUES " + ", ".join(rows_sql)
        with conn.cursor() as cur:
            cur.execute(stmt)

    conn.close()
    return num_rows


def _submit_chunks(
    pool: ProcessPoolExecutor,
    worker_fn: Any,
    args: tuple[Any, ...],
    column_dicts: list[dict[str, Any]],
    num_rows: int,
    pretty_name: str,
    rng: random.Random,
    futures: dict[Future[int], str],
    totals: dict[str, int],
) -> None:
    """Split *num_rows* into _CHUNK_ROWS-sized pieces and submit them."""
    totals[pretty_name] = totals.get(pretty_name, 0) + num_rows
    remaining = num_rows
    while remaining > 0:
        n = min(_CHUNK_ROWS, remaining)
        seed = rng.randrange(SEED_RANGE)
        f = pool.submit(worker_fn, *args, column_dicts, n, seed)
        futures[f] = pretty_name
        remaining -= n


def _await_futures(
    futures: dict[Future[int], str],
    totals: dict[str, int],
) -> None:
    completed: dict[str, int] = {}
    last_pct: dict[str, int] = {}
    for future in as_completed(futures):
        name = futures[future]
        completed[name] = completed.get(name, 0) + future.result()
        done, total = completed[name], totals[name]
        bucket = int(done * 100 / total) // 5
        if bucket > last_pct.get(name, -1):
            last_pct[name] = bucket
            print(f"  {name}: {done:,}/{total:,} ({done / total:.1%})")


def create_initial_data_requiring_mz(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    """Create initial data that requires Materialize to be running (tables, webhooks)."""
    mz_conn = {
        "host": "127.0.0.1",
        "port": c.port("materialized", 6877),
        "user": "mz_system",
        "password": "materialize",
        "dbname": "materialize",
    }

    futures: dict[Future[int], str] = {}
    totals: dict[str, int] = {}
    webhook_items: list[tuple[str, str, str, dict[str, Any], int]] = []

    with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
        for db, schemas in workload["databases"].items():
            for schema, items in schemas.items():
                for name, table in items["tables"].items():
                    num_rows = int(table["rows"] * factor_initial_data)
                    if not num_rows:
                        continue
                    _submit_chunks(
                        pool,
                        _copy_chunk,
                        (mz_conn, [db, schema, name]),
                        table["columns"],
                        num_rows,
                        f"{db}.{schema}.{name}",
                        rng,
                        futures,
                        totals,
                    )
                for name, source in items["sources"].items():
                    if source["type"] == "webhook":
                        num_rows = int(source["messages_total"] * factor_initial_data)
                        if num_rows:
                            webhook_items.append((db, schema, name, source, num_rows))

        if not futures and not webhook_items:
            return False

        if futures:
            print(f"Creating {sum(totals.values()):,} rows across {len(totals)} tables")
            _await_futures(futures, totals)

    for db, schema, name, source, num_rows in webhook_items:
        print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")
        asyncio.run(
            ingest_webhook(c, db, schema, name, source, num_rows, print_progress=True)
        )

    return True


def create_initial_data_external(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    """Create initial data in external systems (Postgres, MySQL, Kafka, SQL Server)."""
    batch_size = 10000

    futures: dict[Future[int], str] = {}
    totals: dict[str, int] = {}
    # SQL Server uses c.testdrive() which isn't picklable, so stays sequential.
    sequential: list[
        tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], int, str]
    ] = []

    # Lazily resolved ports — only looked up when a source of that type is seen.
    _ports: dict[str, int] = {}

    def port(service: str) -> int:
        if service not in _ports:
            _ports[service] = c.default_port(service)
        return _ports[service]

    with ProcessPoolExecutor(max_workers=_NUM_WORKERS) as pool:
        for db, schemas in workload["databases"].items():
            for schema, items in schemas.items():
                for name, source in items["sources"].items():
                    children: list[tuple[dict[str, Any], str]] = []
                    if source["type"] != "webhook" and not source.get("children", {}):
                        children.append((source, f"{db}.{schema}.{name}"))
                    else:
                        for cn, child in source.get("children", {}).items():
                            children.append((child, f"{db}.{schema}.{name}->{cn}"))

                    for child, pretty_name in children:
                        num_rows = int(
                            (child.get("messages_total", child.get("rows", 0)))
                            * factor_initial_data
                        )
                        if not num_rows:
                            continue

                        st = source["type"]
                        if st == "postgres":
                            ref_db, ref_s, ref_t = (
                                get_postgres_reference_db_schema_table(child)
                            )
                            conn = {
                                "host": "127.0.0.1",
                                "port": port("postgres"),
                                "user": "postgres",
                                "password": "postgres",
                                "dbname": ref_db,
                            }
                            _submit_chunks(
                                pool,
                                _copy_chunk,
                                (conn, [ref_s, ref_t]),
                                child["columns"],
                                num_rows,
                                pretty_name,
                                rng,
                                futures,
                                totals,
                            )
                        elif st == "kafka":
                            topic = get_kafka_topic(source)
                            debezium = "ENVELOPE DEBEZIUM" in child["create_sql"]
                            _submit_chunks(
                                pool,
                                _kafka_chunk,
                                (
                                    port("kafka"),
                                    port("schema-registry"),
                                    topic,
                                    debezium,
                                ),
                                child["columns"],
                                num_rows,
                                pretty_name,
                                rng,
                                futures,
                                totals,
                            )
                        elif st == "mysql":
                            from materialize.mzcompose.services.mysql import MySql

                            ref_database, ref_table = get_mysql_reference_db_table(
                                child
                            )
                            conn = {
                                "host": "127.0.0.1",
                                "user": "root",
                                "password": MySql.DEFAULT_ROOT_PASSWORD,
                                "database": ref_database,
                                "port": port("mysql"),
                                "autocommit": False,
                            }
                            _submit_chunks(
                                pool,
                                _mysql_chunk,
                                (conn, ref_table),
                                child["columns"],
                                num_rows,
                                pretty_name,
                                rng,
                                futures,
                                totals,
                            )
                        elif st == "load-generator":
                            pass
                        else:
                            # sql-server etc. — sequential fallback
                            sequential.append(
                                (child, source, child["columns"], num_rows, pretty_name)
                            )

        if not futures and not sequential:
            return False

        if futures:
            print(
                f"Creating {sum(totals.values()):,} rows across {len(totals)} sources"
            )
            _await_futures(futures, totals)

    for child, source, cols, num_rows, pretty_name in sequential:
        data_columns = [
            Column(
                col["name"],
                col["type"],
                col["nullable"],
                col["default"],
                col.get("data_shape"),
            )
            for col in cols
        ]
        print(f"Creating {num_rows} rows for {pretty_name}:")
        for start in range(0, num_rows, batch_size):
            progress = min(start + batch_size, num_rows)
            print(
                f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                end="\r",
                flush=True,
            )
            ingest(
                c, child, source, data_columns, min(batch_size, num_rows - start), rng
            )
        print()

    return True


def create_ingestions(
    c: Composition,
    workload: dict[str, Any],
    stop_event: threading.Event,
    factor_ingestions: float,
    verbose: bool,
    stats: dict[str, int],
) -> list[threading.Thread]:
    """Create threads for continuous data ingestion during the test."""
    threads = []
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, source in items["sources"].items():
                if source["type"] == "webhook":
                    if "messages_second" not in source:
                        continue
                    target_rps = source["messages_second"] * factor_ingestions
                    if target_rps <= 0:
                        continue

                    batch_size = 1
                    period_s = min(batch_size / target_rps, 60)

                    pretty_name = f"{db}.{schema}.{name}"
                    print(
                        f"{target_rps:.3f} ing./s for {pretty_name} ({batch_size} every {period_s:.2f}s)"
                    )

                    def continuous_ingestion_webhook(
                        db: str,
                        schema: str,
                        name: str,
                        source: dict[str, Any],
                        pretty_name: str,
                        batch_size: int,
                        period_s: float,
                        rng: random.Random,
                    ) -> None:
                        import time

                        try:
                            next_time = time.time()

                            while not stop_event.is_set():
                                now = time.time()
                                if now < next_time:
                                    stop_event.wait(timeout=next_time - now)
                                    continue

                                next_time += period_s

                                if verbose:
                                    print(
                                        f"Ingesting {batch_size} rows for {pretty_name}"
                                    )

                                stats["total"] += 1
                                asyncio.run(
                                    ingest_webhook(
                                        c,
                                        db,
                                        schema,
                                        name,
                                        source,
                                        batch_size,
                                    )
                                )

                                after = time.time()
                                if after > next_time:
                                    stats["slow"] += 1
                                    next_time = after
                                    if verbose:
                                        print(f"Can't keep up: {pretty_name}")

                        except Exception as e:
                            stats["failed"] += 1
                            print(f"Failed: {pretty_name}")
                            print(e)
                            stop_event.set()
                            raise

                elif not source.get("children", {}):
                    if "messages_second" not in source:
                        continue
                    target_rps = source["messages_second"] * factor_ingestions
                    if target_rps <= 0:
                        continue

                    batch_size = 1
                    period_s = min(batch_size / target_rps, 60)

                    data_columns = [
                        Column(
                            col["name"],
                            col["type"],
                            col["nullable"],
                            col["default"],
                            col.get("data_shape"),
                        )
                        for col in source["columns"]
                    ]

                    pretty_name = f"{db}.{schema}.{name}"
                    print(
                        f"{target_rps:.3f} ing./s for {pretty_name} "
                        f"({batch_size} every {period_s:.2f}s)"
                    )

                    def continuous_ingestion_source(
                        source: dict[str, Any],
                        pretty_name: str,
                        data_columns: list[Column],
                        batch_size: int,
                        period_s: float,
                        rng: random.Random,
                    ) -> None:
                        import time

                        try:
                            next_time = time.time()

                            while not stop_event.is_set():
                                now = time.time()
                                if now < next_time:
                                    stop_event.wait(timeout=next_time - now)
                                    continue

                                next_time += period_s

                                if verbose:
                                    print(
                                        f"Ingesting {batch_size} rows for {pretty_name}"
                                    )

                                stats["total"] += 1
                                ingest(c, source, source, data_columns, batch_size, rng)

                                after = time.time()
                                if after > next_time:
                                    stats["slow"] += 1
                                    next_time = after
                                    if verbose:
                                        print(f"Can't keep up: {pretty_name}")

                        except Exception as e:
                            stats["failed"] += 1
                            print(f"Failed: {pretty_name}")
                            print(e)
                            stop_event.set()
                            raise

                    threads.append(
                        PropagatingThread(
                            target=continuous_ingestion_source,
                            name=f"ingest-{pretty_name}",
                            args=(
                                source,
                                pretty_name,
                                data_columns,
                                batch_size,
                                period_s,
                                random.Random(random.randrange(SEED_RANGE)),
                            ),
                        )
                    )
                else:
                    for child_name, child in source.get("children", {}).items():
                        if "messages_second" not in child:
                            continue
                        target_rps = child["messages_second"] * factor_ingestions
                        if target_rps <= 0:
                            continue

                        batch_size = 1
                        period_s = min(batch_size / target_rps, 60)

                        data_columns = [
                            Column(
                                col["name"],
                                col["type"],
                                col["nullable"],
                                col["default"],
                                col.get("data_shape"),
                            )
                            for col in child["columns"]
                        ]

                        pretty_name = f"{db}.{schema}.{name}->{child_name}"
                        print(
                            f"{target_rps:.3f} ing./s for {pretty_name} "
                            f"({batch_size} every {period_s:.2f}s)"
                        )

                        def continuous_ingestion_child(
                            source: dict[str, Any],
                            child: dict[str, Any],
                            pretty_name: str,
                            data_columns: list[Column],
                            batch_size: int,
                            period_s: float,
                            rng: random.Random,
                        ) -> None:
                            import time

                            try:
                                next_time = time.time()

                                while not stop_event.is_set():
                                    now = time.time()
                                    if now < next_time:
                                        stop_event.wait(timeout=next_time - now)
                                        continue

                                    next_time += period_s

                                    if verbose:
                                        print(
                                            f"Ingesting {batch_size} rows for {pretty_name}"
                                        )

                                    stats["total"] += 1
                                    ingest(
                                        c, child, source, data_columns, batch_size, rng
                                    )

                                    after = time.time()
                                    if after > next_time:
                                        stats["slow"] += 1
                                        next_time = after
                                        if verbose:
                                            print(f"Can't keep up: {pretty_name}")

                            except Exception as e:
                                stats["failed"] += 1
                                print(f"Failed: {pretty_name}")
                                print(e)
                                stop_event.set()
                                raise

                        threads.append(
                            PropagatingThread(
                                target=continuous_ingestion_child,
                                name=f"ingest-{pretty_name}",
                                args=(
                                    source,
                                    child,
                                    pretty_name,
                                    data_columns,
                                    batch_size,
                                    period_s,
                                    random.Random(random.randrange(SEED_RANGE)),
                                ),
                            )
                        )

    return threads
