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
    _open_ingest_conn,
    _resolve_child_obj,
    delivery_report,
    get_kafka_objects,
    get_parquet_row_count,
    ingest,
    ingest_captured_parquet_kafka,
    ingest_captured_rows,
    ingest_webhook,
    iter_parquet_batches,
    parse_parquet_file,
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
                        nonlocal stop_event
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
                        nonlocal stop_event
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
                            nonlocal stop_event
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


# --- Captured data orchestration ---


def _list_parquet_fqns(data_dir: str) -> list[tuple[str, str]]:
    """List all (fqn, path) pairs for non-empty Parquet files in a directory."""
    results = []
    for name in sorted(os.listdir(data_dir)):
        if name.endswith(".parquet"):
            path = os.path.join(data_dir, name)
            if os.path.getsize(path) > 0:
                fqn = name[: -len(".parquet")]
                results.append((fqn, path))
    return results


def _lookup_fqn(
    fqn: str, workload: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Look up an FQN in the workload and return (meta, source_obj).

    meta always has database, schema, name, columns.
    source_obj is the parent source object (for routing), or None for tables.
    """
    db, schema, name = fqn.split(".", 2)
    meta: dict[str, Any] = {"database": db, "schema": schema, "name": name}

    items = workload.get("databases", {}).get(db, {}).get(schema, {})

    # Check tables first — no parent source, data goes directly into MZ.
    obj = items.get("tables", {}).get(name)
    if obj:
        meta["columns"] = obj.get("columns", [])
        return meta, None

    # Check source children (subsources / tables-from-source).
    for source in items.get("sources", {}).values():
        children = source.get("children", {})
        if fqn in children:
            meta["columns"] = children[fqn].get("columns", [])
            return meta, source
        for child in children.values():
            if child.get("name") == name:
                meta["columns"] = child.get("columns", [])
                return meta, source

    # Source children are keyed by fully-qualified name and can live under a
    # source in a different schema than the child's schema.
    for other_schema_items in workload.get("databases", {}).get(db, {}).values():
        for source in other_schema_items.get("sources", {}).values():
            children = source.get("children", {})
            if fqn in children:
                meta["columns"] = children[fqn].get("columns", [])
                return meta, source

    # Check standalone sources (no children).
    obj = items.get("sources", {}).get(name)
    if obj:
        meta["columns"] = obj.get("columns", [])
        return meta, obj

    meta["columns"] = []
    return meta, None


def _sample_rows(
    rows: list[list[str | None]], factor: float, rng: random.Random
) -> list[list[str | None]]:
    """Return a random sample of rows based on the given factor (0..1]."""
    if factor >= 1.0:
        return rows
    n = max(1, int(len(rows) * factor))
    return rng.sample(rows, n)


def _captured_object_requires_mz(source_obj: dict[str, Any] | None) -> bool:
    """Whether importing this captured object requires the MZ object to exist."""
    if source_obj is None:
        # Standalone tables are imported via COPY into Materialize.
        return True
    return source_obj.get("type") == "webhook"


def _import_non_kafka_object(
    c: Composition,
    workload: dict[str, Any],
    fqn: str,
    data_path: str,
    meta: dict[str, Any],
    source_obj: dict[str, Any] | None,
    factor: float,
    seed: str | int,
) -> None:
    """Import a single non-Kafka object's Parquet data (thread target)."""
    rng = random.Random(seed)
    conn = _open_ingest_conn(c, meta, source_obj)
    try:
        col_types = [col["type"] for col in meta.get("columns", [])]
        for batch in iter_parquet_batches(data_path, column_types=col_types):
            batch = _sample_rows(batch, factor, rng)
            ingest_captured_rows(c, workload, meta, source_obj, batch, conn=conn)
    finally:
        if conn is not None:
            conn.close()
    print(f"  Done: {fqn}")


def import_captured_data_initial(
    c: Composition,
    workload: dict[str, Any],
    data_dir: str,
    factor: float = 1.0,
    seed: str | int = 0,
    requires_mz: bool | None = None,
) -> bool:
    """Import initial captured data (Parquet) into upstream systems or MZ tables.

    Kafka objects are processed serially (already parallel internally).
    Non-Kafka objects are imported in parallel (max 4 concurrent threads).
    """
    parquet_files = _list_parquet_fqns(data_dir)
    if not parquet_files:
        return False

    imported = False
    non_kafka_work: list[tuple[str, str, dict[str, Any], dict[str, Any] | None]] = []

    for fqn, data_path in parquet_files:
        meta, source_obj = _lookup_fqn(fqn, workload)
        if (
            requires_mz is not None
            and _captured_object_requires_mz(source_obj) != requires_mz
        ):
            continue
        total_rows = get_parquet_row_count(data_path)
        if total_rows == 0:
            continue

        if factor < 1.0:
            target_rows = int(total_rows * factor)
            if target_rows == 0:
                continue
            print(
                f"  Importing {fqn} ({target_rows}/{total_rows} rows, factor={factor})"
            )
        else:
            print(f"  Importing {fqn} ({total_rows} rows)")

        source_type = source_obj.get("type") if source_obj else None
        if source_type == "kafka":
            assert source_obj is not None
            child_obj = _resolve_child_obj(meta, source_obj)
            ingest_captured_parquet_kafka(
                c,
                meta,
                source_obj,
                child_obj,
                data_path,
                factor=factor,
                seed=int(seed),
            )
            imported = True
        else:
            non_kafka_work.append((fqn, data_path, meta, source_obj))

    if non_kafka_work:
        max_concurrent = 4
        active_threads: list[PropagatingThread] = []

        for fqn, data_path, meta, source_obj in non_kafka_work:
            while len(active_threads) >= max_concurrent:
                for t in active_threads:
                    t.join(timeout=0.1)
                active_threads = [t for t in active_threads if t.is_alive()]

            t = PropagatingThread(
                target=_import_non_kafka_object,
                name=f"import-{fqn}",
                args=(c, workload, fqn, data_path, meta, source_obj, factor, seed),
            )
            t.start()
            active_threads.append(t)

        for t in active_threads:
            t.join()

        imported = True

    return imported


def _replay_continuous_object(
    c: Composition,
    workload: dict[str, Any],
    fqn: str,
    data_path: str,
    speed: float,
    factor: float,
    seed: str | int,
    stop_event: threading.Event,
) -> None:
    """Replay continuous captured data for a single object at original cadence."""
    meta, source_obj = _lookup_fqn(fqn, workload)
    col_types = [col["type"] for col in meta.get("columns", [])]

    rows_by_timestamp = parse_parquet_file(
        data_path, column_types=col_types, group_by_timestamp=True
    )
    timestamps = sorted(rows_by_timestamp.keys())
    if not timestamps:
        return

    rng = random.Random(seed)
    total = sum(len(v) for v in rows_by_timestamp.values())
    duration_s = (timestamps[-1] - timestamps[0]) / 1000.0 / speed
    if factor < 1.0:
        target = int(total * factor)
        print(
            f"  Replaying {target}/{total} rows for {fqn} across {len(timestamps)} timestamps"
            f" (factor={factor}, {duration_s:.0f}s at {speed}x speed)"
        )
    else:
        print(
            f"  Replaying {total} rows for {fqn} across {len(timestamps)} timestamps"
            f" ({duration_s:.0f}s at {speed}x speed)"
        )

    first_ts = timestamps[0]
    replay_start = time.time()

    conn = _open_ingest_conn(c, meta, source_obj)
    try:
        for ts in timestamps:
            if stop_event.is_set():
                break

            target_time = replay_start + (ts - first_ts) / 1000.0 / speed
            now = time.time()
            if target_time > now:
                stop_event.wait(timeout=target_time - now)
                if stop_event.is_set():
                    break

            rows = rows_by_timestamp[ts]
            if factor < 1.0:
                rows = _sample_rows(rows, factor, rng)

            try:
                ingest_captured_rows(c, workload, meta, source_obj, rows, conn=conn)
            except Exception as e:
                print(f"  Error replaying {fqn} at ts {ts}: {e}")
                break
    finally:
        if conn is not None:
            conn.close()


def import_captured_data_streaming(
    c: Composition,
    workload: dict[str, Any],
    data_dir: str,
    speed: float,
    factor: float,
    seed: str | int,
    stop_event: threading.Event,
) -> list[threading.Thread]:
    """Start threads to replay continuous captured data at original cadence."""
    parquet_files = _list_parquet_fqns(data_dir)
    threads: list[threading.Thread] = []

    for fqn, data_path in parquet_files:
        t = PropagatingThread(
            target=_replay_continuous_object,
            name=f"replay-{fqn}",
            args=(c, workload, fqn, data_path, speed, factor, seed, stop_event),
        )
        threads.append(t)

    return threads
