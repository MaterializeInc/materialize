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
from typing import Any

import psycopg
from psycopg.sql import SQL, Identifier

from materialize.mzcompose.composition import Composition
from materialize.util import PropagatingThread
from materialize.workload_replay.column import Column
from materialize.workload_replay.config import SEED_RANGE
from materialize.workload_replay.ingest import (
    get_parquet_row_count,
    ingest,
    ingest_captured_parquet_kafka,
    ingest_captured_rows,
    ingest_webhook,
    iter_parquet_batches,
    parse_parquet_file,
)


def create_initial_data_requiring_mz(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    """Create initial data that requires Materialize to be running (tables, webhooks)."""
    batch_size = 10000
    created_data = False

    conn = psycopg.connect(
        host="127.0.0.1",
        port=c.port("materialized", 6877),
        user="mz_system",
        password="materialize",
        dbname="materialize",
    )
    conn.autocommit = True

    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, table in items["tables"].items():
                num_rows = int(table["rows"] * factor_initial_data)
                if not num_rows:
                    continue

                data_columns = [
                    Column(
                        col["name"],
                        col["type"],
                        col["nullable"],
                        col["default"],
                        col.get("data_shape"),
                    )
                    for col in table["columns"]
                ]

                print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")

                col_names = [col.name for col in data_columns]

                with conn.cursor() as cur:
                    for start in range(0, num_rows, batch_size):
                        progress = min(start + batch_size, num_rows)
                        print(
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})\033[K",
                            end="\r",
                            flush=True,
                        )

                        copy_stmt = SQL("COPY {}.{}.{} ({}) FROM STDIN").format(
                            Identifier(db),
                            Identifier(schema),
                            Identifier(name),
                            SQL(", ").join(map(Identifier, col_names)),
                        )

                        with cur.copy(copy_stmt) as copy:
                            batch_rows = min(batch_size, num_rows - start)
                            for _ in range(batch_rows):
                                row = [
                                    col.value(rng, in_query=False)
                                    for col in data_columns
                                ]
                                copy.write_row(row)
                print()
                created_data = True

            for name, source in items["sources"].items():
                if source["type"] == "webhook":
                    num_rows = int(source["messages_total"] * factor_initial_data)
                    if not num_rows:
                        continue
                    print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")
                    asyncio.run(
                        ingest_webhook(
                            c,
                            db,
                            schema,
                            name,
                            source,
                            num_rows,
                            print_progress=True,
                        )
                    )
                    created_data = True
    conn.close()
    return created_data


def create_initial_data_external(
    c: Composition,
    workload: dict[str, Any],
    factor_initial_data: float,
    rng: random.Random,
) -> bool:
    """Create initial data in external systems (Postgres, MySQL, Kafka, SQL Server)."""
    batch_size = 10000
    created_data = False
    for db, schemas in workload["databases"].items():
        for schema, items in schemas.items():
            for name, source in items["sources"].items():
                if source["type"] != "webhook" and not source.get("children", {}):
                    num_rows = int(source["messages_total"] * factor_initial_data)
                    if not num_rows:
                        continue
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
                    print(f"Creating {num_rows} rows for {db}.{schema}.{name}:")
                    for start in range(0, num_rows, batch_size):
                        progress = min(start + batch_size, num_rows)
                        print(
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})\033[K",
                            end="\r",
                            flush=True,
                        )
                        ingest(
                            c,
                            source,
                            source,
                            data_columns,
                            min(batch_size, num_rows - start),
                            rng,
                        )
                    created_data = True
                    print()
                else:
                    for child_name, child in source.get("children", {}).items():
                        num_rows = int(child["messages_total"] * factor_initial_data)
                        if not num_rows:
                            continue
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
                        print(
                            f"Creating {num_rows} rows for {db}.{schema}.{name}->{child_name}:"
                        )
                        for start in range(0, num_rows, batch_size):
                            progress = min(start + batch_size, num_rows)
                            print(
                                f"{progress}/{num_rows} ({progress / num_rows:.1%})\033[K",
                                end="\r",
                                flush=True,
                            )
                            ingest(
                                c,
                                child,
                                source,
                                data_columns,
                                min(batch_size, num_rows - start),
                                rng,
                            )
                        created_data = True
                        print()
    return created_data


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
                                ingest(
                                    c,
                                    source,
                                    source,
                                    data_columns,
                                    batch_size,
                                    rng,
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
                                        c,
                                        child,
                                        source,
                                        data_columns,
                                        batch_size,
                                        rng,
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


# --- captured data import functions ---


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


def import_captured_data_initial(
    c: Composition,
    workload: dict[str, Any],
    data_dir: str,
    factor: float = 1.0,
    seed: str | int = 0,
) -> bool:
    """Import initial captured data (Parquet) into upstream systems or MZ tables."""
    parquet_files = _list_parquet_fqns(data_dir)
    if not parquet_files:
        return False

    rng = random.Random(seed)
    imported = False
    for fqn, data_path in parquet_files:
        meta, source_obj = _lookup_fqn(fqn, workload)
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

        # Kafka sources: use the optimized Parquet-to-Avro path.
        source_type = source_obj.get("type") if source_obj else None
        if source_type == "kafka":
            child_obj = source_obj
            children = source_obj.get("children", {})  # type: ignore[union-attr]
            if children and meta["name"] in children:
                child_obj = children[meta["name"]]
            ingest_captured_parquet_kafka(
                c, meta, source_obj, child_obj, data_path,  # type: ignore[arg-type]
                factor=factor, seed=hash(seed),
            )
        else:
            col_types = [col["type"] for col in meta.get("columns", [])]
            for batch in iter_parquet_batches(data_path, column_types=col_types):
                batch = _sample_rows(batch, factor, rng)
                ingest_captured_rows(c, workload, meta, source_obj, batch)
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

    # Parse continuous Parquet: extract mz_timestamp and mz_diff, group data by timestamp.
    rows_by_timestamp = parse_parquet_file(
        data_path, column_types=col_types, group_by_timestamp=True
    )
    timestamps = sorted(rows_by_timestamp.keys())
    if not timestamps:
        return

    rng = random.Random(seed)
    total = sum(len(v) for v in rows_by_timestamp.values())
    if factor < 1.0:
        target = int(total * factor)
        print(
            f"  Replaying {target}/{total} rows for {fqn} across {len(timestamps)} timestamps (factor={factor})"
        )
    else:
        print(f"  Replaying {total} rows for {fqn} across {len(timestamps)} timestamps")

    first_ts = timestamps[0]
    replay_start = time.time()

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
            ingest_captured_rows(c, workload, meta, source_obj, rows)
        except Exception as e:
            print(f"  Error replaying {fqn} at ts {ts}: {e}")
            break


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
