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
import random
import threading
from typing import Any

import psycopg
from psycopg.sql import SQL, Identifier

from materialize.mzcompose.composition import Composition
from materialize.util import PropagatingThread
from materialize.workload_replay.column import Column
from materialize.workload_replay.config import SEED_RANGE
from materialize.workload_replay.ingest import ingest, ingest_webhook


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
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})",
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
                            f"{progress}/{num_rows} ({progress / num_rows:.1%})",
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
                                f"{progress}/{num_rows} ({progress / num_rows:.1%})",
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
