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
    _arrow_val_to_text,
    _decode_packed_timestamp,
    ingest,
    ingest_captured_rows_kafka,
    ingest_captured_rows_mysql,
    ingest_captured_rows_mz_table,
    ingest_captured_rows_postgres,
    ingest_captured_rows_sql_server,
    ingest_captured_rows_webhook,
    ingest_webhook,
    parse_parquet_file,
    parse_tsv_file,
    unescape_copy_field,
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


# --- SQL-captured data import functions ---


def _find_source_for_meta(
    workload: dict[str, Any], meta: dict[str, Any]
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Look up the source and child objects in the workload dict for a given meta.

    Returns (source_obj, child_obj). For standalone tables/webhooks, child_obj is None.
    """
    source_type = meta["source_type"]
    parent_fqn = meta.get("parent_source_fqn")

    if source_type == "table" and not parent_fqn:
        db = meta["database"]
        schema = meta["schema"]
        name = meta["name"]
        # Try tables first, then sources (for persist-captured data where
        # source_type is always "table" even for kafka/webhook sources).
        table = (
            workload.get("databases", {})
            .get(db, {})
            .get(schema, {})
            .get("tables", {})
            .get(name)
        )
        if table is not None:
            return (table, None)
        source_obj = (
            workload.get("databases", {})
            .get(db, {})
            .get(schema, {})
            .get("sources", {})
            .get(name)
        )
        return (source_obj, None) if source_obj else (None, None)

    if parent_fqn:
        parts = parent_fqn.split(".", 2)
        if len(parts) == 3:
            src_db, src_schema, src_name = parts
        else:
            return (None, None)

        source_obj = (
            workload.get("databases", {})
            .get(src_db, {})
            .get(src_schema, {})
            .get("sources", {})
            .get(src_name)
        )
        if source_obj is None:
            return (None, None)

        child_fqn = f"{meta['database']}.{meta['schema']}.{meta['name']}"
        child_obj = source_obj.get("children", {}).get(child_fqn)
        return (source_obj, child_obj)

    # Source without children (webhook, kafka without subsources)
    db = meta["database"]
    schema = meta["schema"]
    name = meta["name"]
    source_obj = (
        workload.get("databases", {})
        .get(db, {})
        .get(schema, {})
        .get("sources", {})
        .get(name)
    )
    return (source_obj, None) if source_obj else (None, None)


def _ingest_captured_rows(
    c: Composition,
    meta: dict[str, Any],
    source_obj: dict[str, Any] | None,
    child_obj: dict[str, Any] | None,
    rows: list[list[str | None]],
) -> None:
    """Route captured rows to the appropriate ingest function based on source_type.

    For persist-captured data where source_type is "table", we check if the
    object has a parent source (subsource of postgres/kafka/etc.) and route
    through the appropriate upstream ingest path.  Standalone tables are
    loaded directly via COPY.
    """
    source_type = meta["source_type"]

    # For source_type "table", check if this is actually a child of a source.
    if source_type == "table" and source_obj is not None:
        parent_type = source_obj.get("type")
        if parent_type == "postgres":
            ingest_captured_rows_postgres(c, meta, child_obj or source_obj, rows)
            return
        if parent_type == "mysql":
            ingest_captured_rows_mysql(c, meta, child_obj or source_obj, rows)
            return
        if parent_type == "sql-server":
            ingest_captured_rows_sql_server(c, meta, child_obj or source_obj, rows)
            return
        if parent_type == "kafka":
            ingest_captured_rows_kafka(
                c, meta, source_obj, child_obj or source_obj, rows
            )
            return
        if parent_type == "webhook":
            ingest_captured_rows_webhook(c, meta, rows)
            return

    if source_type == "table":
        ingest_captured_rows_mz_table(c, meta, rows)
    elif source_type == "postgres":
        assert child_obj is not None
        ingest_captured_rows_postgres(c, meta, child_obj, rows)
    elif source_type == "mysql":
        assert child_obj is not None
        ingest_captured_rows_mysql(c, meta, child_obj, rows)
    elif source_type == "kafka":
        assert source_obj is not None
        target = child_obj if child_obj is not None else source_obj
        ingest_captured_rows_kafka(c, meta, source_obj, target, rows)
    elif source_type == "sql-server":
        assert child_obj is not None
        ingest_captured_rows_sql_server(c, meta, child_obj, rows)
    elif source_type == "webhook":
        ingest_captured_rows_webhook(c, meta, rows)
    elif source_type == "load-generator":
        pass
    else:
        print(
            f"Warning: skipping unsupported source_type '{source_type}' for {meta['name']}"
        )


def import_sql_captured_data_initial(
    c: Composition,
    workload: dict[str, Any],
    data_dir: str,
) -> bool:
    """Import initial SQL-captured data from TSV files.

    Uses capture_meta from the workload to identify objects, loads corresponding
    .tsv files, and routes data to the appropriate external system or table.
    Returns True if any data was imported.
    """
    capture_meta = workload.get("capture_meta", {})
    if not capture_meta:
        return False

    imported = False
    for fqn, meta in sorted(capture_meta.items()):
        # Look for Parquet first, then TSV.
        parquet_path = os.path.join(data_dir, f"{fqn}.parquet")
        tsv_path = os.path.join(data_dir, f"{fqn}.tsv")

        if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
            data_path = parquet_path
            is_parquet = True
        elif os.path.exists(tsv_path) and os.path.getsize(tsv_path) > 0:
            data_path = tsv_path
            is_parquet = False
        else:
            continue

        source_obj, child_obj = _find_source_for_meta(workload, meta)
        if source_obj is None and meta["source_type"] != "table":
            print(f"  Warning: no matching object found for {fqn}, skipping")
            continue

        col_types = [c["type"] for c in meta.get("columns", [])] if is_parquet else None
        rows = (
            parse_parquet_file(data_path, column_types=col_types)
            if is_parquet
            else parse_tsv_file(data_path)
        )
        if not rows:
            continue

        print(f"  Importing {fqn} ({meta['source_type']}, {len(rows)} rows)")
        _ingest_captured_rows(c, meta, source_obj, child_obj, rows)
        imported = True

    return imported


def _parse_continuous_parquet(
    parquet_path: str,
    column_types: list[str] | None = None,
) -> dict[int, list[list[str | None]]]:
    """Parse a continuous-mode Parquet file (with mz_timestamp, mz_diff columns)."""
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    col_names = table.column_names
    if "mz_timestamp" not in col_names or "mz_diff" not in col_names:
        return {}

    ts_col = table.column("mz_timestamp").to_pylist()
    diff_col = table.column("mz_diff").to_pylist()
    # Data columns are everything except mz_timestamp and mz_diff.
    data_col_indices = [
        i for i, name in enumerate(col_names) if name not in ("mz_timestamp", "mz_diff")
    ]
    data_columns = [table.column(i).to_pylist() for i in data_col_indices]
    # Detect fixed_size_binary[16] columns (packed timestamps).
    ts_data_cols: set[int] = set()
    for pos, idx in enumerate(data_col_indices):
        dt = table.schema.field(idx).type
        if str(dt) == "fixed_size_binary[16]":
            ts_data_cols.add(pos)

    rows_by_timestamp: dict[int, list[list[str | None]]] = {}
    for row_idx in range(table.num_rows):
        mz_diff = diff_col[row_idx]
        if mz_diff != 1:
            continue
        ts_val = ts_col[row_idx]
        if ts_val is None:
            continue
        mz_timestamp = int(ts_val)
        row: list[str | None] = []
        for col_pos, col in enumerate(data_columns):
            val = col[row_idx]
            col_type = (
                column_types[col_pos]
                if column_types and col_pos < len(column_types)
                else None
            )
            if val is None:
                row.append(None)
            elif col_pos in ts_data_cols and isinstance(val, bytes) and len(val) == 16:
                row.append(_decode_packed_timestamp(val))
            else:
                row.append(_arrow_val_to_text(val, col_type=col_type))
        rows_by_timestamp.setdefault(mz_timestamp, []).append(row)
    return rows_by_timestamp


def _replay_continuous_object(
    c: Composition,
    workload: dict[str, Any],
    fqn: str,
    meta: dict[str, Any],
    data_path: str,
    speed: float,
    stop_event: threading.Event,
) -> None:
    """Replay continuous captured data for a single object at original cadence."""
    source_obj, child_obj = _find_source_for_meta(workload, meta)
    if source_obj is None and meta["source_type"] != "table":
        print(f"  Warning: no matching object for {fqn}, skipping continuous replay")
        return

    if data_path.endswith(".parquet"):
        col_types = [c["type"] for c in meta.get("columns", [])]
        rows_by_timestamp = _parse_continuous_parquet(data_path, column_types=col_types)
    else:
        rows_by_timestamp: dict[int, list[list[str | None]]] = {}
        with open(data_path) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                fields = line.split("\t")
                if len(fields) < 3:
                    continue

                mz_timestamp_str = fields[0]
                mz_progressed_str = fields[1]
                mz_diff_str = fields[2]

                if mz_progressed_str.lower() in ("t", "true"):
                    continue
                if mz_diff_str == "\\N":
                    continue
                try:
                    mz_diff = int(mz_diff_str)
                except ValueError:
                    continue
                if mz_diff != 1:
                    continue

                try:
                    mz_timestamp = int(mz_timestamp_str)
                except ValueError:
                    continue

                data_fields: list[str | None] = []
                for field in fields[3:]:
                    if field == "\\N":
                        data_fields.append(None)
                    else:
                        data_fields.append(unescape_copy_field(field))

                rows_by_timestamp.setdefault(mz_timestamp, []).append(data_fields)

    timestamps = sorted(rows_by_timestamp.keys())
    if not timestamps:
        return

    print(
        f"  Replaying {sum(len(v) for v in rows_by_timestamp.values())} "
        f"continuous rows for {fqn} across {len(timestamps)} timestamps"
    )

    first_ts = timestamps[0]
    replay_start = time.time()

    for ts in timestamps:
        if stop_event.is_set():
            break

        original_offset_s = (ts - first_ts) / 1000.0
        target_time = replay_start + original_offset_s / speed

        now = time.time()
        if target_time > now:
            stop_event.wait(timeout=target_time - now)
            if stop_event.is_set():
                break

        rows = rows_by_timestamp[ts]
        try:
            _ingest_captured_rows(c, meta, source_obj, child_obj, rows)
        except Exception as e:
            print(f"  Error replaying {fqn} at ts {ts}: {e}")
            break


def import_sql_captured_data_streaming(
    c: Composition,
    workload: dict[str, Any],
    data_dir: str,
    speed: float,
    stop_event: threading.Event,
) -> list[threading.Thread]:
    """Start threads to replay continuous SQL-captured data at original cadence.

    Returns list of started threads (caller should join them after stop_event is set).
    """
    capture_meta = workload.get("capture_meta", {})
    threads: list[threading.Thread] = []

    for fqn, meta in sorted(capture_meta.items()):
        parquet_path = os.path.join(data_dir, f"{fqn}.parquet")
        tsv_path = os.path.join(data_dir, f"{fqn}.tsv")
        if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
            data_path = parquet_path
        elif os.path.exists(tsv_path) and os.path.getsize(tsv_path) > 0:
            data_path = tsv_path
        else:
            continue

        t = PropagatingThread(
            target=_replay_continuous_object,
            name=f"replay-{fqn}",
            args=(c, workload, fqn, meta, data_path, speed, stop_event),
        )
        threads.append(t)

    return threads
