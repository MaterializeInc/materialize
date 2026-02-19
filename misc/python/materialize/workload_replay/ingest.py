# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Data ingestion functions for workload replay.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
import urllib.parse
from collections.abc import Iterator
from functools import cache
from textwrap import dedent
from typing import Any, Literal, overload

import aiohttp
import confluent_kafka  # type: ignore
import psycopg
import pymysql
from confluent_kafka.schema_registry import SchemaRegistryClient  # type: ignore
from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from confluent_kafka.serialization import (  # type: ignore
    MessageField,
    SerializationContext,
)
from psycopg.sql import SQL, Identifier

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.workload_replay.column import Column
from materialize.workload_replay.config import SEED_RANGE
from materialize.workload_replay.util import (
    get_kafka_topic,
    get_mysql_reference_db_table,
    get_postgres_reference_db_schema_table,
    get_sql_server_reference_db_schema_table,
)


def delivery_report(err: str, msg: Any) -> None:
    """Kafka delivery report callback."""
    assert err is None, f"Delivery failed for user record {msg.key()}: {err}"


async def ingest_webhook(
    c: Composition,
    db: str,
    schema: str,
    name: str,
    source: dict[str, Any],
    num_rows: int,
    print_progress: bool = False,
) -> None:
    """Ingest data into a webhook source via HTTP."""
    url = (
        f"http://127.0.0.1:{c.port('materialized', 6876)}/api/webhook/"
        f"{urllib.parse.quote(db, safe='')}/"
        f"{urllib.parse.quote(schema, safe='')}/"
        f"{urllib.parse.quote(name, safe='')}"
    )

    body_column = None
    headers_column = None
    for column in source["columns"]:
        if column["name"] == "body":
            body_column = Column(
                column["name"],
                column["type"],
                column["nullable"],
                column["default"],
                column.get("data_shape"),
            )
        elif column["name"] == "headers":
            headers_column = Column(
                column["name"],
                column["type"],
                column["nullable"],
                column["default"],
                column.get("data_shape"),
            )
    assert body_column

    connector = aiohttp.TCPConnector(limit=5000)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(5000)

        progress = 0
        progress_lock = asyncio.Lock()

        async def report_progress() -> None:
            nonlocal progress
            async with progress_lock:
                progress += 1
                if progress % 10000 == 0 or progress == num_rows:
                    print(
                        f"{progress}/{num_rows} ({progress / num_rows:.1%})",
                        end="\r",
                        flush=True,
                    )

        async def send_one(seed: int):
            rng = random.Random(seed)
            backoff = 0.01

            while True:
                async with sem:
                    async with session.post(
                        url,
                        data=body_column.kafka_value(rng),
                        headers=(
                            headers_column.kafka_value(rng) if headers_column else None
                        ),
                    ) as resp:
                        if resp.status == 200:
                            if print_progress:
                                await report_progress()
                            return
                        elif resp.status == 429:
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 1.0)
                        else:
                            text = await resp.text()
                            raise RuntimeError(
                                f"Webhook ingestion failed: {resp.status}: {text}"
                            )

        await asyncio.gather(
            *(send_one(random.randrange(SEED_RANGE)) for _ in range(num_rows))
        )
        print()


@cache
def get_kafka_objects(
    topic: str,
    columns: tuple,
    debezium: bool,
    schema_registry_port: int,
    kafka_port: int,
):
    """
    Build and cache Kafka producer + Avro serializers for a topic/schema combo.
    `columns` MUST be a tuple so this function can be cached.
    """

    # Avro record/namespace names must match [A-Za-z_][A-Za-z0-9_]*.
    avro_name = re.sub(r"[^A-Za-z0-9_]", "_", topic)

    registry = SchemaRegistryClient({"url": f"http://127.0.0.1:{schema_registry_port}"})

    producer = confluent_kafka.Producer(
        {
            "bootstrap.servers": f"127.0.0.1:{kafka_port}",
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "queue.buffering.max.kbytes": 1048576,
            "compression.type": "lz4",
            "acks": "1",
            "retries": 3,
        }
    )

    col_names = [c.name for c in columns]

    if debezium:
        # Avro namespaces allow dots (they're dot-separated component names),
        # so use the original topic name for namespace and connect.name to match
        # the schema names in the captured SEED KEY/VALUE SCHEMA definitions.
        # Only record "name" fields require the sanitized avro_name.
        value_record_schema = {
            "type": "record",
            "name": "Value",
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns
            ],
            "connect.name": f"{topic}.Value",
        }

        envelope_schema = {
            "type": "record",
            "name": "Envelope",
            "namespace": topic,
            "fields": [
                {
                    "name": "before",
                    "type": ["null", value_record_schema],
                    "default": None,
                },
                {"name": "after", "type": ["null", "Value"], "default": None},
                {
                    "name": "source",
                    "type": {
                        "type": "record",
                        "name": "Source",
                        "namespace": "io.debezium.connector.mysql",
                        "fields": [
                            {"name": "version", "type": "string"},
                            {"name": "connector", "type": "string"},
                            {"name": "name", "type": "string"},
                            {"name": "ts_ms", "type": "long"},
                            {
                                "name": "snapshot",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "db", "type": "string"},
                            {
                                "name": "sequence",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {
                                "name": "table",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "server_id", "type": "long"},
                            {
                                "name": "gtid",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "file", "type": "string"},
                            {"name": "pos", "type": "long"},
                            {"name": "row", "type": "int"},
                            {
                                "name": "thread",
                                "type": ["null", "long"],
                                "default": None,
                            },
                            {
                                "name": "query",
                                "type": ["null", "string"],
                                "default": None,
                            },
                        ],
                        "connect.name": "io.debezium.connector.mysql.Source",
                    },
                },
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": ["null", "long"], "default": None},
                {"name": "transaction", "type": ["null", "string"], "default": None},
            ],
            "connect.name": f"{topic}.Envelope",
        }

        key_schema = {
            "type": "record",
            "name": "Key",
            "namespace": topic,
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns
            ],
            "connect.name": f"{topic}.Key",
        }

        value_serializer = AvroSerializer(
            registry,
            json.dumps(envelope_schema),
            lambda d, ctx: d,
        )

        key_serializer = AvroSerializer(
            registry,
            json.dumps(key_schema),
            lambda d, ctx: d,
        )

    else:
        key_col = columns[0]

        value_schema = {
            "type": "record",
            "name": avro_name,
            "namespace": "com.materialize",
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns
            ],
        }

        key_schema = {
            "type": "record",
            "name": f"{avro_name}_key",
            "namespace": "com.materialize",
            "fields": [
                {
                    "name": key_col.name,
                    "type": key_col.avro_type(),
                    **(
                        {"default": key_col.default}
                        if key_col.default is not None
                        else {}
                    ),
                }
            ],
        }

        value_serializer = AvroSerializer(
            registry,
            json.dumps(value_schema),
            lambda d, ctx: d,
        )

        key_serializer = AvroSerializer(
            registry,
            json.dumps(key_schema),
            lambda d, ctx: d,
        )

    value_ctx = SerializationContext(topic, MessageField.VALUE)
    key_ctx = SerializationContext(topic, MessageField.KEY)

    return producer, value_serializer, key_serializer, value_ctx, key_ctx, col_names


def ingest(
    c: Composition,
    child: dict[str, Any],
    source: dict[str, Any],
    columns: list[Column],
    num_rows: int,
    rng: random.Random,
) -> None:
    """Ingest data into a source (Postgres, MySQL, Kafka, SQL Server)."""
    if source["type"] == "postgres":
        ref_database, ref_schema, ref_table = get_postgres_reference_db_schema_table(
            child
        )
        conn = psycopg.connect(
            host="127.0.0.1",
            port=c.default_port("postgres"),
            user="postgres",
            password="postgres",
            dbname=ref_database,
        )
        conn.autocommit = True

        col_names = [col.name for col in columns]

        with conn.cursor() as cur:
            copy_stmt = SQL("COPY {}.{} ({}) FROM STDIN").format(
                Identifier(ref_schema),
                Identifier(ref_table),
                SQL(", ").join(map(Identifier, col_names)),
            )

            with cur.copy(copy_stmt) as copy:
                for _ in range(num_rows):
                    row = [col.value(rng, in_query=False) for col in columns]
                    copy.write_row(row)

    elif source["type"] == "mysql":
        ref_database, ref_table = get_mysql_reference_db_table(child)

        conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            database=ref_database,
            port=c.default_port("mysql"),
            autocommit=False,
        )

        value_funcs = [col.value for col in columns]
        rows_sql = []
        for _ in range(num_rows):
            row = [fn(rng) for fn in value_funcs]
            rows_sql.append("(" + ", ".join(row) + ")")

        stmt = f"INSERT INTO {ref_table} VALUES " + ", ".join(rows_sql)

        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.close()

    elif source["type"] == "kafka":
        batch_values_kafka = []
        for _ in range(num_rows):
            row = [col.kafka_value(rng) for col in columns]
            batch_values_kafka.append(row)

        topic = get_kafka_topic(source)
        debezium = "ENVELOPE DEBEZIUM" in child["create_sql"]

        producer, serializer, key_serializer, sctx, ksctx, col_names = (
            get_kafka_objects(
                topic,
                tuple(columns),
                debezium,
                c.default_port("schema-registry"),
                c.default_port("kafka"),
            )
        )
        now_ms = int(time.time() * 1000)
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
        for row in batch_values_kafka:
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

                        key_value = after_value

                        producer.produce(
                            topic=topic,
                            key=key_serializer(key_value, ksctx),
                            value=serializer(envelope_value, sctx),
                            on_delivery=delivery_report,
                        )
                    else:
                        key_dict = {col_names[0]: row[0]}
                        value_dict = dict(zip(col_names, row))

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
    elif source["type"] == "sql-server":
        batch_values = []
        for _ in range(num_rows):
            row = [col.value(rng) for col in columns]
            batch_values.append(f"({', '.join(row)})")

        ref_database, ref_schema, ref_table = get_sql_server_reference_db_schema_table(
            child
        )
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE {ref_database};
                INSERT INTO {ref_schema}.{ref_table} VALUES {', '.join(batch_values)}
            """
            ),
            quiet=True,
            silent=True,
        )
    elif source["type"] == "load-generator":
        pass
    else:
        raise ValueError(f"Unhandled source type {source['type']}")


# --- Captured data ingestion functions ---


def _decode_packed_numeric(binary_data: bytes) -> str:
    """Decode MZ's PackedNumeric (40 bytes) to decimal string."""
    import struct

    digits = struct.unpack_from("<I", binary_data, 0)[0]
    exponent = struct.unpack_from("<i", binary_data, 4)[0]
    lsu = [struct.unpack_from("<H", binary_data, 8 + i * 2)[0] for i in range(13)]
    bits = binary_data[34]

    if digits == 0:
        return "0"

    # Reconstruct coefficient from LSU (groups of 3 digits, LSB first).
    n_units = (digits + 2) // 3
    coeff_str = ""
    for i in range(n_units - 1, -1, -1):
        group = str(lsu[i])
        if i < n_units - 1:
            group = group.zfill(3)
        coeff_str += group
    coeff_str = coeff_str[:digits]

    # Place decimal point.  decimal_pos = digits + exponent
    decimal_pos = digits + exponent
    if decimal_pos <= 0:
        result = "0." + "0" * (-decimal_pos) + coeff_str
    elif decimal_pos >= digits:
        result = coeff_str + "0" * (decimal_pos - digits)
    else:
        result = coeff_str[:decimal_pos] + "." + coeff_str[decimal_pos:]

    is_negative = (bits & 0x08) != 0  # DECNEG
    if is_negative:
        result = "-" + result
    return result


def _decode_packed_timestamp(binary_data: bytes) -> str:
    """Decode MZ's PackedNaiveDateTime (16 bytes) to timestamp string."""
    import struct

    year_raw = struct.unpack_from(">I", binary_data, 0)[0]
    year_unsigned = year_raw ^ 0x80000000
    year = year_unsigned if year_unsigned < 0x80000000 else year_unsigned - 0x100000000
    ordinal = struct.unpack_from(">I", binary_data, 4)[0]
    secs = struct.unpack_from(">I", binary_data, 8)[0]
    nano = struct.unpack_from(">I", binary_data, 12)[0]

    # ordinal → month, day
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    days_in_month = [31, 29 if is_leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    month, day = 1, ordinal
    for dm in days_in_month:
        if day <= dm:
            break
        day -= dm
        month += 1

    hours = secs // 3600
    minutes = (secs % 3600) // 60
    seconds = secs % 60
    micro = nano // 1000

    ts = f"{year:04d}-{month:02d}-{day:02d} {hours:02d}:{minutes:02d}:{seconds:02d}"
    if micro > 0:
        ts += f".{micro:06d}".rstrip("0")
    return ts


def _arrow_val_to_text(val: Any, col_type: str | None = None) -> str | None:
    """Convert a pyarrow-decoded Python value to PostgreSQL COPY text."""
    import datetime

    if val is None:
        return None
    if isinstance(val, bool):
        return "t" if val else "f"
    if isinstance(val, datetime.date):
        return val.isoformat()
    if isinstance(val, int | float):
        # Date columns in persist are stored as int32 (days since Unix epoch).
        if col_type == "date" and isinstance(val, int):
            return _days_to_date_string(val)
        return str(val)
    if isinstance(val, str):
        return val
    if isinstance(val, bytes):
        return "\\x" + val.hex()
    if isinstance(val, list):
        # Check for map (list of {key, val} dicts).
        if val and isinstance(val[0], dict) and "key" in val[0]:
            pairs = []
            for entry in val:
                k = str(entry["key"]) if entry["key"] is not None else ""
                v = str(entry["val"]) if entry.get("val") is not None else "NULL"
                pairs.append(f"{k}=>{v}")
            return "{" + ",".join(pairs) + "}"
        # Regular list/array.
        elems = [_arrow_val_to_text(e) or "NULL" for e in val]
        return "{" + ",".join(elems) + "}"
    if isinstance(val, dict):
        # numeric: struct<approx, binary>
        if "approx" in val and "binary" in val and val["binary"] is not None:
            return _decode_packed_numeric(val["binary"])
        # array: struct<dims, vals> (persist's integer[]/etc. encoding)
        if "vals" in val and "dims" in val:
            items = val["vals"]
            if items is None:
                return "{}"
            elems = [_arrow_val_to_text(e) or "NULL" for e in items]
            return "{" + ",".join(elems) + "}"
        # array: struct<items, dimensions> (alternate field names)
        if "items" in val and "dimensions" in val:
            items = val["items"]
            if items is None:
                return "{}"
            elems = [_arrow_val_to_text(e) or "NULL" for e in items]
            return "{" + ",".join(elems) + "}"
        return str(val)
    return str(val)


def _days_to_date_string(days: int) -> str:
    """Convert days since Postgres epoch (2000-01-01) to a date string.

    MZ's persist stores dates as pg_epoch_days (days since 2000-01-01).
    """
    import datetime

    try:
        d = datetime.date(2000, 1, 1) + datetime.timedelta(days=days)
        return d.isoformat()
    except (ValueError, OverflowError):
        # Date is outside Python's range (before 0001-01-01 or after 9999-12-31).
        # Use Julian Day Number conversion for BCE dates.
        jdn = days + 2451545  # JDN of 2000-01-01
        return _jdn_to_date_string(jdn)


def _jdn_to_date_string(jdn: int) -> str:
    """Convert Julian Day Number to Postgres-compatible date string."""
    # Meeus algorithm: JDN to proleptic Gregorian calendar
    a = jdn + 32044
    b = (4 * a + 3) // 146097
    c = a - (146097 * b) // 4
    d = (4 * c + 3) // 1461
    e = c - (1461 * d) // 4
    m = (5 * e + 2) // 153

    day = e - (153 * m + 2) // 5 + 1
    month = m + 3 - 12 * (m // 10)
    year = 100 * b + d - 4800 + m // 10

    if year <= 0:
        # BCE: year 0 = 1 BC, year -1 = 2 BC, etc.
        bc_year = 1 - year
        return f"{bc_year:04d}-{month:02d}-{day:02d} BC"
    return f"{year:04d}-{month:02d}-{day:02d}"


@overload
def parse_parquet_file(
    parquet_path: str,
    column_types: list[str] | None = None,
    group_by_timestamp: Literal[False] = False,
) -> list[list[str | None]]: ...


@overload
def parse_parquet_file(
    parquet_path: str,
    column_types: list[str] | None = None,
    *,
    group_by_timestamp: Literal[True],
) -> dict[int, list[list[str | None]]]: ...


def parse_parquet_file(
    parquet_path: str,
    column_types: list[str] | None = None,
    group_by_timestamp: bool = False,
) -> list[list[str | None]] | dict[int, list[list[str | None]]]:
    """Read a Parquet file and return rows as list of str|None values.

    Converts all values to string representation compatible with
    PostgreSQL COPY text format (same format as parse_tsv_file output).

    Handles MZ's internal persist encodings:
    - map → list<struct<key,val>> → {k=>v,...}
    - numeric → struct<approx,binary> → PackedNumeric decode
    - timestamp → fixed_size_binary[16] → PackedNaiveDateTime decode
    - array → struct<dims,vals> → {elem,...}
    - date → int32 (days since epoch) → YYYY-MM-DD
    """
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)

    # Detect columns that need special fixed_size_binary[16] timestamp decoding.
    ts_cols: set[int] = set()
    for i in range(table.num_columns):
        dt = table.schema.field(i).type
        if str(dt) == "fixed_size_binary[16]":
            ts_cols.add(i)

    # When group_by_timestamp, first two columns are mz_timestamp and mz_diff.
    # column_types refers to the data columns only (starting at index 2).
    data_col_start = 2 if group_by_timestamp else 0
    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]

    def _convert_row(row_idx: int) -> list[str | None]:
        row: list[str | None] = []
        for col_idx in range(data_col_start, table.num_columns):
            val = columns[col_idx][row_idx]
            type_idx = col_idx - data_col_start
            col_type = (
                column_types[type_idx]
                if column_types and type_idx < len(column_types)
                else None
            )
            if val is None:
                row.append(None)
            elif col_idx in ts_cols and isinstance(val, bytes) and len(val) == 16:
                row.append(_decode_packed_timestamp(val))
            else:
                row.append(_arrow_val_to_text(val, col_type=col_type))
        return row

    if group_by_timestamp:
        from collections import defaultdict

        ts_col = columns[0]  # mz_timestamp
        diff_col = columns[1]  # mz_diff
        grouped: dict[int, list[list[str | None]]] = defaultdict(list)
        for row_idx in range(table.num_rows):
            if diff_col[row_idx] != 1:
                continue
            ts = ts_col[row_idx]
            assert ts is not None, "mz_timestamp must not be NULL"
            grouped[int(ts)].append(_convert_row(row_idx))
        return dict(grouped)
    else:
        return [_convert_row(row_idx) for row_idx in range(table.num_rows)]


def get_parquet_row_count(parquet_path: str) -> int:
    """Get the total number of rows in a Parquet file from metadata (no data read)."""
    import pyarrow.parquet as pq

    return pq.ParquetFile(parquet_path).metadata.num_rows


def iter_parquet_batches(
    parquet_path: str,
    column_types: list[str] | None = None,
    batch_size: int = 10000,
) -> Iterator[list[list[str | None]]]:
    """Yield batches of rows from a Parquet file with bounded memory usage.

    Unlike parse_parquet_file which loads the entire file into memory,
    this reads one RecordBatch at a time so only ~batch_size rows worth
    of Python objects are alive at any point.
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_path)
    schema = pf.schema_arrow

    # Detect columns that need special fixed_size_binary[16] timestamp decoding.
    ts_cols: set[int] = set()
    for i in range(len(schema)):
        if str(schema.field(i).type) == "fixed_size_binary[16]":
            ts_cols.add(i)

    for batch in pf.iter_batches(batch_size=batch_size):
        num_cols = batch.num_columns
        columns = [batch.column(i).to_pylist() for i in range(num_cols)]

        rows: list[list[str | None]] = []
        for row_idx in range(batch.num_rows):
            row: list[str | None] = []
            for col_idx in range(num_cols):
                val = columns[col_idx][row_idx]
                col_type = (
                    column_types[col_idx]
                    if column_types and col_idx < len(column_types)
                    else None
                )
                if val is None:
                    row.append(None)
                elif col_idx in ts_cols and isinstance(val, bytes) and len(val) == 16:
                    row.append(_decode_packed_timestamp(val))
                else:
                    row.append(_arrow_val_to_text(val, col_type=col_type))
            rows.append(row)

        if rows:
            yield rows


def _tsv_to_typed_value(value: str | None, sql_type: str) -> Any:
    """Convert a TSV string value to a typed Python value for Kafka/Avro."""
    if value is None:
        return None
    t = sql_type.lower()
    if t in ("smallint", "integer", "int2", "int4", "uint2", "uint4"):
        return int(value)
    elif t in ("bigint", "int8", "uint8"):
        return int(value)
    elif t in ("real", "float", "float4"):
        return float(value)
    elif t in ("double precision", "float8", "numeric"):
        return float(value)
    elif t == "boolean":
        return value.lower() in ("t", "true", "1")
    elif t == "bytea":
        if value.startswith("\\x"):
            return bytes.fromhex(value[2:])
        return value.encode()
    elif t in ("timestamp with time zone", "timestamp without time zone"):
        return int(value) if value.isdigit() else value
    else:
        return value


def ingest_captured_rows_mz_table(
    c: Composition,
    meta: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """COPY captured rows into a Materialize table."""
    conn = psycopg.connect(
        host="127.0.0.1",
        port=c.port("materialized", 6877),
        user="mz_system",
        password="materialize",
        dbname="materialize",
    )
    conn.autocommit = True

    col_names = [col["name"] for col in meta["columns"]]

    copy_stmt = SQL("COPY {}.{}.{} ({}) FROM STDIN").format(
        Identifier(meta["database"]),
        Identifier(meta["schema"]),
        Identifier(meta["name"]),
        SQL(", ").join(map(Identifier, col_names)),
    )

    batch_size = 10000
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            with cur.copy(copy_stmt) as copy:
                for row in batch:
                    copy.write_row(row)

    conn.close()


def ingest_captured_rows_postgres(
    c: Composition,
    meta: dict[str, Any],
    child_obj: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """COPY captured rows into upstream Postgres."""
    ref_db, ref_schema, ref_table = get_postgres_reference_db_schema_table(child_obj)

    conn = psycopg.connect(
        host="127.0.0.1",
        port=c.default_port("postgres"),
        user="postgres",
        password="postgres",
        dbname=ref_db,
    )
    conn.autocommit = True

    col_names = [col["name"] for col in meta["columns"]]

    copy_stmt = SQL("COPY {}.{} ({}) FROM STDIN").format(
        Identifier(ref_schema),
        Identifier(ref_table),
        SQL(", ").join(map(Identifier, col_names)),
    )

    batch_size = 10000
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            with cur.copy(copy_stmt) as copy:
                for row in batch:
                    copy.write_row(row)

    conn.close()


def ingest_captured_rows_mysql(
    c: Composition,
    meta: dict[str, Any],
    child_obj: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """Batch INSERT captured rows into upstream MySQL."""
    ref_db, ref_table = get_mysql_reference_db_table(child_obj)

    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password=MySql.DEFAULT_ROOT_PASSWORD,
        database=ref_db,
        port=c.default_port("mysql"),
        autocommit=False,
    )

    col_names = [col["name"] for col in meta["columns"]]
    placeholders = ", ".join(["%s"] * len(col_names))
    col_list = ", ".join(f"`{name}`" for name in col_names)
    stmt = f"INSERT INTO `{ref_table}` ({col_list}) VALUES ({placeholders})"

    batch_size = 1000
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            cur.executemany(stmt, [tuple(row) for row in batch])
    conn.commit()
    conn.close()


def ingest_captured_rows_kafka(
    c: Composition,
    meta: dict[str, Any],
    source_obj: dict[str, Any],
    child_obj: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """Produce captured rows as Avro messages to Kafka topic."""
    topic = get_kafka_topic(source_obj)
    debezium = "ENVELOPE DEBEZIUM" in child_obj.get("create_sql", "")

    columns = [
        Column(col["name"], col["type"], col.get("nullable", True), None, None)
        for col in meta["columns"]
    ]

    producer, serializer, key_serializer, sctx, ksctx, col_names = get_kafka_objects(
        topic,
        tuple(columns),
        debezium,
        c.default_port("schema-registry"),
        c.default_port("kafka"),
    )

    now_ms = int(time.time() * 1000)
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
    for row in rows:
        typed_values = [
            _tsv_to_typed_value(val, meta["columns"][i]["type"])
            for i, val in enumerate(row)
        ]
        while True:
            try:
                if debezium:
                    after_value = dict(zip(col_names, typed_values))
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
                    key_dict = {col_names[0]: typed_values[0]}
                    value_dict = dict(zip(col_names, typed_values))
                    producer.produce(
                        topic=topic,
                        key=key_serializer(key_dict, ksctx),
                        value=serializer(value_dict, sctx),
                        on_delivery=delivery_report,
                    )
                break
            except BufferError:
                producer.poll(0.01)
    producer.flush()


def _arrow_val_to_avro(val: Any, is_packed_ts: bool) -> Any:
    """Convert persist-internal pyarrow encodings to Avro-compatible Python types.

    Most pyarrow types (str, int, float, bool, bytes) already match their Avro
    counterparts and never reach this function.  Only packed timestamps and
    packed numerics from persist's internal Arrow encoding need conversion.
    """
    if val is None:
        return None
    if is_packed_ts and isinstance(val, bytes) and len(val) == 16:
        # Packed NaiveDateTime → epoch milliseconds for Avro "long".
        from datetime import datetime as _dt

        ts_str = _decode_packed_timestamp(val)
        parsed = _dt.fromisoformat(ts_str.replace(" ", "T"))
        epoch = _dt(1970, 1, 1)
        return int((parsed - epoch).total_seconds() * 1000)
    if isinstance(val, dict) and "binary" in val:
        # Packed numeric (struct<approx, binary>) → float for Avro "double".
        binary = val.get("binary")
        if binary is not None:
            return float(_decode_packed_numeric(binary))
        return val.get("approx")
    return val


def _kafka_ingest_worker(
    parquet_path: str,
    row_group_indices: list[int],
    topic: str,
    debezium: bool,
    columns_meta: list[dict[str, Any]],
    schema_registry_port: int,
    kafka_port: int,
    progress_value: Any,
    total_rows: int,
    batch_size: int,
) -> None:
    """Worker that ingests assigned parquet row groups to Kafka.

    Designed to be called both directly (single-process) and as a
    multiprocessing target (parallel).  All arguments are picklable so
    that ``spawn`` context works.
    """
    import pyarrow.parquet as pq

    col_names = [col["name"] for col in columns_meta]
    num_cols = len(col_names)

    columns = [
        Column(col["name"], col["type"], col.get("nullable", True), None, None)
        for col in columns_meta
    ]

    producer, serializer, key_serializer, sctx, ksctx, _ = get_kafka_objects(
        topic,
        tuple(columns),
        debezium,
        schema_registry_port,
        kafka_port,
    )

    now_ms = int(time.time() * 1000)
    envelope_value = None
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
        envelope_value = {
            "before": None,
            "after": None,
            "source": source_struct,
            "op": "c",
            "ts_ms": now_ms,
            "transaction": None,
        }

    pf = pq.ParquetFile(parquet_path)
    schema = pf.schema_arrow

    # Pre-compute which columns need special conversion.
    needs_conversion = [False] * num_cols
    is_packed_ts = [False] * num_cols
    for i in range(num_cols):
        field_type = str(schema.field(i).type)
        if field_type == "fixed_size_binary[16]":
            needs_conversion[i] = True
            is_packed_ts[i] = True
        elif field_type.startswith("struct<"):
            needs_conversion[i] = True
    any_need_conversion = any(needs_conversion)

    rows_done = 0
    producer.poll(0)
    for rg_idx in row_group_indices:
        table = pf.read_row_group(rg_idx)
        for batch in table.to_batches(max_chunksize=batch_size):
            py_columns = [
                batch.column(i).to_pylist() for i in range(batch.num_columns)
            ]

            for row_idx in range(batch.num_rows):
                after_value: dict[str, Any] = {}
                if any_need_conversion:
                    for col_idx in range(num_cols):
                        val = py_columns[col_idx][row_idx]
                        if needs_conversion[col_idx] and val is not None:
                            val = _arrow_val_to_avro(val, is_packed_ts[col_idx])
                        after_value[col_names[col_idx]] = val
                else:
                    for col_idx in range(num_cols):
                        after_value[col_names[col_idx]] = py_columns[col_idx][row_idx]

                while True:
                    try:
                        if debezium:
                            envelope_value["after"] = after_value
                            producer.produce(
                                topic=topic,
                                key=key_serializer(after_value, ksctx),
                                value=serializer(envelope_value, sctx),
                                on_delivery=delivery_report,
                            )
                        else:
                            key_dict = {col_names[0]: after_value[col_names[0]]}
                            producer.produce(
                                topic=topic,
                                key=key_serializer(key_dict, ksctx),
                                value=serializer(after_value, sctx),
                                on_delivery=delivery_report,
                            )
                        break
                    except BufferError:
                        producer.poll(0.01)

            rows_done += batch.num_rows
            if progress_value is not None:
                with progress_value.get_lock():
                    progress_value.value += batch.num_rows
            else:
                print(
                    f"    {rows_done}/{total_rows} ({rows_done / total_rows:.1%})",
                    end="\r",
                    flush=True,
                )
            producer.poll(0)

        del table

    producer.flush()


def ingest_captured_parquet_kafka(
    c: Composition,
    meta: dict[str, Any],
    source_obj: dict[str, Any],
    child_obj: dict[str, Any],
    parquet_path: str,
    batch_size: int = 10000,
) -> None:
    """Stream Parquet data directly to Kafka, skipping string conversion.

    Pyarrow values (str, int, float, bytes) go straight into Avro dicts
    without an Arrow->string->typed-value round-trip.  Uses multiprocessing
    to saturate multiple CPU cores when the parquet file has enough row
    groups.
    """
    import multiprocessing
    import os

    import pyarrow.parquet as pq

    topic = get_kafka_topic(source_obj)
    debezium = "ENVELOPE DEBEZIUM" in child_obj.get("create_sql", "")
    columns_meta = meta["columns"]

    pf = pq.ParquetFile(parquet_path)
    total_rows = pf.metadata.num_rows
    num_row_groups = pf.metadata.num_row_groups

    sr_port = c.default_port("schema-registry")
    kafka_port = c.default_port("kafka")

    num_workers = min(os.cpu_count() or 1, num_row_groups)

    if num_workers <= 1:
        # Single-process fast path.
        _kafka_ingest_worker(
            parquet_path,
            list(range(num_row_groups)),
            topic,
            debezium,
            columns_meta,
            sr_port,
            kafka_port,
            None,
            total_rows,
            batch_size,
        )
        print()
        return

    # Distribute row groups round-robin across workers.
    worker_row_groups: list[list[int]] = [[] for _ in range(num_workers)]
    for i in range(num_row_groups):
        worker_row_groups[i % num_workers].append(i)

    # Use 'spawn' to avoid inheriting the parent's @cache'd Kafka
    # producers whose background threads don't survive fork.
    ctx = multiprocessing.get_context("spawn")
    progress = ctx.Value("l", 0)

    processes: list[multiprocessing.process.BaseProcess] = []
    for worker_rgs in worker_row_groups:
        if not worker_rgs:
            continue
        p = ctx.Process(
            target=_kafka_ingest_worker,
            args=(
                parquet_path,
                worker_rgs,
                topic,
                debezium,
                columns_meta,
                sr_port,
                kafka_port,
                progress,
                total_rows,
                batch_size,
            ),
        )
        p.start()
        processes.append(p)

    # Monitor progress until all workers finish.
    while any(p.is_alive() for p in processes):
        done = progress.value
        print(
            f"    {done}/{total_rows} ({done / total_rows:.1%})",
            end="\r",
            flush=True,
        )
        time.sleep(0.5)

    for p in processes:
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(
                f"Kafka ingestion worker failed with exit code {p.exitcode}"
            )

    print(f"    {total_rows}/{total_rows} (100.0%)")
    print()


def ingest_captured_rows_sql_server(
    c: Composition,
    meta: dict[str, Any],
    child_obj: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """INSERT captured rows into upstream SQL Server via testdrive."""
    ref_db, ref_schema, ref_table = get_sql_server_reference_db_schema_table(child_obj)

    batch_size = 100
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        values_strs = []
        for row in batch:
            formatted = []
            for i, val in enumerate(row):
                if val is None:
                    formatted.append("NULL")
                else:
                    escaped = val.replace("'", "''")
                    formatted.append(f"'{escaped}'")
            values_strs.append(f"({', '.join(formatted)})")

        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE {ref_db};
                INSERT INTO {ref_schema}.{ref_table} VALUES {', '.join(values_strs)}
            """
            ),
            quiet=True,
            silent=True,
        )


def ingest_captured_rows_webhook(
    c: Composition,
    meta: dict[str, Any],
    rows: list[list[str | None]],
) -> None:
    """HTTP POST captured rows to a webhook source."""
    url = (
        f"http://127.0.0.1:{c.port('materialized', 6876)}/api/webhook/"
        f"{urllib.parse.quote(meta['database'], safe='')}/"
        f"{urllib.parse.quote(meta['schema'], safe='')}/"
        f"{urllib.parse.quote(meta['name'], safe='')}"
    )

    async def _post_all() -> None:
        connector = aiohttp.TCPConnector(limit=100)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            sem = asyncio.Semaphore(100)
            for row in rows:
                body = row[0] if row else ""
                backoff = 0.01
                while True:
                    async with sem:
                        async with session.post(url, data=body) as resp:
                            if resp.status == 200:
                                break
                            elif resp.status == 429:
                                await asyncio.sleep(backoff)
                                backoff = min(backoff * 2, 1.0)
                            else:
                                text = await resp.text()
                                raise RuntimeError(
                                    f"Webhook ingestion failed: {resp.status}: {text}"
                                )

    asyncio.run(_post_all())


def ingest_captured_rows(
    c: Composition,
    workload: dict[str, Any],
    meta: dict[str, Any],
    source_obj: dict[str, Any] | None,
    rows: list[list[str | None]],
) -> None:
    """Dispatch captured rows to the appropriate upstream system."""
    if not rows:
        return

    # MZ table (no parent source, or explicitly a table).
    if source_obj is None or meta.get("object_type") == "table":
        ingest_captured_rows_mz_table(c, meta, rows)
        return

    source_type = source_obj.get("type", "")

    # Find the child object (for subsources) or use source itself.
    child_obj = source_obj
    children = source_obj.get("children", {})
    if children and meta["name"] in children:
        child_obj = children[meta["name"]]

    if source_type == "postgres":
        ingest_captured_rows_postgres(c, meta, child_obj, rows)
    elif source_type == "mysql":
        ingest_captured_rows_mysql(c, meta, child_obj, rows)
    elif source_type == "kafka":
        ingest_captured_rows_kafka(c, meta, source_obj, child_obj, rows)
    elif source_type == "sql-server":
        ingest_captured_rows_sql_server(c, meta, child_obj, rows)
    elif source_type == "webhook":
        ingest_captured_rows_webhook(c, meta, rows)
    elif source_type == "load-generator":
        pass  # nothing to replay
    else:
        print(
            f"  Warning: unknown source type '{source_type}' for {meta.get('name', '?')}, skipping"
        )
