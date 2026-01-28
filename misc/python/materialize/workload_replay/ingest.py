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
import time
import urllib.parse
from functools import cache
from textwrap import dedent
from typing import Any

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
            "name": topic,
            "namespace": "com.materialize",
            "fields": [
                {
                    "name": c.name,
                    "type": c.avro_type(),
                    **({"default": c.default} if c.default is not None else {}),
                }
                for c in columns[1:]
            ],
        }

        key_schema = {
            "type": "record",
            "name": f"{topic}_key",
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
