# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Checks the replica's schema registry with Confluent's Python serializer.

Run against an environmentd started with misc/kafka-listener/run-environmentd.sh:

    bin/pyactivate misc/kafka-listener/registry.py

A table with an inline Avro schema has the subject `<topic>-value` in the
registry of its cluster. A serializer that registers its schema, as Confluent
serializers do by default, writes to the table if its schema has the same
canonical form as the table's, and is refused otherwise.
"""

import json
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from pathlib import Path

import psycopg
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, str(Path(__file__).resolve().parent))
from demo import kafka_endpoint, sql  # noqa: E402

CLUSTER = "listener_registry"
USER, PASSWORD = "producer", "secret"

SCHEMA = {
    "type": "record",
    "name": "order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "item", "type": "string"},
    ],
}
# The same canonical form: other field order within each field, a doc and a default.
EQUIVALENT = {
    "name": "order",
    "type": "record",
    "doc": "an order",
    "fields": [
        {"type": "long", "name": "id"},
        {"name": "item", "type": "string", "default": ""},
    ],
}
INCOMPATIBLE = {
    "type": "record",
    "name": "order",
    "fields": [{"name": "id", "type": "long"}, {"name": "qty", "type": "int"}],
}


def check(name: str, ok: bool, detail: object = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'} {name}: {detail}")
    if not ok:
        raise SystemExit(1)


def get(url: str, auth: tuple[str, str] | None) -> tuple[int, object]:
    request = urllib.request.Request(url)
    if auth:
        token = b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def produce(
    bootstrap: str, topic: str, serializer: AvroSerializer, rows: list[dict]
) -> None:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": USER,
            "sasl.password": PASSWORD,
            "enable.idempotence": True,
        }
    )
    errors = []
    for row in rows:
        value = serializer(row, SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic, value=value, on_delivery=lambda e, _: errors.append(e))
    producer.flush(60)
    check(f"delivered {len(rows)} to {topic}", errors == [None] * len(rows), errors[:3])


def main() -> None:
    with psycopg.connect(
        host="localhost", port=6877, user="mz_system", autocommit=True
    ) as system:
        sql(system, "ALTER SYSTEM SET enable_kafka_listener_source = true")
        sql(system, "ALTER SYSTEM SET enable_replica_kafka_listener = true")
    conn = psycopg.connect(
        host="localhost", port=6875, user="materialize", autocommit=True
    )
    sql(conn, f"DROP CLUSTER IF EXISTS {CLUSTER} CASCADE")
    sql(conn, "DROP SOURCE IF EXISTS reg CASCADE")
    sql(conn, "DROP SECRET IF EXISTS reg_password")
    sql(conn, f"CREATE CLUSTER {CLUSTER} SIZE 'scale=1,workers=1'")
    sql(conn, f"CREATE SECRET reg_password AS '{PASSWORD}'")
    sql(
        conn,
        f"CREATE SOURCE reg IN CLUSTER {CLUSTER} FROM KAFKA LISTENER ("
        f"TOPIC PREFIX 'reg.', SASL USERNAME '{USER}', SASL PASSWORD SECRET reg_password, "
        "VALIDATE ON PRODUCE)",
    )
    schema = json.dumps(SCHEMA).replace("'", "''")
    sql(
        conn,
        f'CREATE TABLE reg_orders FROM SOURCE reg (REFERENCE "reg.orders") '
        f"FORMAT AVRO USING SCHEMA '{schema}'",
    )
    sql(
        conn,
        'CREATE TABLE reg_notes FROM SOURCE reg (REFERENCE "reg.notes") FORMAT JSON',
    )
    (cluster_id,) = sql(conn, f"SELECT id FROM mz_clusters WHERE name = '{CLUSTER}'")[0]
    bootstrap = kafka_endpoint(cluster_id)
    url = "http://" + kafka_endpoint(cluster_id, port="kafka-registry")
    auth = (USER, PASSWORD)

    print(f"Registry at {url}")
    for _ in range(60):
        status, subjects = get(f"{url}/subjects", auth)
        if subjects:
            break
        time.sleep(0.5)
    check(
        "subjects with credentials",
        (status, subjects) == (200, ["reg.orders-value"]),
        subjects,
    )
    check("subjects without credentials", get(f"{url}/subjects", None) == (200, []))
    status, body = get(f"{url}/subjects", (USER, "wrong"))
    check("wrong password", status == 401, body)

    client = SchemaRegistryClient(
        {"url": url, "basic.auth.user.info": f"{USER}:{PASSWORD}"}
    )
    latest = client.get_latest_version("reg.orders-value")
    check(
        "latest version",
        json.loads(latest.schema.schema_str) == SCHEMA,
        latest.schema_id,
    )

    print("Producing with Confluent serializers")
    # The default: the serializer registers its schema and encodes with the returned ID.
    registering = AvroSerializer(client, json.dumps(EQUIVALENT))
    produce(
        bootstrap,
        "reg.orders",
        registering,
        [{"id": i, "item": f"a{i}"} for i in range(5)],
    )
    # The serializer encodes with the subject's latest schema.
    latest_serializer = AvroSerializer(
        client,
        None,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    produce(
        bootstrap,
        "reg.orders",
        latest_serializer,
        [{"id": i, "item": f"b{i}"} for i in range(5, 10)],
    )
    incompatible = AvroSerializer(client, json.dumps(INCOMPATIBLE))
    try:
        incompatible(
            {"id": 1, "qty": 2}, SerializationContext("reg.orders", MessageField.VALUE)
        )
        check("incompatible schema refused", False)
    except SchemaRegistryError as e:
        check(
            "incompatible schema refused",
            e.http_status_code == 409,
            e.error_message[:80],
        )

    sql(conn, f"SET cluster = {CLUSTER}")
    for _ in range(120):
        rows = sql(conn, "SELECT id, item FROM reg_orders ORDER BY id")
        if len(rows) >= 10:
            break
        time.sleep(0.5)
    expected = [(i, f"a{i}") for i in range(5)] + [(i, f"b{i}") for i in range(5, 10)]
    check("rows in reg_orders", rows == expected, rows)
    print("All checks passed")


if __name__ == "__main__":
    main()
