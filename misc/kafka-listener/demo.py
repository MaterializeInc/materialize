# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Demo of Kafka listener sources: Kafka producers writing straight into Materialize.

Run against an environmentd started with misc/kafka-listener/run-environmentd.sh:

    bin/pyactivate misc/kafka-listener/demo.py

The demo creates a cluster whose replica accepts Kafka connections, two Kafka
listener sources on it, and a view joining them. It then produces records with
librdkafka (through confluent-kafka), one topic without authentication and one
with SASL/PLAIN, and queries the results.
"""

import argparse
import json
import random
import time
from pathlib import Path

import psycopg
from confluent_kafka import Producer

MZ_ROOT = Path(__file__).resolve().parent.parent.parent
CLUSTER = "kafka_ingest"
SASL_USERNAME = "ingest"
SASL_PASSWORD = "hunter2"

SETUP = f"""
DROP CLUSTER IF EXISTS {CLUSTER} CASCADE;
DROP VIEW IF EXISTS page_views_by_user;
CREATE CLUSTER {CLUSTER} SIZE 'scale=1,workers=1';

DROP SECRET IF EXISTS kafka_ingest_password;
CREATE SECRET kafka_ingest_password AS '{SASL_PASSWORD}';

-- Append-only events. Anyone who can reach the replica may produce.
CREATE SOURCE page_views
  IN CLUSTER {CLUSTER}
  FROM KAFKA LISTENER (TOPIC 'page_views')
  KEY FORMAT TEXT VALUE FORMAT JSON
  INCLUDE KEY AS user_id, OFFSET, TIMESTAMP AS produced_at, HEADERS
  ENVELOPE NONE;

-- An upserted table of users. Producers must authenticate with SASL/PLAIN.
CREATE SOURCE users
  IN CLUSTER {CLUSTER}
  FROM KAFKA LISTENER (
    TOPIC 'users',
    SASL USERNAME '{SASL_USERNAME}',
    SASL PASSWORD SECRET kafka_ingest_password
  )
  KEY FORMAT TEXT VALUE FORMAT JSON
  ENVELOPE UPSERT;

CREATE VIEW page_views_by_user AS
  SELECT u.key AS user_id, u.data->>'name' AS name, count(p.user_id) AS views
  FROM users u LEFT JOIN page_views p ON p.user_id = u.key
  GROUP BY 1, 2;
"""


def sql(conn: psycopg.Connection, query: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(query.encode())
        return cur.fetchall() if cur.description else []


def print_query(conn: psycopg.Connection, query: str) -> None:
    print(f"\n> {query}")
    with conn.cursor() as cur:
        cur.execute(query.encode())
        names = [d.name for d in cur.description or []]
        rows = cur.fetchall()
    widths = [
        max([len(n)] + [len(str(r[i])) for r in rows]) for i, n in enumerate(names)
    ]
    print("  " + " | ".join(n.ljust(w) for n, w in zip(names, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in rows:
        print("  " + " | ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def kafka_endpoint(cluster_id: str, timeout: float = 60, port: str = "kafka") -> str:
    """Finds the address at which the cluster's replica accepts Kafka connections, or
    serves its schema registry with `port="kafka-registry"`.

    Locally, the process orchestrator proxies each replica port through a TCP
    port on localhost and records the proxies in a Prometheus service discovery
    file.
    """
    path = MZ_ROOT / "mzdata" / "prometheus" / "cluster.json"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            for target in json.loads(path.read_text()):
                labels = target["labels"]
                if (
                    labels.get("mz_orchestrator_cluster_id") == cluster_id
                    and labels.get("mz_orchestrator_port") == port
                ):
                    host, number = target["targets"][0].rsplit(":", 1)
                    if host == "0.0.0.0":
                        host = "127.0.0.1"
                    return f"{host}:{number}"
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(0.5)
    raise RuntimeError(
        f"no {port} endpoint for cluster {cluster_id} in {path}; "
        "was the cluster created after enable_replica_kafka_listener was set?"
    )


def produce(producer: Producer, topic: str, records: list[tuple]) -> None:
    errors = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(err)

    started = time.time()
    for key, value, *headers in records:
        producer.produce(
            topic,
            key=key,
            value=None if value is None else json.dumps(value),
            headers=headers[0] if headers else None,
            on_delivery=on_delivery,
        )
    remaining = producer.flush(30)
    elapsed = time.time() - started
    if errors or remaining:
        raise RuntimeError(f"producing to {topic} failed: {errors or remaining}")
    print(
        f"produced {len(records)} records to {topic}, "
        f"all acknowledged as durable in {elapsed:.2f}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6875)
    parser.add_argument("--system-port", type=int, default=6877)
    parser.add_argument("--users", type=int, default=5)
    parser.add_argument("--page-views", type=int, default=1000)
    args = parser.parse_args()

    print("Enabling Kafka listener sources")
    with psycopg.connect(
        host=args.host, port=args.system_port, user="mz_system", autocommit=True
    ) as system:
        sql(system, "ALTER SYSTEM SET enable_kafka_listener_source = true")
        sql(system, "ALTER SYSTEM SET enable_replica_kafka_listener = true")

    conn = psycopg.connect(
        host=args.host, port=args.port, user="materialize", autocommit=True
    )
    print("Creating cluster, sources and view")
    for statement in SETUP.split(";"):
        if statement.strip():
            sql(conn, statement)

    (cluster_id,) = sql(conn, f"SELECT id FROM mz_clusters WHERE name = '{CLUSTER}'")[0]
    bootstrap = kafka_endpoint(cluster_id)
    print(f"Cluster {CLUSTER} ({cluster_id}) accepts Kafka producers at {bootstrap}")

    common = {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "gzip",
    }
    users_producer = Producer(
        {
            **common,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": SASL_USERNAME,
            "sasl.password": SASL_PASSWORD,
        }
    )
    events_producer = Producer(common)

    names = ["ada", "grace", "edsger", "barbara", "ken", "dennis", "frances"]
    users = [
        (f"user-{i}", {"name": names[i % len(names)], "plan": "free"})
        for i in range(args.users)
    ]
    produce(users_producer, "users", users)

    page_views = [
        (
            f"user-{random.randrange(args.users)}",
            {"path": random.choice(["/", "/pricing", "/docs", "/blog"])},
            [("trace-id", f"{i:08x}")],
        )
        for i in range(args.page_views)
    ]
    produce(events_producer, "page_views", page_views)

    # Upsert semantics: update one user and delete another with a tombstone.
    produce(
        users_producer,
        "users",
        [("user-0", {"name": "ada lovelace", "plan": "pro"}), ("user-1", None)],
    )

    # Acknowledged records are durable, but views may lag them by a moment.
    time.sleep(2)

    print_query(conn, "SELECT count(*) FROM page_views")
    print_query(conn, "SELECT * FROM users ORDER BY key")
    print_query(conn, "SELECT * FROM page_views_by_user ORDER BY views DESC")
    print_query(
        conn,
        'SELECT user_id, data, "offset", produced_at, headers '
        'FROM page_views ORDER BY "offset" DESC LIMIT 3',
    )
    print_query(
        conn,
        "SELECT s.name, st.status, st.error FROM mz_sources s "
        "JOIN mz_internal.mz_source_statuses st ON s.id = st.id "
        "WHERE s.name IN ('page_views', 'users')",
    )

    print(f"""
Try it yourself, for example with kcat:

  echo 'user-2:{{"path": "/hello"}}' | kcat -b {bootstrap} -t page_views -P -K:
  echo 'user-9:{{"name": "kcat"}}' | kcat -b {bootstrap} -t users -P -K: \\
      -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN \\
      -X sasl.username={SASL_USERNAME} -X sasl.password={SASL_PASSWORD}

  psql -h {args.host} -p {args.port} -U materialize -c 'SELECT * FROM page_views_by_user'
""")


if __name__ == "__main__":
    main()
