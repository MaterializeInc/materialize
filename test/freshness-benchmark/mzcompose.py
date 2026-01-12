# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Benchmark for measuring Materialize's end-to-end freshness latency.

This test measures the time between when a message is committed to Kafka
and when Materialize reports that message as visible via a SUBSCRIBE query
with progress messages.

The experimental setup:
- One Materialize instance
- One Kafka instance (via Redpanda)
- A single topic "data" with key-value integer messages

The test harness:
1. Generates uniform 100 QPS load (one message every 10ms)
2. Each message has key=0 and value=sequence_number
3. Records commit time for each message
4. SUBSCRIBE to the source with progress messages
5. Records reaction time based on progress messages
6. Calculates latency = reaction_time - commit_time
7. Reports p50, p75, p90, p99, max statistics

The benchmark can run multiple experiments with different numbers of
materialized views and outputs results to a CSV file for further analysis.

When using --mz-url for remote Materialize instances, you must also provide
--remote-kafka-broker pointing to a Kafka broker accessible from both the test
harness and the remote Materialize instance.
"""

import csv
import os
import threading
import time
import uuid
from textwrap import dedent

import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from materialize.mz_env_util import get_cloud_hostname
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PgConnInfo, parse_pg_conn_string

# Use a fixed external port for Kafka so we can connect from outside Docker
KAFKA_EXTERNAL_PORT = 30123

SERVICES = [
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123", "9092"],
        allow_host_ports=True,
        advertised_listeners=[
            f"HOST://127.0.0.1:{KAFKA_EXTERNAL_PORT}",
            "PLAINTEXT://kafka:9092",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Cockroach(setup_materialize=True, in_memory=False),
    PostgresMetadata(),
    Materialized(),
]

def generate_topic_name() -> str:
    """Generate a unique topic name for each test run."""
    return f"freshness-benchmark-{uuid.uuid4().hex[:8]}"


def get_kafka_admin_config(
    kafka_addr: str,
    kafka_username: str | None = None,
    kafka_password: str | None = None,
) -> dict:
    """Build Kafka admin client configuration."""
    config = {"bootstrap.servers": kafka_addr}
    if kafka_username and kafka_password:
        config.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": kafka_username,
            "sasl.password": kafka_password,
        })
    return config


def create_topic(
    kafka_addr: str,
    topic_name: str,
    max_retries: int = 30,
    retry_interval: float = 1.0,
    kafka_username: str | None = None,
    kafka_password: str | None = None,
) -> None:
    """Create the Kafka topic for the benchmark with retry logic."""
    for attempt in range(max_retries):
        try:
            admin_config = get_kafka_admin_config(kafka_addr, kafka_username, kafka_password)
            admin = AdminClient(admin_config)
            topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            futures = admin.create_topics([topic])
            for name, future in futures.items():
                # Wait for the topic creation with a timeout
                future.result(timeout=10)
                print(f"Created topic: {name}")
            return
        except Exception as e:
            error_str = str(e)
            # Check if topic already exists (not an error)
            if "TOPIC_ALREADY_EXISTS" in error_str:
                print(f"Topic {topic_name} already exists")
                return
            # Retry on timeout or broker not available errors
            if attempt < max_retries - 1:
                print(
                    f"Topic creation attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {retry_interval}s..."
                )
                time.sleep(retry_interval)
            else:
                raise RuntimeError(
                    f"Failed to create topic after {max_retries} attempts: {e}"
                )


def delete_topic(
    kafka_addr: str,
    topic_name: str,
    kafka_username: str | None = None,
    kafka_password: str | None = None,
) -> None:
    """Delete a Kafka topic."""
    try:
        admin_config = get_kafka_admin_config(kafka_addr, kafka_username, kafka_password)
        admin = AdminClient(admin_config)
        futures = admin.delete_topics([topic_name])
        for name, future in futures.items():
            future.result(timeout=10)
            print(f"Deleted topic: {name}")
    except Exception as e:
        print(f"Warning: Failed to delete topic {topic_name}: {e}")


def wait_for_source_ready(
    c: Composition,
    source_name: str,
    conn: PgConnInfo | None = None,
    timeout_seconds: int = 120,
    poll_interval: float = 1.0,
) -> None:
    """Wait for a source to reach 'running' status."""
    query = f"""
        SELECT status
        FROM mz_internal.mz_source_statuses
        WHERE name = '{source_name}'
    """

    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise RuntimeError(
                f"Timeout waiting for source '{source_name}' to become ready "
                f"after {timeout_seconds} seconds"
            )

        try:
            if conn:
                pg_conn = conn.connect()
                pg_conn.autocommit = True
                with pg_conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
                pg_conn.close()
            else:
                result = c.sql_query(query)
                row = result[0] if result else None

            if row and row[0] == "running":
                print(f"Source '{source_name}' is running (took {elapsed:.1f}s)")
                return

            status = row[0] if row else "not found"
            print(f"Source status: {status}, waiting...")

        except Exception as e:
            print(f"Error checking source status: {e}, retrying...")

        time.sleep(poll_interval)


def setup_materialize(
    c: Composition,
    num_views: int,
    topic_name: str,
    conn: PgConnInfo | None = None,
    kafka_broker: str = "kafka:9092",
    kafka_username: str | None = None,
    kafka_password: str | None = None,
) -> str:
    """
    Set up the Kafka source and materialized views in Materialize.

    Args:
        c: The composition object
        num_views: Number of chained materialized views to create
        topic_name: Name of the Kafka topic to consume from
        conn: Optional PgConnInfo for remote Materialize connection
        kafka_broker: Kafka broker address for Materialize to connect to
        kafka_username: Optional username for Kafka SASL authentication
        kafka_password: Optional password for Kafka SASL authentication

    Returns:
        The name of the final view/source to subscribe to
    """

    def run_sql(sql: str, user: str = "materialize", port: int | None = None) -> None:
        """Execute SQL against local or remote Materialize."""
        if conn:
            pg_conn = conn.connect()
            pg_conn.autocommit = True
            with pg_conn.cursor() as cur:
                # Materialize only supports single-statement execution,
                # so split on semicolons and execute each separately
                for statement in sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        cur.execute(statement)
            pg_conn.close()
        else:
            c.sql(sql, user=user, port=port)

    # Clean up any existing objects from previous runs
    run_sql(
        dedent(
            """
            DROP CLUSTER IF EXISTS beefy CASCADE;
            DROP CONNECTION IF EXISTS kafka_conn;
            DROP SECRET IF EXISTS kafka_password;
            """
        )
    )

    # Create a beefy cluster for all objects
    run_sql("CREATE CLUSTER beefy SIZE '400cc';")

    # Build SASL options if credentials are provided
    if kafka_username and kafka_password:
        sasl_options = f"""
                SECURITY PROTOCOL SASL_PLAINTEXT,
                SASL MECHANISMS 'SCRAM-SHA-256',
                SASL USERNAME '{kafka_username}',
                SASL PASSWORD SECRET kafka_password"""
        # Create a secret for the password first
        run_sql(f"CREATE SECRET kafka_password AS '{kafka_password}';")
    else:
        sasl_options = "SECURITY PROTOCOL PLAINTEXT"

    run_sql(
        dedent(
            f"""
            CREATE CONNECTION kafka_conn TO KAFKA (
                BROKER '{kafka_broker}',
                {sasl_options}
            );

            CREATE SOURCE data_source
            IN CLUSTER beefy
            FROM KAFKA CONNECTION kafka_conn (
                TOPIC '{topic_name}'
            )
            KEY FORMAT TEXT
            VALUE FORMAT TEXT
            INCLUDE KEY
            ENVELOPE NONE;
            """
        )
    )

    run_sql(f"CREATE MATERIALIZED VIEW max_data IN CLUSTER beefy AS (SELECT key, MAX(text) FROM data_source GROUP BY key);")
    for i in range(2, num_views + 1):
        run_sql(f"CREATE MATERIALIZED VIEW dummy_mv{i} IN CLUSTER beefy AS (SELECT * FROM max_data);")

    run_sql(f"CREATE MATERIALIZED VIEW data_mv1 IN CLUSTER beefy AS (SELECT DISTINCT * FROM data_source);")
    return "data_mv1"


class LatencyStats:
    """Container for latency statistics."""

    def __init__(self, latencies_ms: list[float]):
        if not latencies_ms:
            self.p50 = 0.0
            self.p75 = 0.0
            self.p90 = 0.0
            self.p99 = 0.0
            self.max = 0.0
            self.count = 0
            return

        arr = np.array(latencies_ms)
        self.p50 = float(np.percentile(arr, 50))
        self.p75 = float(np.percentile(arr, 75))
        self.p90 = float(np.percentile(arr, 90))
        self.p99 = float(np.percentile(arr, 99))
        self.max = float(np.max(arr))
        self.count = len(latencies_ms)

    def __str__(self) -> str:
        return dedent(
            f"""
            Latency Statistics (n={self.count}):
              p50:  {self.p50:>8.2f} ms
              p75:  {self.p75:>8.2f} ms
              p90:  {self.p90:>8.2f} ms
              p99:  {self.p99:>8.2f} ms
              max:  {self.max:>8.2f} ms
            """
        ).strip()


class FreshnessBenchmark:
    """
    Benchmark harness for measuring end-to-end freshness latency.

    The harness runs two concurrent activities:
    1. A producer that sends messages to Kafka at a fixed rate
    2. A subscriber that reads progress messages from Materialize

    For each message, we track:
    - commit_time: when Kafka acknowledged the message
    - reaction_time: when Materialize reported the message as visible
    """

    def __init__(
        self,
        c: Composition,
        kafka_addr: str,
        duration_seconds: int,
        subscribe_target: str,
        topic_name: str,
        conn: PgConnInfo | None = None,
        kafka_username: str | None = None,
        kafka_password: str | None = None,
    ):
        self.c = c
        self.kafka_addr = kafka_addr
        self.duration_seconds = duration_seconds
        self.subscribe_target = subscribe_target
        self.topic_name = topic_name
        self.conn = conn  # Remote connection info, if any

        # Shared state protected by lock
        self.lock = threading.Lock()
        self.commit_times: dict[int, float] = {}  # sequence -> commit timestamp
        self.reaction_times: dict[int, float] = {}  # sequence -> reaction timestamp
        self.max_produced_seq = -1
        self.running = True

        # Kafka producer
        producer_config = {"bootstrap.servers": kafka_addr}
        if kafka_username and kafka_password:
            producer_config.update({
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": kafka_username,
                "sasl.password": kafka_password,
            })
        self.producer = Producer(producer_config)

    def _delivery_callback(self, err, msg, seq: int) -> None:
        """Callback invoked when Kafka acknowledges a message."""
        if err is not None:
            print(f"Message delivery failed for seq {seq}: {err}")
            return

        commit_time = time.time()
        with self.lock:
            self.commit_times[seq] = commit_time

    def _producer_thread(self) -> None:
        """
        Producer thread: sends messages at 100 QPS (every 10ms).
        Each message has key=0 and value=sequence_number.
        """
        seq = 0
        interval = 0.001  # 10ms = 100 QPS
        start_time = time.time()
        end_time = start_time + self.duration_seconds

        while time.time() < end_time and self.running:
            target_time = start_time + seq * interval

            # Send the message
            key = "0"
            value = str(seq)

            # Capture seq in closure for callback
            current_seq = seq
            self.producer.produce(
                topic=self.topic_name,
                key=key.encode(),
                value=value.encode(),
                callback=lambda err, msg, s=current_seq: self._delivery_callback(
                    err, msg, s
                ),
            )

            # Poll to trigger delivery callbacks
            self.producer.poll(0)

            with self.lock:
                self.max_produced_seq = seq

            seq += 1

            # Sleep until next target time
            sleep_time = target_time + interval - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Flush remaining messages
        self.producer.flush(timeout=10)
        print(f"Producer finished: sent {seq} messages")

    def _subscriber_thread(self) -> None:
        """
        Subscriber thread: reads SUBSCRIBE with progress messages.
        When a progress message is received, it marks the reaction time
        for all messages seen since the last progress message.
        """
        # Get a fresh connection for the subscriber
        # Need to disable autocommit for SUBSCRIBE with cursor
        if self.conn:
            conn = self.conn.connect()
        else:
            conn = self.c.sql_connection()
        conn.autocommit = False  # Override default for SUBSCRIBE transaction

        cursor = conn.cursor()

        # Start a transaction and declare a cursor for SUBSCRIBE
        cursor.execute("BEGIN")
        cursor.execute("SET CLUSTER = beefy")
        cursor.execute(
            f"DECLARE sub CURSOR FOR SUBSCRIBE (SELECT * FROM {self.subscribe_target}) WITH (PROGRESS)"
        )

        # Track sequences seen since last progress message
        pending_seqs: set[int] = set()
        max_reacted_seq = -1

        while self.running:
            # Fetch available rows with a short timeout
            cursor.execute("FETCH ALL sub WITH (timeout='100ms')")
            rows = cursor.fetchall()

            for row in rows:
                # Row format: (mz_timestamp, mz_progressed, mz_diff, key, text)
                # With PROGRESS, mz_progressed indicates if this is a progress message
                # For TEXT format sources, columns are named "key" and "text"
                mz_timestamp, mz_progressed, mz_diff, key, text = row

                if mz_progressed:
                    # Progress message: mark reaction time for all pending sequences
                    reaction_time = time.time()
                    with self.lock:
                        for seq in pending_seqs:
                            if seq in self.commit_times and seq not in self.reaction_times:
                                self.reaction_times[seq] = reaction_time
                    if pending_seqs:
                        max_reacted_seq = max(max_reacted_seq, max(pending_seqs))
                    pending_seqs.clear()
                else:
                    # Data message: parse sequence number from value (stored in "text" column)
                    # With UPSERT, we see mz_diff=1 for inserts/updates
                    if text is not None and mz_diff == 1:
                        try:
                            seq = int(text)
                            pending_seqs.add(seq)
                        except ValueError:
                            pass

        cursor.execute("COMMIT")
        cursor.close()
        conn.close()
        print(f"Subscriber finished: processed up to seq {max_reacted_seq}")

    def run(self) -> tuple[LatencyStats, list[float]]:
        """Run the benchmark and return latency statistics and raw latencies."""
        print(f"Starting benchmark for {self.duration_seconds} seconds...")

        producer_thread = threading.Thread(target=self._producer_thread)
        subscriber_thread = threading.Thread(target=self._subscriber_thread)

        producer_thread.start()
        subscriber_thread.start()

        # Wait for producer to finish
        producer_thread.join()

        # Give subscriber a bit more time to catch up
        time.sleep(2)

        # Stop the subscriber
        self.running = False
        subscriber_thread.join()

        # Calculate latencies
        latencies_ms = []
        with self.lock:
            for seq, commit_time in self.commit_times.items():
                if seq in self.reaction_times:
                    latency_ms = (self.reaction_times[seq] - commit_time) * 1000
                    latencies_ms.append(latency_ms)

        print(f"Collected {len(latencies_ms)} latency measurements")
        return LatencyStats(latencies_ms), latencies_ms


def parse_view_counts(view_counts_str: str) -> list[int]:
    """Parse a comma-separated list of view counts (must be >= 1)."""
    counts = []
    for part in view_counts_str.split(","):
        part = part.strip()
        if "-" in part:
            # Handle ranges like "1-5"
            start, end = part.split("-", 1)
            counts.extend(range(int(start.strip()), int(end.strip()) + 1))
        else:
            counts.append(int(part))

    # Filter and validate: require at least 1 materialized view
    counts = sorted(set(counts))
    if any(c < 1 for c in counts):
        raise ValueError("View counts must be >= 1 (at least one materialized view is required)")
    return counts


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration of the benchmark in seconds (default: 60)",
    )

    parser.add_argument(
        "--tick-interval",
        type=str,
        default="1s",
        help="Source ticking rate / metadata fetch interval (default: 1s, e.g., 100ms, 500ms, 1s)",
    )

    parser.add_argument(
        "--view-counts",
        type=str,
        default="5",
        help="Comma-separated list of view counts to test (minimum 1), supports ranges (default: 5, e.g., '1,3,5' or '1-5' or '1,2,5,10')",
    )

    parser.add_argument(
        "--output-csv",
        type=str,
        default="freshness_results.csv",
        help="Path to the output CSV file for latency measurements (default: freshness_results.csv)",
    )

    parser.add_argument(
        "--consensus-backend",
        type=str,
        choices=["postgres", "cockroach"],
        default="postgres",
        help="Consensus backend to use: 'postgres' (default) or 'cockroach'",
    )

    parser.add_argument(
        "--mz-url",
        type=str,
        help="Remote Materialize instance to run against (e.g., postgres://user:pass@host:6875/materialize)",
    )

    parser.add_argument(
        "--remote-kafka-broker",
        type=str,
        help="Kafka broker address accessible from remote Materialize (required with --mz-url)",
    )

    parser.add_argument(
        "--kafka-username",
        type=str,
        help="Username for Kafka SASL authentication",
    )

    parser.add_argument(
        "--kafka-password",
        type=str,
        help="Password for Kafka SASL authentication",
    )

    args = parser.parse_args()

    view_counts = parse_view_counts(args.view_counts)
    print(f"Will run benchmarks for view counts: {view_counts}")

    # Determine if we're using a remote Materialize instance
    remote_conn: PgConnInfo | None = None

    if args.mz_url:
        assert args.remote_kafka_broker, "--remote-kafka-broker is required with --mz-url"
        remote_conn = parse_pg_conn_string(args.mz_url)
        print(f"Using remote Materialize: {remote_conn.host}:{remote_conn.port}")

    # Determine Kafka addresses
    if remote_conn:
        # For remote Mz, we use the same Kafka broker for both producing and Mz consumption
        kafka_addr = args.remote_kafka_broker
        mz_kafka_broker = args.remote_kafka_broker
    else:
        # For local Mz, use local Kafka with fixed external port for producing
        kafka_addr = f"127.0.0.1:{KAFKA_EXTERNAL_PORT}"
        mz_kafka_broker = "kafka:9092"  # Docker network address for Mz

    # Determine consensus backend configuration (only used for local Mz)
    if args.consensus_backend == "cockroach":
        metadata_store = "cockroach"
    else:
        metadata_store = "postgres-metadata"

    if not remote_conn:
        print(f"Using consensus backend: {args.consensus_backend} (metadata_store={metadata_store})")

    # Collect all results for CSV output
    all_results: list[dict] = []

    for num_views in view_counts:
        # Generate a unique topic name for this experiment
        topic_name = generate_topic_name()

        print("\n" + "=" * 60)
        print(f"EXPERIMENT: {num_views} materialized view(s)")
        print(f"Topic: {topic_name}")
        print("=" * 60)

        if remote_conn:
            # For remote Materialize, we don't need to start local services
            # Just create the topic on the remote Kafka
            print(f"Kafka address: {kafka_addr}")
            print("--- Creating Kafka topic")
            create_topic(kafka_addr, topic_name, kafka_username=args.kafka_username, kafka_password=args.kafka_password)
        else:
            # Start fresh for each experiment
            print("--- Starting services")
            c.down(destroy_volumes=True)

            with c.override(
                Materialized(
                    external_metadata_store=True,
                    metadata_store=metadata_store,
                )
            ):
                c.up("zookeeper", "kafka", "schema-registry", metadata_store, "materialized")

            print(f"Kafka address: {kafka_addr}")

            # Create topic (with retry logic for Redpanda startup)
            print("--- Creating Kafka topic")
            create_topic(kafka_addr, topic_name, kafka_username=args.kafka_username, kafka_password=args.kafka_password)

            # Configure source ticking rate
            print(f"--- Setting kafka_default_metadata_fetch_interval to {args.tick_interval}")
            c.sql(
                f"ALTER SYSTEM SET kafka_default_metadata_fetch_interval = '{args.tick_interval}'",
                user="mz_system",
                port=6877,
            )

        # Set up Materialize with the specified number of views
        print(f"--- Setting up Materialize source with {num_views} materialized view(s)")
        subscribe_target = setup_materialize(
            c,
            num_views,
            topic_name=topic_name,
            conn=remote_conn,
            kafka_broker=mz_kafka_broker,
            kafka_username=args.kafka_username,
            kafka_password=args.kafka_password,
        )
        print(f"--- Will subscribe to: {subscribe_target}")

        # Wait for the source to be ready
        print("--- Waiting for source to be ready")
        wait_for_source_ready(c, "data_source", conn=remote_conn)

        # Run the benchmark
        print(f"--- Running freshness benchmark for {args.duration} seconds")
        benchmark = FreshnessBenchmark(
            c,
            kafka_addr,
            args.duration,
            subscribe_target,
            topic_name=topic_name,
            conn=remote_conn,
            kafka_username=args.kafka_username,
            kafka_password=args.kafka_password,
        )
        stats, latencies_ms = benchmark.run()

        print("\n" + "-" * 50)
        print(f"RESULTS FOR {num_views} VIEW(S)")
        print("-" * 50)
        print(stats)
        print("-" * 50)

        # Clean up the topic
        print(f"--- Cleaning up topic: {topic_name}")
        delete_topic(kafka_addr, topic_name, kafka_username=args.kafka_username, kafka_password=args.kafka_password)

        # Collect results for CSV
        for latency in latencies_ms:
            all_results.append(
                {
                    "num_views": num_views,
                    "latency_ms": latency,
                }
            )

    # Write results to CSV
    output_path = args.output_csv
    if not os.path.isabs(output_path):
        # Make path relative to the test directory
        output_path = os.path.join(os.path.dirname(__file__), output_path)

    print(f"\n--- Writing {len(all_results)} measurements to {output_path}")
    with open(output_path, "w", newline="") as csvfile:
        fieldnames = ["num_views", "latency_ms"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # Print summary table
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Views':>6} | {'p50':>10} | {'p75':>10} | {'p90':>10} | {'p99':>10} | {'max':>10}")
    print("-" * 70)

    for num_views in view_counts:
        view_latencies = [r["latency_ms"] for r in all_results if r["num_views"] == num_views]
        if view_latencies:
            view_stats = LatencyStats(view_latencies)
            print(
                f"{num_views:>6} | {view_stats.p50:>10.2f} | {view_stats.p75:>10.2f} | "
                f"{view_stats.p90:>10.2f} | {view_stats.p99:>10.2f} | {view_stats.max:>10.2f}"
            )

    print("=" * 60)
    print(f"Results written to: {output_path}")

