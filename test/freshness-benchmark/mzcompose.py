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
"""

import threading
import time
from textwrap import dedent

import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper

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
    Materialized(),
]

TOPIC_NAME = "data"


def create_topic(kafka_addr: str, max_retries: int = 30, retry_interval: float = 1.0) -> None:
    """Create the Kafka topic for the benchmark with retry logic."""
    for attempt in range(max_retries):
        try:
            admin = AdminClient({"bootstrap.servers": kafka_addr})
            topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
            futures = admin.create_topics([topic])
            for topic_name, future in futures.items():
                # Wait for the topic creation with a timeout
                future.result(timeout=10)
                print(f"Created topic: {topic_name}")
            return
        except Exception as e:
            error_str = str(e)
            # Check if topic already exists (not an error)
            if "TOPIC_ALREADY_EXISTS" in error_str:
                print(f"Topic {TOPIC_NAME} already exists")
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


def setup_materialize(c: Composition) -> None:
    """Set up the Kafka source in Materialize."""
    c.sql(
        dedent(
            """
            CREATE CONNECTION kafka_conn TO KAFKA (
                BROKER 'kafka:9092',
                SECURITY PROTOCOL PLAINTEXT
            );

            CREATE SOURCE data_source
            FROM KAFKA CONNECTION kafka_conn (
                TOPIC 'data'
            )
            KEY FORMAT TEXT
            VALUE FORMAT TEXT
            INCLUDE KEY
            ENVELOPE NONE;

            CREATE MATERIALIZED VIEW data_mv1 AS (SELECT DISTINCT * FROM data_source);
            CREATE MATERIALIZED VIEW data_mv2 AS (SELECT DISTINCT * FROM data_mv1);
            CREATE MATERIALIZED VIEW data_mv3 AS (SELECT DISTINCT * FROM data_mv2);
            CREATE MATERIALIZED VIEW data_mv4 AS (SELECT DISTINCT * FROM data_mv3);
            CREATE MATERIALIZED VIEW data_mv5 AS (SELECT DISTINCT * FROM data_mv4);
            """
        )
    )


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

    def __init__(self, c: Composition, kafka_addr: str, duration_seconds: int):
        self.c = c
        self.kafka_addr = kafka_addr
        self.duration_seconds = duration_seconds

        # Shared state protected by lock
        self.lock = threading.Lock()
        self.commit_times: dict[int, float] = {}  # sequence -> commit timestamp
        self.reaction_times: dict[int, float] = {}  # sequence -> reaction timestamp
        self.max_produced_seq = -1
        self.running = True

        # Kafka producer
        self.producer = Producer({"bootstrap.servers": kafka_addr})

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
                topic=TOPIC_NAME,
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
        conn = self.c.sql_connection()
        conn.autocommit = False  # Override default for SUBSCRIBE transaction

        cursor = conn.cursor()

        # Start a transaction and declare a cursor for SUBSCRIBE
        cursor.execute("BEGIN")
        cursor.execute(
            "DECLARE sub CURSOR FOR SUBSCRIBE (SELECT * FROM data_mv5) WITH (PROGRESS)"
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

    def run(self) -> LatencyStats:
        """Run the benchmark and return latency statistics."""
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
        return LatencyStats(latencies_ms)


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

    args = parser.parse_args()

    print("--- Starting services")
    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "materialized")

    # Use the fixed external port for Kafka
    kafka_addr = f"127.0.0.1:{KAFKA_EXTERNAL_PORT}"
    print(f"Kafka address: {kafka_addr}")

    # Create topic (with retry logic for Redpanda startup)
    print("--- Creating Kafka topic")
    create_topic(kafka_addr)

    # Configure source ticking rate
    print(f"--- Setting kafka_default_metadata_fetch_interval to {args.tick_interval}")
    c.sql(
        f"ALTER SYSTEM SET kafka_default_metadata_fetch_interval = '{args.tick_interval}'",
        user="mz_system",
        port=6877,
    )

    # Set up Materialize
    print("--- Setting up Materialize source")
    setup_materialize(c)

    # Wait for the source to be ready
    print("--- Waiting for source to be ready")
    time.sleep(5)

    # Run the benchmark
    print(f"--- Running freshness benchmark for {args.duration} seconds")
    benchmark = FreshnessBenchmark(c, kafka_addr, args.duration)
    stats = benchmark.run()

    print("\n" + "=" * 50)
    print("BENCHMARK RESULTS")
    print("=" * 50)
    print(stats)
    print("=" * 50)

