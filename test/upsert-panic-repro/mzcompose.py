# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Thread

import requests

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio, minio_blob_uri
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper

SYSTEM_PARAMS = {
    **get_default_system_parameters(),
    "max_sources": "10000",
    "max_tables": "10000",
    "max_objects_per_schema": "10000",
    "storage_rocksdb_use_merge_operator": "true",
    "storage_upsert_prevent_snapshot_buffering": "true",
    "storage_upsert_max_snapshot_batch_buffering": "10000",
    "storage_dataflow_max_inflight_bytes": "1048576",
    "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction": "0.001",
    "storage_dataflow_max_inflight_bytes_disk_only": "false",
    "max_replicas_per_cluster": "10",
}

MZ_ENV_EXTRA = [
    "MZ_PERSIST_COMPACTION_DISABLED=true",
    # thread::sleep at key points — simulates OS scheduler starvation
    # on an overloaded system:
    "FAILPOINTS="
    "upsert_post_persist_progress_delay=50%sleep(300);"
    "upsert_pre_consolidate_delay=50%sleep(300);"
    "upsert_pre_drain_delay=50%sleep(300)",
]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Cockroach(setup_materialize=True, in_memory=True),
    Minio(setup_materialize=True),
    Toxiproxy(),
    Materialized(
        sanity_restart=False,
        system_parameter_defaults=SYSTEM_PARAMS,
        persist_blob_url=minio_blob_uri("toxiproxy"),
        external_metadata_store="toxiproxy",
        metadata_store="cockroach",
        default_replication_factor=2,
        environment_extra=MZ_ENV_EXTRA,
    ),
    Testdrive(
        no_consistency_checks=True,
        no_reset=True,
        seed=1,
        default_timeout="600s",
    ),
]

# Large values (~5KB) make persist batch parts big, so blob writes are
# naturally slow — widens the window between AtTime drain and feedback.
PAD = "x" * 5000

KEY_SCHEMA = (
    '{"type": "record", "name": "Key", "fields": [{"name": "k", "type": "long"}]}'
)
VAL_SCHEMA = (
    '{"type": "record", "name": "Value", "fields": ['
    '{"name": "f_int", "type": "int"},'
    '{"name": "f_long", "type": "long"},'
    '{"name": "f_str", "type": "string"},'
    '{"name": "f_double", "type": "double"},'
    '{"name": "f_bool", "type": "boolean"},'
    '{"name": "f_nullable", "type": ["null", "string"], "default": null}'
    "]}"
)


def setup_toxiproxy_proxies(c):
    """Create toxiproxy proxies for minio and cockroach."""
    port = c.default_port("toxiproxy")
    toxi_url = f"http://localhost:{port}"

    r = requests.post(
        f"{toxi_url}/proxies",
        json={
            "name": "minio",
            "listen": "0.0.0.0:9000",
            "upstream": "minio:9000",
            "enabled": True,
        },
    )
    assert r.status_code == 201, f"Failed to create minio proxy: {r.text}"

    r = requests.post(
        f"{toxi_url}/proxies",
        json={
            "name": "consensus",
            "listen": "0.0.0.0:26257",
            "upstream": "cockroach:26257",
            "enabled": True,
        },
    )
    assert r.status_code == 201, f"Failed to create consensus proxy: {r.text}"

    # Permanent jitter on consensus — mimics production network variability.
    # Low base latency (50ms) with high jitter (200ms) means most operations
    # are fast but some randomly take 50-250ms.  This occasionally delays
    # persist CAS (compare_and_append) without consistently stalling reads.
    r = requests.post(
        f"{toxi_url}/proxies/consensus/toxics",
        json={
            "name": "consensus-jitter",
            "type": "latency",
            "attributes": {"latency": 50, "jitter": 200},
        },
    )
    assert r.status_code == 200, f"Failed to add consensus-jitter: {r.text}"
    print("Toxiproxy proxies created (consensus jitter=50±200ms)")


def set_toxiproxy_blob_write_throttle(c, enabled, bandwidth_kb=50, latency_ms=500):
    """Throttle blob WRITES (upstream) while keeping reads (downstream) fast.

    This creates the key asymmetry for the bug:
    - persist_source reads are fast -> persist_upper advances -> AtTime drains fire
    - persist_sink writes are slow -> our output is delayed reaching the shard
    - Gap between drain and feedback = orphaned +1 window
    """
    port = c.default_port("toxiproxy")
    toxi_url = f"http://localhost:{port}"

    if not enabled:
        requests.delete(f"{toxi_url}/proxies/minio/toxics/blob-write-bw")
        requests.delete(f"{toxi_url}/proxies/minio/toxics/blob-write-latency")
        return

    # Upstream bandwidth limit — throttles large PUT bodies (batch parts)
    # while tiny GET requests (<1KB) pass through almost instantly.
    r = requests.patch(
        f"{toxi_url}/proxies/minio/toxics/blob-write-bw",
        json={"attributes": {"rate": bandwidth_kb}},
    )
    if r.status_code == 404:
        r = requests.post(
            f"{toxi_url}/proxies/minio/toxics",
            json={
                "name": "blob-write-bw",
                "type": "bandwidth",
                "stream": "upstream",
                "attributes": {"rate": bandwidth_kb},
            },
        )
        assert r.status_code == 200, f"Failed to add blob-write-bw: {r.text}"

    # Upstream latency — adds per-chunk delay on writes.
    r = requests.patch(
        f"{toxi_url}/proxies/minio/toxics/blob-write-latency",
        json={"attributes": {"latency": latency_ms, "jitter": latency_ms // 2}},
    )
    if r.status_code == 404:
        r = requests.post(
            f"{toxi_url}/proxies/minio/toxics",
            json={
                "name": "blob-write-latency",
                "type": "latency",
                "stream": "upstream",
                "attributes": {"latency": latency_ms, "jitter": latency_ms // 2},
            },
        )
        assert r.status_code == 200, f"Failed to add blob-write-latency: {r.text}"


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", type=int, default=900)
    # Fewer sources = more pressure per source = slower feedback per source
    parser.add_argument("--num-sources", type=int, default=3)
    parser.add_argument("--num-keys", type=int, default=500000)
    parser.add_argument("--num-hot-keys", type=int, default=10000)
    parser.add_argument("--parallelism", type=int, default=50)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)
    random.seed(seed)
    print(f"Seed: {seed}")

    n = args.num_sources
    end_time = datetime.now() + timedelta(seconds=args.runtime)

    # Per-source randomized partition counts (1..32)
    partitions = [random.choice([1, 2, 4, 8, 16, 32]) for _ in range(n)]
    # Randomized cluster size and replication factor
    workers = random.choice([1, 2, 4, 8, 16])
    replication_factor = random.choice([1, 2, 4, 8])
    # Randomize number of background pump threads
    num_pump_threads = random.choice([1, 2, 4, 8])

    print(
        f"Config: {n} sources, workers={workers}, rf={replication_factor}, pump_threads={num_pump_threads}"
    )
    print(f"Partition counts: {partitions}")

    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "cockroach", "minio", "toxiproxy")
    setup_toxiproxy_proxies(c)
    c.up("materialized", Service("testdrive", idle=True))

    c.testdrive(
        f"""\
> CREATE CLUSTER upsert_cluster SIZE 'scale=1,workers={workers}', REPLICATION FACTOR {replication_factor};
> SET CLUSTER = upsert_cluster;

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT;

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
""",
    )

    print(f"Creating {n} topics...")
    for i in range(n):
        c.testdrive(f"$ kafka-create-topic topic=s-{i} partitions={partitions[i]}\n")

    print(f"Registering schemas on {n} topics...")
    for i in range(n):
        c.testdrive(
            f"$ kafka-ingest format=avro topic=s-{i}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}\n"
            f'{{"k": 0}} '
            f'{{"f_int": 0, "f_long": 0, "f_str": "seed", "f_double": 0.0,'
            f' "f_bool": true, "f_nullable": null}}'
            "\n",
        )

    print(f"Creating {n} upsert sources...")
    for i in range(n):
        topic = f"testdrive-s-{i}-${{testdrive.seed}}"
        c.testdrive(
            f"""\
> CREATE SOURCE s_{i}
  IN CLUSTER upsert_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> CREATE TABLE s_{i}_tbl
  FROM SOURCE s_{i} (REFERENCE "{topic}");
"""
        )
    print(f"  {n}/{n}.")

    # Pre-populate all keys so the persist shard has meaningful size
    print(f"Pre-populating {args.num_keys} keys per source...")
    for i in range(n):
        c.testdrive(
            f"$ kafka-ingest format=avro topic=s-{i}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}"
            f" repeat={args.num_keys}\n"
            f'{{"k": ${{kafka-ingest.iteration}}}} '
            f'{{"f_int": ${{kafka-ingest.iteration}}, '
            f'"f_long": 0, '
            f'"f_str": "init{PAD}", '
            f'"f_double": 0.0, '
            f'"f_bool": true, '
            f'"f_nullable": null}}'
            "\n",
        )
    # Wait for data to be ingested and persisted
    time.sleep(30)
    print("Pre-population done.")

    set_toxiproxy_blob_write_throttle(c, enabled=False)

    # --- Helper: parallel testdrive across sources ---
    def ingest_parallel(sources, gen_line):
        """Run kafka-ingest on multiple sources in parallel via threads."""
        with ThreadPoolExecutor(
            max_workers=min(len(sources), args.parallelism)
        ) as pool:
            futs = {pool.submit(c.testdrive, gen_line(s)): s for s in sources}
            for f in as_completed(futs):
                f.result()

    def ingest_one(src_id, nk, tag, round_num):
        """Generate a testdrive snippet for one source."""
        return (
            f"$ kafka-ingest format=avro topic=s-{src_id}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}"
            f" repeat={nk}\n"
            f'{{"k": ${{kafka-ingest.iteration}}}} '
            f'{{"f_int": ${{kafka-ingest.iteration}}, '
            f'"f_long": {round_num}, '
            f'"f_str": "{tag}{PAD}", '
            f'"f_double": 0.0, '
            f'"f_bool": true, '
            f'"f_nullable": null}}'
            "\n"
        )

    def tombstone_one(src_id, nk):
        return (
            f"$ kafka-ingest format=avro topic=s-{src_id}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}"
            f" repeat={nk}\n"
            f'{{"k": ${{kafka-ingest.iteration}}}}'
            "\n"
        )

    # --- Background pump: continuously update random keys on all sources ---
    pump_stop = False

    def background_pump(thread_id):
        rnd = random.Random(seed + thread_id * 1000 + 7)
        round_num = 0
        while not pump_stop:
            round_num += 1
            sid = rnd.randint(0, n - 1)
            nk = rnd.randint(1, args.num_hot_keys)
            try:
                c.testdrive(
                    ingest_one(sid, nk, f"bg{thread_id}r{round_num}", round_num)
                )
            except Exception:
                time.sleep(1)

    pump_threads = []
    for tid in range(num_pump_threads):
        t = Thread(target=background_pump, args=(tid,), daemon=True)
        t.start()
        pump_threads.append(t)
    print(f"Background pump started ({num_pump_threads} threads).")

    cycle = 0
    all_srcs = list(range(n))

    while datetime.now() < end_time:
        cycle += 1
        # Randomize per-cycle: hot key count, which sources, timing
        nk = random.randint(1, args.num_hot_keys)
        nsrc = random.randint(1, n)
        srcs = random.sample(all_srcs, nsrc) if nsrc < n else all_srcs

        t0 = time.monotonic()

        # Step 1: Tombstone hot keys (parallel)
        ingest_parallel(srcs, lambda s: tombstone_one(s, nk))
        t1 = time.monotonic()

        # Step 2: Brief wait for tombstones to persist
        time.sleep(random.uniform(0.1, 1.0))

        # Step 3: Throttle blob writes — reads stay fast
        bw_kb = random.randint(10, 200)
        lat_ms = random.randint(100, 2000)
        set_toxiproxy_blob_write_throttle(
            c, enabled=True, bandwidth_kb=bw_kb, latency_ms=lat_ms
        )

        # Step 4: Re-insert at T1 (parallel)
        ingest_parallel(
            srcs,
            lambda s: ingest_one(s, nk, f"c{cycle}a", cycle * 100),
        )
        t4 = time.monotonic()

        # Step 5: Cross reclocking boundary — randomized wait
        time.sleep(random.uniform(1.0, 3.0))

        # Step 6: Re-insert at T2 (parallel)
        ingest_parallel(
            srcs,
            lambda s: ingest_one(s, nk, f"c{cycle}b", cycle * 100 + 1),
        )
        t6 = time.monotonic()

        # Step 7: Drop throttle
        set_toxiproxy_blob_write_throttle(c, enabled=False)
        time.sleep(random.uniform(0.1, 0.5))
        t7 = time.monotonic()

        print(
            f"  Cycle {cycle} ({nsrc}srcs {nk}keys bw={bw_kb}KB/s lat={lat_ms}ms): "
            f"tomb={t1-t0:.1f}s T1={t4-t1:.1f}s T2={t6-t4:.1f}s "
            f"total={t7-t0:.1f}s"
        )

    pump_stop = True
    for t in pump_threads:
        t.join(timeout=10)

    print(f"Done. {cycle} cycles. Seed: {seed}.")
