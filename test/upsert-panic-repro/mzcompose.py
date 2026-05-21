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
from materialize.mzcompose.services.zookeeper import Zookeeper

# Match production customer config flags
SYSTEM_PARAMS = {
    **get_default_system_parameters(),
    "max_sources": "10000",
    "max_tables": "10000",
    "max_objects_per_schema": "10000",
    "max_replicas_per_cluster": "10",
    # Must be true for the continual feedback upsert code path
    "storage_use_continual_feedback_upsert": "true",
    # Merge operator — required for diff_sum panic
    "storage_rocksdb_use_merge_operator": "true",
    # Match customer: backpressure enabled for ALL sources
    "storage_dataflow_max_inflight_bytes": "268435456",  # 256MiB
    "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction": "0.01",  # 1%
    "storage_dataflow_max_inflight_bytes_disk_only": "false",
    # Multi-replica sources — key for the bug
    "enable_multi_replica_sources": "true",
}

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Cockroach(setup_materialize=True, in_memory=True),
    Minio(setup_materialize=True),
    Materialized(
        sanity_restart=False,
        system_parameter_defaults=SYSTEM_PARAMS,
        metadata_store="cockroach",
        default_replication_factor=2,
    ),
    Testdrive(
        no_consistency_checks=True,
        no_reset=True,
        seed=1,
        default_timeout="600s",
    ),
]

PAD = "x" * 5000

KEY_SCHEMA = (
    '{"type": "record", "name": "Key", "fields": [{"name": "k", "type": "long"}]}'
)
VAL_SCHEMA = (
    '{"type": "record", "name": "Value", "fields": ['
    '{"name": "f_ts", "type": "long"},'
    '{"name": "f_str", "type": "string"}'
    "]}"
)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", type=int, default=3600)
    parser.add_argument("--num-sources", type=int, default=3)
    parser.add_argument("--num-keys", type=int, default=500)
    parser.add_argument("--num-hot-keys", type=int, default=500)
    parser.add_argument("--parallelism", type=int, default=5)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--deploy-cycle", type=int, default=10)
    args = parser.parse_args()

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)
    random.seed(seed)
    print(f"Seed: {seed}")

    n = args.num_sources
    end_time = datetime.now() + timedelta(seconds=args.runtime)

    partitions = [random.choice([8, 16, 32]) for _ in range(n)]
    workers = 16
    replication_factor = 2
    num_pump_threads = 8

    print(
        f"Config: {n} sources, workers={workers}, rf={replication_factor}, "
        f"pump_threads={num_pump_threads}"
    )

    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "cockroach", "minio")

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
            f'{{"f_ts": 0, "f_str": "seed"}}'
            "\n",
        )

    print(f"Creating {n} upsert sources...")
    for i in range(n):
        topic = f"testdrive-s-{i}-${{testdrive.seed}}"
        c.testdrive(
            f"""\
> CREATE SOURCE s_{i}_tbl
  IN CLUSTER upsert_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
"""
        )
    print(f"  {n}/{n}.")

    # Create temporal MVs with mz_now()
    print("Creating temporal materialized views...")
    for i in range(n):
        c.testdrive(
            f"""\
> CREATE MATERIALIZED VIEW mv_recent_{i} IN CLUSTER upsert_cluster
  AS SELECT COUNT(*) AS cnt FROM s_{i}_tbl
  WHERE mz_now() < f_ts::numeric + 30000;

> CREATE MATERIALIZED VIEW mv_window_{i} IN CLUSTER upsert_cluster
  AS SELECT COUNT(*) AS cnt FROM s_{i}_tbl
  WHERE mz_now() < f_ts::numeric + 10000;

> CREATE DEFAULT INDEX IN CLUSTER upsert_cluster ON s_{i}_tbl;
"""
        )
    print(f"  {n * 3} temporal objects created.")

    # Pre-populate
    ts_ms = int(time.time() * 1000)
    print(f"Pre-populating {args.num_keys} keys per source...")
    for i in range(n):
        c.testdrive(
            f"$ kafka-ingest format=avro topic=s-{i}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}"
            f" repeat={args.num_keys}\n"
            f'{{"k": ${{kafka-ingest.iteration}}}} '
            f'{{"f_ts": {ts_ms}, '
            f'"f_str": "init{PAD}"}}'
            "\n",
        )
    time.sleep(30)
    print("Pre-population done.")

    # --- Helpers ---
    def ingest_parallel(sources, gen_line):
        with ThreadPoolExecutor(
            max_workers=min(len(sources), args.parallelism)
        ) as pool:
            futs = {pool.submit(c.testdrive, gen_line(s)): s for s in sources}
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception:
                    pass

    def ingest_one(src_id, nk, tag, round_num):
        ts_ms = int(time.time() * 1000)
        return (
            f"$ kafka-ingest format=avro topic=s-{src_id}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}"
            f" repeat={nk}\n"
            f'{{"k": ${{kafka-ingest.iteration}}}} '
            f'{{"f_ts": {ts_ms}, '
            f'"f_str": "{tag}{PAD}"}}'
            "\n"
        )

    # --- Background pump ---
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

    # --- Main loop ---
    cycle = 0
    deploys = 0
    all_srcs = list(range(n))

    while datetime.now() < end_time:
        cycle += 1
        nk = random.randint(1, args.num_hot_keys)

        t0 = time.monotonic()

        # ============================================================
        # Every N cycles: 0dt deploy (restart materialized)
        # Both old and new instances run simultaneously during the
        # transition — the new instance rehydrates while the old one
        # continues writing. This is the production scenario.
        # ============================================================
        if cycle % args.deploy_cycle == 0:
            deploys += 1
            print(f"\n  >>> RESTART (#{deploys}) <<<")

            # Kill materialized — simulates 0dt deploy restart.
            # The clusterd processes restart and rehydrate from persist
            # while new Kafka data keeps flowing.
            c.kill("materialized")
            time.sleep(2)
            c.up("materialized")
            # Wait for MZ to be healthy
            time.sleep(15)

            # Blast data during rehydration
            for rnd_i in range(5):
                ingest_parallel(
                    all_srcs,
                    lambda s: ingest_one(
                        s, nk, f"deploy{deploys}r{rnd_i}",
                        deploys * 1000 + rnd_i,
                    ),
                )
                time.sleep(random.uniform(1.0, 1.5))

            t7 = time.monotonic()
            print(f"  >>> Deploy done ({t7-t0:.1f}s) <<<\n")
            continue

        # ============================================================
        # Normal data injection
        # ============================================================
        ingest_parallel(
            all_srcs,
            lambda s: ingest_one(s, nk, f"c{cycle}", cycle * 100),
        )

        t7 = time.monotonic()
        print(
            f"  Cycle {cycle} ({nk}keys x {n}srcs): "
            f"total={t7-t0:.1f}s"
        )

    pump_stop = True
    for t in pump_threads:
        t.join(timeout=10)

    print(f"Done. {cycle} cycles, {deploys} deploys. Seed: {seed}.")
