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
}

MZ_ENV_EXTRA = [
    "MZ_PERSIST_COMPACTION_DISABLED=false",
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
        # Route persist blob and consensus through toxiproxy for latency injection
        persist_blob_url=minio_blob_uri("toxiproxy"),
        external_metadata_store="toxiproxy",
        metadata_store="cockroach",
        default_replication_factor=1,
        environment_extra=MZ_ENV_EXTRA,
    ),
    Testdrive(
        no_consistency_checks=True,
        no_reset=True,
        seed=1,
        default_timeout="600s",
    ),
]

# Padding to make values ~500 bytes (closer to production 374-byte values)
PAD = "x" * 400

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
    """Create toxiproxy proxies for minio and postgres (no latency yet)."""
    port = c.default_port("toxiproxy")
    toxi_url = f"http://localhost:{port}"

    # Proxy for minio (persist blob storage) - listen on 9000
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

    # Proxy for postgres-metadata (persist consensus) - listen on 26257
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
    print("Toxiproxy proxies created (no latency yet)")


def add_toxiproxy_latency(c):
    """Add latency to persist storage after materialized is healthy."""
    port = c.default_port("toxiproxy")
    toxi_url = f"http://localhost:{port}"

    r = requests.post(
        f"{toxi_url}/proxies/minio/toxics",
        json={
            "name": "blob-latency",
            "type": "latency",
            "attributes": {"latency": 100, "jitter": 0},
        },
    )
    assert r.status_code == 200, f"Failed to add blob latency: {r.text}"

    r = requests.post(
        f"{toxi_url}/proxies/consensus/toxics",
        json={
            "name": "consensus-latency",
            "type": "latency",
            "attributes": {"latency": 100, "jitter": 0},
        },
    )
    assert r.status_code == 200, f"Failed to add consensus latency: {r.text}"


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", type=int, default=900)
    parser.add_argument("--num-sources", type=int, default=10)
    parser.add_argument("--num-keys", type=int, default=50000)
    parser.add_argument("--num-hot-keys", type=int, default=100)
    parser.add_argument("--parallelism", type=int, default=25)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)
    random.seed(seed)
    print(f"Seed: {seed}")

    n = args.num_sources
    end_time = datetime.now() + timedelta(seconds=args.runtime)

    # Per-source randomized partition counts (1..32)
    partitions = [random.choice([1, 2, 4, 8, 16, 32]) for _ in range(n)]
    # Randomized cluster size
    workers = random.choice([4, 8, 16])
    replication_factor = 2

    print(f"Config: {n} sources, workers={workers}, rf={replication_factor}")
    print(f"Partition counts: {partitions}")

    c.down(destroy_volumes=True)
    # Start infrastructure first, then set up toxiproxy proxies (no latency),
    # then start materialized. Latency is added after sources are created.
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
    # (~5000 keys × ~500 bytes = ~2.5MB per source → multi-batch rehydration)
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
    # Wait for data to be ingested and persisted (longer for 50K+ keys)
    time.sleep(30)
    print("Pre-population done.")

    # Now add latency to persist storage to slow down the feedback loop.
    # This creates a window where two AtTime drains at different timestamps
    # can fire for the same tombstoned key before feedback arrives.
    add_toxiproxy_latency(c)

    errors = []

    def data_pump(thread_id):
        rnd = 0
        while datetime.now() < end_time:
            try:
                src_id = random.randint(0, n - 1)
                partitions[src_id]
                # 80% of writes target "hot" keys (first num_hot_keys)
                # 20% target the full key range (for shard size)
                if random.random() < 0.8:
                    nk = random.randint(1, args.num_hot_keys)
                else:
                    nk = random.randint(args.num_hot_keys, min(args.num_keys, 500))

                # Tombstone keys, then re-insert at multiple timestamps.
                # With toxiproxy adding ~4s latency to persist, the
                # feedback loop takes ~8-10s. Re-inserts 1.5s apart get
                # different MZ timestamps and can both emit +1 without
                # retraction if the key is tombstoned (finalized=None).
                c.testdrive(
                    f"$ kafka-ingest format=avro topic=s-{src_id}"
                    f" key-format=avro key-schema={KEY_SCHEMA}"
                    f" schema={VAL_SCHEMA}"
                    f" repeat={nk}\n"
                    f'{{"k": ${{kafka-ingest.iteration}}}}'
                    "\n",
                )
                for reinsert_round in range(3):
                    time.sleep(1.1)  # Just enough to cross a 1s reclocking boundary
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA}"
                        f" repeat={nk}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}} '
                        f'{{"f_int": ${{kafka-ingest.iteration}}, '
                        f'"f_long": {rnd * 77 + reinsert_round}, '
                        f'"f_str": "tr-t{thread_id}r{rnd}i{reinsert_round}{PAD}", '
                        f'"f_double": {rnd * 0.01 + reinsert_round * 0.001:.6f}, '
                        f'"f_bool": true, '
                        f'"f_nullable": null}}'
                        "\n",
                    )

                rnd += 1
            except Exception as e:
                errors.append(e)

    print(f"Starting {args.parallelism} data pump threads for {args.runtime}s...")
    threads = []
    for i in range(args.parallelism):
        t = Thread(name=f"pump_{i}", target=data_pump, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    # Wait for data pump threads to finish (the bug should trigger naturally
    # due to persist latency from toxiproxy)
    remaining = (end_time - datetime.now()).total_seconds()
    print(f"Waiting {remaining:.0f}s for data pump threads...")
    for t in threads:
        t.join(timeout=remaining + 30)

    if errors:
        print(f"Data pump errors: {len(errors)}")
        for e in errors[:5]:
            print(f"  {e}")

    print(f"Done. Seed: {seed}.")
