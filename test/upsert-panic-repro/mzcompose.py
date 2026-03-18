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
from threading import Thread, Event

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import (
    DeploymentStatus,
    Materialized,
)
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SYSTEM_PARAMS = {
    **get_default_system_parameters(),
    "max_sources": "10000",
    "max_tables": "10000",
    "max_objects_per_schema": "10000",
    "storage_rocksdb_use_merge_operator": "true",
    "storage_upsert_prevent_snapshot_buffering": "true",
    "storage_upsert_max_snapshot_batch_buffering": "1",
    "storage_dataflow_max_inflight_bytes": "1048576",
    "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction": "0.001",
    "storage_dataflow_max_inflight_bytes_disk_only": "false",
}

MZ_ENV_EXTRA = [
    "MZ_PERSIST_COMPACTION_DISABLED=true",
    "FAILPOINTS=upsert_skip_retraction=return",
]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    CockroachOrPostgresMetadata(),
    Materialized(
        name="mz_old",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMS,
        external_metadata_store=True,
        default_replication_factor=1,
        environment_extra=MZ_ENV_EXTRA,
    ),
    Materialized(
        name="mz_new",
        sanity_restart=False,
        deploy_generation=1,
        system_parameter_defaults=SYSTEM_PARAMS,
        restart="on-failure",
        external_metadata_store=True,
        default_replication_factor=2,
        environment_extra=MZ_ENV_EXTRA,
    ),
    Testdrive(
        materialize_url="postgres://materialize@mz_old:6875",
        materialize_url_internal="postgres://materialize@mz_old:6877",
        mz_service="mz_old",
        no_consistency_checks=True,
        no_reset=True,
        seed=1,
        default_timeout="600s",
    ),
]

# Padding to make values ~500 bytes (closer to production 374-byte values)
PAD = "x" * 400

# Dedicated key range for targeted tombstone injection.
# These keys are ONLY used by targeted_inject(), not by the random data pump.
# Must be within [0, num_keys) to be pre-populated, and above the data pump's
# key range (0..500) to avoid interference.
TARGETED_KEY_START = 40000
TARGETED_KEY_COUNT = 50

KEY_SCHEMA = '{"type": "record", "name": "Key", "fields": [{"name": "k", "type": "long"}]}'
VAL_SCHEMA = (
    '{"type": "record", "name": "Value", "fields": ['
    '{"name": "f_int", "type": "int"},'
    '{"name": "f_long", "type": "long"},'
    '{"name": "f_str", "type": "string"},'
    '{"name": "f_double", "type": "double"},'
    '{"name": "f_bool", "type": "boolean"},'
    '{"name": "f_nullable", "type": ["null", "string"], "default": null}'
    ']}'
)


def _targeted_body_tombstone():
    """Generate kafka-ingest body for tombstoning targeted keys (key-only, no value)."""
    lines = []
    for j in range(TARGETED_KEY_COUNT):
        lines.append(f'{{"k": {TARGETED_KEY_START + j}}}')
    return "\n".join(lines) + "\n"


def _targeted_body_insert(round_num):
    """Generate kafka-ingest body for inserting values into targeted keys."""
    lines = []
    for j in range(TARGETED_KEY_COUNT):
        key = TARGETED_KEY_START + j
        lines.append(
            f'{{"k": {key}}} '
            f'{{"f_int": {j}, '
            f'"f_long": {round_num * 1000 + j}, '
            f'"f_str": "tgt-r{round_num}k{j}{PAD}", '
            f'"f_double": {round_num * 0.001:.6f}, '
            f'"f_bool": true, '
            f'"f_nullable": null}}'
        )
    return "\n".join(lines) + "\n"


def targeted_tombstone_only(c, n, label):
    """Tombstone targeted keys and wait for tombstones to be persisted."""
    tombstone_body = _targeted_body_tombstone()
    print(f"  [{label}] Tombstoning {TARGETED_KEY_COUNT} keys...")
    for i in range(n):
        c.testdrive(
            f"$ kafka-ingest format=avro topic=s-{i}"
            f" key-format=avro key-schema={KEY_SCHEMA}"
            f" schema={VAL_SCHEMA}\n"
            + tombstone_body,
        )
    print(f"  [{label}] Waiting for tombstones to reach persist...")
    time.sleep(8)


def targeted_inject_data(c, n, label, rounds=40, delay=0.05):
    """Inject re-insertion data for targeted keys at many timestamps.

    Each round is a separate kafka-ingest → different Kafka CreateTime →
    different MZ timestamp after reclocking. This creates data for
    tombstoned keys at ROUNDS distinct timestamps.
    """
    print(f"  [{label}] Injecting {rounds} rounds of data for targeted keys...")
    for r in range(rounds):
        body = _targeted_body_insert(r)
        for i in range(n):
            try:
                c.testdrive(
                    f"$ kafka-ingest format=avro topic=s-{i}"
                    f" key-format=avro key-schema={KEY_SCHEMA}"
                    f" schema={VAL_SCHEMA}\n"
                    + body,
                )
            except Exception:
                pass
        time.sleep(delay)
    print(f"  [{label}] Data injection done ({rounds} rounds).")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", type=int, default=900)
    parser.add_argument("--num-sources", type=int, default=2)
    parser.add_argument("--num-keys", type=int, default=50000)
    parser.add_argument("--num-hot-keys", type=int, default=30)
    parser.add_argument("--parallelism", type=int, default=15)
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
    replication_factor = random.choice([1, 2])
    deploy_interval = 99999  # Keep instance alive for steady-state reproduction

    print(f"Config: {n} sources, workers={workers}, rf={replication_factor}, "
          f"deploy_interval={deploy_interval}s")
    print(f"Partition counts: {partitions}")

    c.down(destroy_volumes=True)
    c.up("zookeeper", "kafka", "schema-registry", "mz_old",
         Service("testdrive", idle=True))

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
        c.testdrive(
            f"$ kafka-create-topic topic=s-{i} partitions={partitions[i]}\n"
        )

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
        c.testdrive(f"""\
> CREATE SOURCE s_{i}
  IN CLUSTER upsert_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> CREATE TABLE s_{i}_tbl
  FROM SOURCE s_{i} (REFERENCE "{topic}");
""")
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

    errors = []

    def data_pump(thread_id):
        rnd = 0
        while datetime.now() < end_time:
            try:
                src_id = random.randint(0, n - 1)
                p = partitions[src_id]
                # 80% of writes target "hot" keys (first num_hot_keys)
                # 20% target the full key range (for shard size)
                if random.random() < 0.8:
                    nk = random.randint(1, args.num_hot_keys)
                else:
                    nk = random.randint(args.num_hot_keys, min(args.num_keys, 500))
                op = random.choice(
                    ["cross-partition", "fill",
                     "tombstone-reinsert", "tombstone-reinsert",
                     "tombstone-reinsert", "tombstone-reinsert",
                     "tombstone-reinsert", "tombstone-reinsert"]
                )

                if op == "cross-partition":
                    part_id = random.randint(0, p - 1)
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA} partition={part_id}"
                        f" repeat={nk}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}} '
                        f'{{"f_int": ${{kafka-ingest.iteration}}, '
                        f'"f_long": {rnd * 100 + part_id}, '
                        f'"f_str": "xp-t{thread_id}r{rnd}p{part_id}{PAD}", '
                        f'"f_double": {part_id * 0.01:.4f}, '
                        f'"f_bool": {str(random.choice([True, False])).lower()}, '
                        f'"f_nullable": {{"string": "p{part_id}"}}}}'
                        "\n",
                    )

                elif op == "fill":
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA}"
                        f" repeat={nk}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}} '
                        f'{{"f_int": ${{kafka-ingest.iteration}}, '
                        f'"f_long": {rnd}, '
                        f'"f_str": "fill-t{thread_id}r{rnd}{PAD}", '
                        f'"f_double": {rnd * 0.1:.4f}, '
                        f'"f_bool": true, '
                        f'"f_nullable": {random.choice(["null", "{" + '"string": "x"' + "}"])}}}'
                        "\n",
                    )

                elif op == "churn":
                    b = random.randint(nk // 4, nk)
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA}"
                        f" repeat={b}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}} '
                        f'{{"f_int": ${{kafka-ingest.iteration}}, '
                        f'"f_long": {rnd * 1000000}, '
                        f'"f_str": "churn-t{thread_id}r{rnd}{PAD}", '
                        f'"f_double": {rnd * 0.001:.6f}, '
                        f'"f_bool": {str(rnd % 2 == 0).lower()}, '
                        f'"f_nullable": {{"string": "v{rnd}"}}}}'
                        "\n",
                    )

                elif op == "retract":
                    b = random.randint(1, nk)
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA}"
                        f" repeat={b}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}}'
                        "\n",
                    )

                elif op == "tombstone-reinsert":
                    # Retract all keys, then immediately re-insert them
                    # MULTIPLE times at different timestamps. With
                    # snapshot_buffering_max=10000, the stash can
                    # accumulate data at multiple timestamps for the same
                    # key. The AtTime drain processes all of them in one
                    # call — the second sees the provisional from the
                    # first, triggering diff_sum=2 if finalized=None.
                    c.testdrive(
                        f"$ kafka-ingest format=avro topic=s-{src_id}"
                        f" key-format=avro key-schema={KEY_SCHEMA}"
                        f" schema={VAL_SCHEMA}"
                        f" repeat={nk}\n"
                        f'{{"k": ${{kafka-ingest.iteration}}}}'
                        "\n",
                    )
                    # Multiple re-inserts at different Kafka timestamps.
                    # Sleep 1.5s between rounds to ensure different MZ
                    # reclocked timestamps (reclocking has ~1s granularity).
                    for reinsert_round in range(5):
                        time.sleep(1.5)
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

    # Alternating 0dt deploys and hard kills
    deploy_gen = 1
    mz1 = "mz_old"
    mz2 = "mz_new"
    restart_count = 0

    while datetime.now() < end_time:
        time.sleep(deploy_interval)

        # Area 19 flow: tombstone → persist → inject data → kill → restart.
        # The MZ_UPSERT_FORCE_ORPHAN_EXIT mechanism in Rust code will:
        # 1. Keep partial_drain_time set during rehydration
        # 2. Trigger AtTime drains for tombstoned keys
        # 3. Detect BUG PRE-CONDITION (orphaned +1 for tombstoned key)
        # 4. Sleep 15s to let persist_sink flush the orphaned +1s
        # 5. Exit process
        # 6. Docker restarts the container → next rehydration hits diff_sum=2 → PANIC
        label = f"cycle-{deploy_gen}"
        try:
            # Step 1: Tombstone targeted keys and wait for tombstones to persist
            targeted_tombstone_only(c, n, label)

            # Step 2: Inject re-insertion data rapidly. This data goes to Kafka
            # and the MZ instance starts processing it. We want some of this data
            # to be in Kafka but NOT YET fully persisted when we kill.
            targeted_inject_data(c, n, label, rounds=20, delay=0.02)

            # Step 3: Kill the instance. Re-insertion data may be partially
            # persisted. The key state in the shard: tombstoned (finalized=None)
            # for targeted keys, with re-insertion data in Kafka.
            print(f"  [{label}] Killing {mz1}...")
            c.kill(mz1, signal="SIGKILL")
            time.sleep(1)

            # Step 4: Inject MORE re-insertion data while instance is dead.
            # This ensures there's plenty of Kafka data at distinct timestamps
            # for the targeted keys when the next instance rehydrates.
            targeted_inject_data(c, n, label, rounds=30, delay=0.02)

        except Exception as e:
            print(f"  [{label}] Pre-kill setup failed: {e}")

        # Step 5: Restart. The restarted instance rehydrates from the persist
        # shard (tombstoned keys) and replays Kafka (re-insertions).
        # With MZ_UPSERT_FORCE_ORPHAN_EXIT:
        # - partial_drain_time stays set during rehydration
        # - AtTime drains fire for tombstoned keys → orphaned +1s
        # - BUG PRE-CONDITION fires → sleep 15s → exit(1)
        # - Docker restarts → rehydration → diff_sum=2 → PANIC
        print(f"  [{label}] Restarting {mz1}...")
        try:
            with c.override(
                Materialized(
                    name=mz1,
                    sanity_restart=False,
                    deploy_generation=deploy_gen - 1,
                    system_parameter_defaults=SYSTEM_PARAMS,
                    restart="on-failure",
                    external_metadata_store=True,
                    default_replication_factor=2,
                    environment_extra=MZ_ENV_EXTRA,
                ),
                Testdrive(
                    materialize_url=f"postgres://materialize@{mz1}:6875",
                    materialize_url_internal=f"postgres://materialize@{mz1}:6877",
                    mz_service=mz1,
                    no_consistency_checks=True,
                    no_reset=True,
                    seed=1,
                    default_timeout="600s",
                ),
            ):
                c.up(mz1)
                # Wait for the instance to come up. It may:
                # a) Become leader normally (BUG PRE-CONDITION not hit)
                # b) Exit during rehydration (force_orphan_exit triggered)
                #    → Docker restarts → panic on next rehydration
                # c) Panic during rehydration (diff_sum=2 from previous cycle)
                #
                # We wait up to 120s for it to become leader. If it panics
                # or exits, the restart="on-failure" will restart it, and
                # we should see the panic in the logs.
                try:
                    c.await_mz_deployment_status(
                        DeploymentStatus.IS_LEADER, mz1, timeout=120
                    )
                    print(f"  [{label}] {mz1} is leader.")
                except Exception as e:
                    print(f"  [{label}] {mz1} did not become leader (expected if panic): {e}")
                    # Check logs for the panic
                    print(f"  [{label}] Check logs: bin/mzcompose --find upsert-panic-repro logs {mz1}")
                    # Try to bring it back up
                    time.sleep(5)
                    c.up(mz1)
                    try:
                        c.await_mz_deployment_status(
                            DeploymentStatus.IS_LEADER, mz1, timeout=120
                        )
                    except Exception:
                        print(f"  [{label}] {mz1} still not up after retry. Moving on.")

            restart_count += 1
        except Exception as e:
            print(f"  [{label}] Restart failed: {e}")

        deploy_gen += 1

    print("Waiting for data pump threads to finish...")
    for t in threads:
        t.join(timeout=30)

    if errors:
        print(f"Data pump errors: {len(errors)}")
        for e in errors[:5]:
            print(f"  {e}")

    print(f"Done. Seed: {seed}. Completed {deploy_gen - 1} deploys, {restart_count} hard kills.")
