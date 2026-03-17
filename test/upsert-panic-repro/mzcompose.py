# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(
        options=["--orchestrator-process-scratch-directory=/scratch"],
        additional_system_parameter_defaults={
            "unsafe_enable_unorchestrated_cluster_replicas": "true",
            "storage_dataflow_delay_sources_past_rehydration": "true",
        },
        environment_extra=["MZ_PERSIST_COMPACTION_DISABLED=false"],
        default_replication_factor=2,
        support_external_clusterd=True,
    ),
    Testdrive(no_reset=True, consistent_seed=True, default_timeout="600s"),
    Clusterd(name="clusterd1"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--rounds", type=int, default=50)
    parser.add_argument("--num-sources", type=int, default=100)
    parser.add_argument("--num-keys", type=int, default=5000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    prefix = "storm"
    n = args.num_sources

    clusterd = Clusterd(name="clusterd1", workers=args.workers)
    clusterd.config["ulimits"] = {"nofile": {"soft": 65536, "hard": 65536}}

    with c.override(
        Materialized(
            options=["--orchestrator-process-scratch-directory=/scratch"],
            additional_system_parameter_defaults={
                "unsafe_enable_unorchestrated_cluster_replicas": "true",
                "storage_dataflow_delay_sources_past_rehydration": "true",
            },
            environment_extra=["MZ_PERSIST_COMPACTION_DISABLED=false"],
            default_replication_factor=2,
            support_external_clusterd=True,
        ),
        clusterd,
        Testdrive(no_reset=True, consistent_seed=True, default_timeout="600s"),
    ):
        c.down(destroy_volumes=True)
        c.up("zookeeper", "kafka", "schema-registry", "materialized", "clusterd1")

        print(f"Creating {n} topics...")
        for start in range(0, n, 50):
            end = min(start + 50, n)
            lines = [
                f"$ kafka-create-topic topic={prefix}-{i} partitions=4"
                for i in range(start, end)
            ]
            _td(c, "\n".join(lines))

        _td(
            c,
            """\
> CREATE CONNECTION IF NOT EXISTS kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
""",
        )
        _td(
            c,
            f"""\
> CREATE CLUSTER storage_cluster REPLICAS (
    r1 (
        STORAGECTL ADDRESSES ['clusterd1:2100'],
        STORAGE ADDRESSES ['clusterd1:2103'],
        COMPUTECTL ADDRESSES ['clusterd1:2101'],
        COMPUTE ADDRESSES ['clusterd1:2102'],
        WORKERS {args.workers}
    )
  );
""",
        )

        print(f"Creating {n} upsert sources...")
        for i in range(n):
            if i > 0 and i % 50 == 0:
                print(f"  {i}/{n}...")
            topic = f"testdrive-{prefix}-{i}-${{testdrive.seed}}"
            _td(
                c,
                f"""\
> CREATE SOURCE {prefix}_{i}
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC '{topic}');

> CREATE TABLE {prefix}_{i}_tbl
  FROM SOURCE {prefix}_{i} (REFERENCE "{topic}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT;
""",
            )
        print(f"  {n}/{n}.")

        print(f"Seeding {args.num_keys} keys across {n} sources...")
        _ingest_all(c, prefix, n, args.num_keys, "seed-${kafka-ingest.iteration}")
        _verify_all(c, prefix, n, args.num_keys)
        print("Seed complete. Starting restart storm...")

        for rnd in range(args.rounds):
            print(f"Round {rnd + 1}/{args.rounds}")

            _ingest_all(
                c,
                prefix,
                n,
                args.num_keys,
                f"r{rnd}-${{kafka-ingest.iteration}}",
            )

            print("  Kill clusterd1...")
            c.kill("clusterd1")

            _ingest_all(
                c,
                prefix,
                n,
                args.num_keys,
                f"post-{rnd}-${{kafka-ingest.iteration}}",
            )

            print("  Restart clusterd1...")
            c.up("clusterd1")

            _verify_all(c, prefix, n, args.num_keys)
            print(f"  Round {rnd + 1} OK")

        print("Done.")


def _td(c, text):
    c.testdrive(args=["--no-reset"], input=text)


def _ingest_all(c, prefix, n, num_keys, value):
    lines = []
    for i in range(n):
        lines.append(
            f"$ kafka-ingest format=bytes topic={prefix}-{i}"
            f" key-format=bytes key-terminator=: repeat={num_keys}\n"
            f'"${{kafka-ingest.iteration}}":"{value}"'
        )
    _td(c, "\n\n".join(lines) + "\n")


def _verify_all(c, prefix, n, expected):
    lines = []
    for i in range(n):
        lines.append(f"> SELECT count(*) FROM {prefix}_{i}_tbl;\n{expected}")
    _td(c, "\n\n".join(lines) + "\n")
