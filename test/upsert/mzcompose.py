# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This mzcompose currently tests `UPSERT` sources with `DISK` configured.
# TODO(guswynn): move ALL upsert-related tests into this directory.

from pathlib import Path
from textwrap import dedent

from materialize import ci_util
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

materialized_environment_extra = ["MZ_PERSIST_COMPACTION_DISABLED=false"]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(
        options=[
            "--orchestrator-process-scratch-directory=/scratch",
        ],
        additional_system_parameter_defaults={
            "disk_cluster_replicas_default": "true",
            "enable_unmanaged_cluster_replicas": "true",
            "storage_dataflow_delay_sources_past_rehydration": "true",
        },
        environment_extra=materialized_environment_extra,
    ),
    Testdrive(),
    Clusterd(
        name="clusterd1",
    ),
    Redpanda(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--compaction-disabled",
        action="store_true",
        help="Run with MZ_PERSIST_COMPACTION_DISABLED",
    )
    args = parser.parse_args()

    if args.compaction_disabled:
        materialized_environment_extra[0] = "MZ_PERSIST_COMPACTION_DISABLED=true"

    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_testdrive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "--kafka-default-partitions",
        type=int,
        metavar="N",
        help="set the default number of kafka partitions per topic",
    )
    parser.add_argument(
        "--default-size",
        type=int,
        default=Materialized.Size.DEFAULT_SIZE,
        help="Use SIZE 'N-N' for replicas and SIZE 'N' for sources",
    )

    parser.add_argument("--replicas", type=int, default=1, help="use multiple replicas")

    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    args = parser.parse_args()

    dependencies = ["materialized", "zookeeper", "kafka", "schema-registry"]

    testdrive = Testdrive(
        forward_buildkite_shard=True,
        kafka_default_partitions=args.kafka_default_partitions,
        validate_postgres_stash="materialized",
    )

    materialized = Materialized(
        default_size=args.default_size,
        options=[
            "--orchestrator-process-scratch-directory=/scratch",
        ],
        additional_system_parameter_defaults={
            "disk_cluster_replicas_default": "true",
        },
        environment_extra=materialized_environment_extra,
    )

    with c.override(testdrive, materialized):
        c.up(*dependencies)

        if args.replicas > 1:
            c.sql("DROP CLUSTER default CASCADE")
            # Make sure a replica named 'r1' always exists
            replica_names = [
                "r1" if replica_id == 0 else f"replica{replica_id}"
                for replica_id in range(0, args.replicas)
            ]
            replica_string = ",".join(
                f"{replica_name} (SIZE '{materialized.default_replica_size}')"
                for replica_name in replica_names
            )
            c.sql(f"CREATE CLUSTER default REPLICAS ({replica_string})")

        junit_report = ci_util.junit_report_filename(c.name)

        try:
            junit_report = ci_util.junit_report_filename(c.name)
            for file in args.files:
                c.run(
                    "testdrive",
                    f"--junit-report={junit_report}",
                    f"--var=replicas={args.replicas}",
                    f"--var=default-replica-size={materialized.default_replica_size}",
                    f"--var=default-storage-size={materialized.default_storage_size}",
                    file,
                )
                c.sanity_restart_mz()
        finally:
            ci_util.upload_junit_report(
                "testdrive", Path(__file__).parent / junit_report
            )


def workflow_rehydration(c: Composition) -> None:
    """Test creating sources in a remote clusterd process."""

    dependencies = [
        "materialized",
        "zookeeper",
        "kafka",
        "schema-registry",
        "clusterd1",
    ]

    for style, mz, clusterd in [
        (
            "with DISK",
            Materialized(
                options=[
                    "--orchestrator-process-scratch-directory=/scratch",
                ],
                additional_system_parameter_defaults={
                    "enable_unmanaged_cluster_replicas": "true",
                    "disk_cluster_replicas_default": "true",
                    "enable_disk_cluster_replicas": "true",
                    # Force backpressure to be enabled.
                    "storage_dataflow_max_inflight_bytes": "1",
                    "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction": "0.01",
                    "storage_dataflow_max_inflight_bytes_disk_only": "false",
                    "storage_dataflow_delay_sources_past_rehydration": "true",
                    # Enabling shrinking buffers
                    "upsert_rocksdb_shrink_allocated_buffers_by_ratio": "4",
                    "storage_shrink_upsert_unused_buffers_by_ratio": "4",
                },
                environment_extra=materialized_environment_extra,
            ),
            Clusterd(
                name="clusterd1",
                options=[
                    "--scratch-directory=/scratch",
                    "--announce-memory-limit=1048376000",  # 1GiB
                ],
            ),
        ),
        (
            "without DISK",
            Materialized(
                options=[
                    "--orchestrator-process-scratch-directory=/scratch",
                ],
                additional_system_parameter_defaults={
                    "enable_unmanaged_cluster_replicas": "true",
                    # Force backpressure to be enabled.
                    "storage_dataflow_max_inflight_bytes": "1",
                    "storage_dataflow_max_inflight_bytes_to_cluster_size_fraction": "0.01",
                    "storage_dataflow_max_inflight_bytes_disk_only": "false",
                    "storage_dataflow_delay_sources_past_rehydration": "true",
                },
                environment_extra=materialized_environment_extra,
            ),
            Clusterd(
                name="clusterd1",
            ),
        ),
    ]:
        with c.override(
            mz,
            clusterd,
            Testdrive(no_reset=True, consistent_seed=True),
        ):

            print(f"Running rehydration workflow {style}")
            c.down(destroy_volumes=True)

            c.up(*dependencies)
            c.run("testdrive", "rehydration/01-setup.td")
            c.run("testdrive", "rehydration/02-source-setup.td")

            c.kill("materialized")
            c.kill("clusterd1")
            c.up("materialized")
            c.up("clusterd1")

            c.run("testdrive", "rehydration/03-after-rehydration.td")

        c.run("testdrive", "rehydration/04-reset.td")


def workflow_failpoint(c: Composition) -> None:
    """Test behaviour when upsert state errors"""
    print("Running failpoint workflow")

    c.down(destroy_volumes=True)
    c.up("materialized")
    c.run("testdrive", "failpoint/01-setup.td")

    for failpoint in [
        (
            "fail_merge_snapshot_chunk",
            "upsert: Failed to rehydrate state: Error merging snapshot values",
        ),
        (
            "fail_state_multi_put",
            "upsert: Failed to update records in state: Error putting values into state",
        ),
        (
            "fail_state_multi_get",
            "upsert: Failed to fetch records from state: Error getting values from state",
        ),
    ]:
        run_one_failpoint(c, failpoint[0], failpoint[1])


def run_one_failpoint(c: Composition, failpoint: str, error_message: str) -> None:
    print(f">>> Running failpoint test for failpoint {failpoint}")

    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
    ):
        dependencies = ["zookeeper", "kafka", "clusterd1", "materialized"]
        c.up(*dependencies)
        c.run("testdrive", "failpoint/02-source.td")
        c.kill("clusterd1")

        with c.override(
            # Start clusterd with failpoint
            Clusterd(
                name="clusterd1",
                environment_extra=[f"FAILPOINTS={failpoint}=return"],
            ),
        ):
            c.up("clusterd1")
            c.run(
                "testdrive", f"--var=error={error_message}", "failpoint/03-failpoint.td"
            )
            c.kill("clusterd1")

        # Running without set failpoint
        c.up("clusterd1")
        c.run("testdrive", "failpoint/04-recover.td")

    c.run("testdrive", "failpoint/05-reset.td")
    c.kill("clusterd1")


def workflow_incident_49(c: Composition) -> None:
    """Regression test for incident 49."""

    c.down(destroy_volumes=True)

    dependencies = [
        "materialized",
        "zookeeper",
        "kafka",
        "schema-registry",
    ]

    for style, mz in [
        (
            "with DISK",
            Materialized(
                additional_system_parameter_defaults={
                    "disk_cluster_replicas_default": "true",
                    "storage_dataflow_delay_sources_past_rehydration": "true",
                },
                environment_extra=materialized_environment_extra,
            ),
        ),
        (
            "without DISK",
            Materialized(
                additional_system_parameter_defaults={
                    "disk_cluster_replicas_default": "false",
                    "storage_dataflow_delay_sources_past_rehydration": "true",
                },
                environment_extra=materialized_environment_extra,
            ),
        ),
    ]:
        with c.override(
            mz,
            Testdrive(no_reset=True, consistent_seed=True),
        ):
            print(f"Running incident-49 workflow {style}")
            c.down(destroy_volumes=True)
            c.up(*dependencies)

            c.run("testdrive", "incident-49/01-setup.td")

            c.kill("materialized")
            c.up("materialized")

            c.run("testdrive", "incident-49/02-after-rehydration.td")

        c.run("testdrive", "incident-49/03-reset.td")


def workflow_rocksdb_cleanup(c: Composition) -> None:
    """Testing rocksdb cleanup after dropping sources"""
    c.down(destroy_volumes=True)
    dependencies = [
        "materialized",
        "zookeeper",
        "kafka",
        "schema-registry",
    ]
    c.up(*dependencies)

    # Returns rockdb's cluster level and source level paths for a given source name
    def rocksdb_path(source_name: str) -> tuple[str, str]:
        (source_id, cluster_id, replica_id) = c.sql_query(
            f"""select s.id, s.cluster_id, c.id
            from mz_sources s
            join mz_cluster_replicas c
            on s.cluster_id = c.cluster_id
            where s.name ='{source_name}'"""
        )[0]
        prefix = "/scratch"
        cluster_prefix = f"cluster-{cluster_id}-replica-{replica_id}"
        return f"{prefix}/{cluster_prefix}", f"{prefix}/{cluster_prefix}/{source_id}"

    # Returns the number of files recursive in a given directory
    def num_files(dir: str) -> int:
        num_files = c.exec(
            "materialized", "bash", "-c", f"find {dir} -type f | wc -l", capture=True
        ).stdout.strip()
        return int(num_files)

    scenarios = [
        ("drop-source.td", "DROP SOURCE dropped_upsert", True),
        ("drop-cluster-cascade.td", "DROP CLUSTER c1 CASCADE", True),
        ("drop-source-in-cluster.td", "DROP SOURCE dropped_upsert", False),
    ]

    for testdrive_file, drop_stmt, cluster_dropped in scenarios:
        with c.override(
            Testdrive(no_reset=True),
        ):
            c.up("testdrive", persistent=True)
            c.exec("testdrive", f"rocksdb-cleanup/{testdrive_file}")

            (_, kept_source_path) = rocksdb_path("kept_upsert")
            (dropped_cluster_path, dropped_source_path) = rocksdb_path("dropped_upsert")

            assert num_files(kept_source_path) > 0
            assert num_files(dropped_source_path) > 0

            c.testdrive(f"> {drop_stmt}")

            assert num_files(kept_source_path) > 0

            if cluster_dropped:
                assert num_files(dropped_cluster_path) == 0
            else:
                assert num_files(dropped_source_path) == 0

        c.testdrive(
            dedent(
                """
            $ nop

            # this is to reset testdrive
            """
            )
        )


def workflow_autospill(c: Composition) -> None:
    """Testing auto spill to disk"""
    c.down(destroy_volumes=True)
    dependencies = [
        "zookeeper",
        "kafka",
        "materialized",
        "schema-registry",
    ]

    with c.override(
        Materialized(
            options=[
                "--orchestrator-process-scratch-directory=/mzdata/source_data",
            ],
            additional_system_parameter_defaults={
                "upsert_source_disk_default": "true",
                "upsert_rocksdb_auto_spill_to_disk": "true",
                "upsert_rocksdb_auto_spill_threshold_bytes": "200",
                "storage_dataflow_delay_sources_past_rehydration": "true",
            },
        ),
    ):
        c.up(*dependencies)
        c.run("testdrive", "autospill/bytes.td")


# This should not be run on ci and is not added to workflow_default above!
# This test is there to compare rehydration metrics with different configs.
# Can be run locally with the command ./mzcompose run load-test
def workflow_load_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    from string import ascii_lowercase
    from textwrap import dedent

    # Following variables can be updated to tweak how much data the kafka
    # topic should be populated with and what should be the upsert state.
    pad_len = 1024
    string_pad = "x" * pad_len  # 1KB
    repeat = 250 * 1024  # repeat * string_pad = 250MB upsert state size
    updates_count = 4  # repeat * updates_count = 1GB total kafka topic size with multiple updates for the same key

    backpressure_bytes = 100 * 1024 * 1024  # 100MB

    c.down(destroy_volumes=True)
    c.up("redpanda", "materialized", "clusterd1")
    # initial hydration
    with c.override(
        Testdrive(no_reset=True, consistent_seed=True),
        Materialized(
            options=[
                "--orchestrator-process-scratch-directory=/scratch",
            ],
            additional_system_parameter_defaults={
                "disk_cluster_replicas_default": "true",
                "enable_disk_cluster_replicas": "true",
                # Force backpressure to be enabled.
                "storage_dataflow_max_inflight_bytes": f"{backpressure_bytes}",
                "storage_dataflow_max_inflight_bytes_disk_only": "true",
                "storage_dataflow_delay_sources_past_rehydration": "true",
            },
            environment_extra=materialized_environment_extra,
            memory="5Gb",
        ),
        Clusterd(
            name="clusterd1",
            options=[
                "--scratch-directory=/scratch",
                "--announce-memory-limit=1048376000",  # 1GiB
            ],
            memory="3.5Gb",
        ),
    ):
        c.up("testdrive", persistent=True)
        c.exec("testdrive", "load-test/setup.td")
        c.testdrive(
            dedent(
                """
                $ kafka-ingest format=bytes topic=topic1 key-format=bytes key-terminator=:
                "AAA":"START MARKER"
                """
            )
        )
        for letter in ascii_lowercase[:updates_count]:
            c.exec(
                "testdrive",
                f"--var=value={letter}{string_pad}",
                f"--var=repeat={repeat}",
                "load-test/insert.td",
            )

        c.testdrive(
            dedent(
                """
                $ kafka-ingest format=bytes topic=topic1 key-format=bytes key-terminator=:
                "ZZZ":"END MARKER"
                """
            )
        )

        c.testdrive(
            dedent(
                f"""
                > select * from v1;
                {repeat + 2}
                """
            )
        )
        c.kill("clusterd1")
        c.up("clusterd1")

        c.testdrive(
            dedent(
                f"""
                > select * from v1;
                {repeat + 2}
                """
            )
        )

        rehydration_latency_ms = c.sql_query(
            """select max(rehydration_latency_ms)
            from mz_internal.mz_source_statistics st
            join mz_sources s on s.id = st.id
            where name = 's1';"""
        )[0]
        print(f"It took {rehydration_latency_ms} minutes!")
