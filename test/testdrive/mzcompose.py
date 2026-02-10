# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Testdrive is the basic framework and language for defining product tests under
the expected-result/actual-result (aka golden testing) paradigm. A query is
retried until it produces the desired result.
"""

import glob
import os

import boto3
from mypy_boto3_s3 import S3Client

from materialize import MZ_ROOT, buildkite, ci_util
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.fivetran_destination import FivetranDestination
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import metadata_store_services
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Mz(app_password=""),
    Minio(setup_materialize=True, additional_directories=["copytos3", "copyfroms3"]),
    Materialized(
        external_blob_store=True,
        sanity_restart=False,
    ),
    FivetranDestination(volumes_extra=["tmp:/share/tmp"]),
    Testdrive(external_blob_store=True),
    *metadata_store_services(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "--slow",
        action="store_true",
        help="include slow tests (usually only in Nightly)",
    )
    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )
    parser.add_argument(
        "--aws-region",
        help="run against the specified AWS region instead of localstack",
    )
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
        help="Use SIZE 'scale=N,workers=N' for replicas and SIZE 'scale=N,workers=1' for sources",
    )
    parser.add_argument(
        "--system-param",
        type=str,
        action="append",
        nargs="*",
        help="System parameters to set in Materialize, i.e. what you would set with `ALTER SYSTEM SET`",
    )

    parser.add_argument("--replicas", type=int, default=1, help="use multiple replicas")

    parser.add_argument(
        "--default-timeout",
        type=str,
        help="set the default timeout for Testdrive",
    )

    parser.add_argument(
        "--rewrite-results",
        action="store_true",
        help="Rewrite results, disables junit reports",
    )

    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )

    parser.add_argument(
        "--check-statement-logging",
        action="store_true",
        help="Run statement logging consistency checks (adds a few seconds at the end of every test file)",
    )

    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    (args, passthrough_args) = parser.parse_known_args()

    dependencies = [
        "fivetran-destination",
        "materialized",
        "postgres",
        "mysql",
        "minio",
    ]
    if args.redpanda:
        dependencies += ["redpanda"]
    else:
        dependencies += ["zookeeper", "kafka", "schema-registry"]

    additional_system_parameter_defaults = {"default_cluster_replication_factor": "1"}
    for val in args.system_param or []:
        x = val[0].split("=", maxsplit=1)
        assert len(x) == 2, f"--system-param '{val}' should be the format <key>=<val>"
        additional_system_parameter_defaults[x[0]] = x[1]

    materialized = Materialized(
        default_size=args.default_size,
        external_blob_store=True,
        blob_store_is_azure=args.azurite,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
        default_replication_factor=1,
        sanity_restart=False,
    )

    testdrive = Testdrive(
        kafka_default_partitions=args.kafka_default_partitions,
        aws_region=args.aws_region,
        validate_catalog_store=True,
        default_timeout=args.default_timeout,
        volumes_extra=["mzdata:/mzdata"],
        external_blob_store=True,
        blob_store_is_azure=args.azurite,
        fivetran_destination=True,
        fivetran_destination_files_path="/share/tmp",
        check_statement_logging=args.check_statement_logging,
        entrypoint_extra=[
            f"--var=uses-redpanda={args.redpanda}",
        ],
    )

    with c.override(testdrive, materialized):
        c.up(*dependencies, Service("testdrive", idle=True))

        c.sql(
            "ALTER SYSTEM SET max_clusters = 50;",
            port=6877,
            user="mz_system",
        )

        non_default_testdrive_vars = []

        minio_addr = c.port("minio", "9000")
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{minio_addr}",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        generate_parquet_files(s3)

        if args.replicas > 1:
            c.sql("DROP CLUSTER quickstart CASCADE", user="mz_system", port=6877)
            # Make sure a replica named 'r1' always exists
            replica_names = [
                "r1" if replica_id == 0 else f"replica{replica_id}"
                for replica_id in range(0, args.replicas)
            ]
            replica_string = ",".join(
                f"{replica_name} (SIZE '{materialized.default_replica_size}')"
                for replica_name in replica_names
            )
            c.sql(
                f"CREATE CLUSTER quickstart REPLICAS ({replica_string})",
                user="mz_system",
                port=6877,
            )

            # Note that any command that outputs SHOW CLUSTERS will have output
            # that depends on the number of replicas testdrive has. This means
            # it might be easier to skip certain tests if the number of replicas
            # is > 1.
            c.sql(
                f"""
                CREATE CLUSTER testdrive_single_replica_cluster SIZE = '{materialized.default_replica_size}';
                GRANT ALL PRIVILEGES ON CLUSTER testdrive_single_replica_cluster TO materialize;
                """,
                user="mz_system",
                port=6877,
            )

            non_default_testdrive_vars.append(f"--var=replicas={args.replicas}")
            non_default_testdrive_vars.append(
                "--var=single-replica-cluster=testdrive_single_replica_cluster"
            )

        if args.default_size != 1:
            non_default_testdrive_vars.append(
                f"--var=default-replica-size={materialized.default_replica_size}"
            )
            non_default_testdrive_vars.append(
                f"--var=default-storage-size={materialized.default_storage_size}"
            )

        print(f"Passing through arguments to testdrive {passthrough_args}\n")
        # do not set default args, they should be set in the td file using set-arg-default to easen the execution
        # without mzcompose

        def process(file: str) -> None:
            if not args.slow and file in (
                "explain-pushdown.td",
                "fivetran-destination.td",
                # Slow but often fails, still run on test pipeline
                # "introspection-sources.td",
                "kafka-upsert-sources.td",
                "materialized-view-refresh-options.td",
                "upsert-source-race.td",
            ):
                return
            junit_report = ci_util.junit_report_filename(f"{c.name}_{file}")
            c.run_testdrive_files(
                (
                    "--rewrite-results"
                    if args.rewrite_results
                    else f"--junit-report={junit_report}"
                ),
                *non_default_testdrive_vars,
                *passthrough_args,
                file,
            )
            # Uploading successful junit files wastes time and contains no useful information
            os.remove(MZ_ROOT / "test" / "testdrive" / junit_report)

        files = buildkite.shard_list(
            sorted(
                [
                    file
                    for pattern in args.files
                    for file in glob.glob(
                        pattern, root_dir=MZ_ROOT / "test" / "testdrive"
                    )
                ]
            ),
            lambda file: file,
        )
        c.test_parts(files, process)
        c.sanity_restart_mz()


def generate_parquet_files(s3: S3Client):
    import datetime
    import decimal
    import uuid

    import pyarrow as pa
    import pyarrow.parquet as pq

    record_col = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
            pa.field("avg", pa.float64()),
        ]
    )
    # arrays for various types
    arrays = [
        # i8
        pa.array([-1, 2, 3], type=pa.int8()),
        # u8
        pa.array([10, 20, 30], type=pa.uint8()),
        # i16
        pa.array([-1000, 2000, 3000], type=pa.int16()),
        # u16
        pa.array([10000, 20000, 30000], type=pa.uint16()),
        # i32
        pa.array([-100000, 200000, 300000], type=pa.int32()),
        # u32
        pa.array([1000000, 2000000, 3000000], type=pa.uint32()),
        # i64
        pa.array([-1 * 10**9, 2 * 10**9, 3 * 10**9], type=pa.int64()),
        # u64
        pa.array([10**18, 2 * 10**18, 3 * 10**18], type=pa.uint64()),
        # f32
        pa.array([-1.0, 2.5, 3.7], type=pa.float32()),
        # f64
        pa.array([-1.0, 2.5, 3.7], type=pa.float64()),
        # boolean
        pa.array([True, False, True], type=pa.bool_()),
        # string
        pa.array(["apple", "banana", "cherry"], type=pa.string()),
        # uuid
        pa.array([b"raw1", b"raw2", b"raw3"], type=pa.binary()),
        # date32
        pa.array(
            [
                datetime.date(2025, 11, 1),
                datetime.date(2025, 11, 2),
                datetime.date(2025, 11, 3),
            ],
            type=pa.date32(),
        ),
        # timestamp (milliseconds)
        pa.array(
            [
                datetime.datetime(2025, 11, 1, 10, 0, 0),
                datetime.datetime(2025, 11, 1, 11, 30, 0),
                datetime.datetime(2025, 11, 1, 12, 0, 0),
            ],
            type=pa.timestamp("ms"),
        ),
        # timestampz (milliseconds) - not supported
        # pa.array(
        #     [
        #         datetime.datetime(2025, 11, 1, 10, 0, 0, tzinfo=datetime.timezone.utc),
        #         datetime.datetime(2025, 11, 1, 11, 30, 0, tzinfo=datetime.timezone.utc),
        #         datetime.datetime(2025, 11, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        #     ],
        #     type=pa.timestamp("ms", "UTC"),
        # ),
        # time32
        pa.array(
            [
                datetime.time(9, 0, 0),
                datetime.time(10, 30, 15),
                datetime.time(11, 45, 30),
            ],
            type=pa.time32("s"),
        ),
        # list
        pa.array(
            [pa.array([-1, 2]), pa.array([3, 4, 5]), pa.array([])],
            type=pa.list_(pa.int64()),
        ),
        # decimal128
        pa.array(
            [decimal.Decimal("-54.321"), decimal.Decimal("123.45"), None],
            type=pa.decimal128(precision=10, scale=5),
        ),
        # json
        pa.array(
            ['{"a": 5, "b": { "c": 1.1 } }', '{ "d": "str", "e" : [1,2,3] }', "{}"],
            type=pa.string(),
        ),
        # record
        pa.array(
            [
                {"name": "Taco", "age": 3, "avg": 2.2},
                {"name": "Burger", "age": 2, "avg": 4.5},
                {"name": "SlimJim", "age": 1, "avg": 1.14},
            ],
            type=record_col,
        ),
        # uuid
        pa.array(
            [
                uuid.UUID("badc0deb-adc0-deba-dc0d-ebadc0debadc").bytes,
                uuid.UUID("deadbeef-dead-4eef-8eef-deaddeadbeef").bytes,
                uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
            ],
            type=pa.uuid(),
        ),
    ]

    field_names = [
        "int8_col",
        "uint8_col",
        "int16_col",
        "uint16_col",
        "int32_col",
        "uint32_col",
        "int64_col",
        "uint64_col",
        "float32_col",
        "float64_col",
        "bool_col",
        "string_col",
        "binary_col",
        "date32_col",
        "timestamp_ms_col",
        # "timestampz_ms_col", -- timestamp with timezone not supported
        "time32_col",
        "list_col",
        "decimal_col",
        "json_col",
        "record_col",
        "uuid_col"
    ]
    table = pa.Table.from_arrays(arrays, names=field_names)

    bucket = "copyfroms3"

    for compression in ["none", "snappy", "gzip", "brotli", "zstd", "lz4"]:
        suffix = "" if compression == "none" else f".{compression}"
        local_file = MZ_ROOT / "test" / "testdrive" / f"types.parquet{suffix}"
        pq.write_table(table, local_file, compression=compression)  # type: ignore
        key = f"parquet/{local_file.name}"
        s3.upload_file(str(local_file), bucket, key)
        print(f"File '{local_file}' successfully uploaded to '{bucket}/{key}'")
