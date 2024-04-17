# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import csv
from io import BytesIO, StringIO

import pyarrow.parquet  #
from minio import Minio

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio as MinioService
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    MinioService(
        setup_materialize=True,
        additional_directories=["copytos3"],
        ports=["9000:9000", "9001:9001"],
        allow_host_ports=True,
    ),
    Materialized(),  # Overridden below
    Testdrive(),  # Overridden below
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--default-size",
        type=int,
        default=1,  # Reduced memory
        help="Use SIZE 'N-N' for replicas and SIZE 'N' for sources",
    )

    args = parser.parse_args()

    dependencies = ["minio", "materialized"]

    testdrive = Testdrive(
        forward_buildkite_shard=True,
        aws_region=None,
        validate_catalog_store=True,
        default_timeout="1800s",
        volumes_extra=["mzdata:/mzdata"],
        external_minio=True,
        no_reset=True,
    )

    materialized = Materialized(
        default_size=args.default_size,
        external_minio=True,
    )

    with c.override(testdrive, materialized):
        c.up(*dependencies)

        c.sql(
            "ALTER SYSTEM SET max_clusters = 50;",
            port=6877,
            user="mz_system",
        )

        s3 = Minio(
            f"127.0.0.1:{c.default_port('minio')}",
            "minioadmin",
            "minioadmin",
            region="minio",
            secure=False,
        )
        c.run_testdrive_files("setup.td")

        for size in [
            "tpch10mb",
            "tpch1gb",
            # "tpch10gb",  # SELECT(*) in Mz is way too slow
        ]:
            c.run_testdrive_files(f"{size}.td")
            for table in [
                "customer",
                "lineitem",
                "nation",
                "orders",
                "part",
                "partsupp",
                "region",
                "supplier",
            ]:
                expected = sorted(
                    [
                        [str(c) for c in row]
                        for row in c.sql_query(f"SELECT * FROM {size}.{table}")
                    ]
                )
                actual_csv = []
                for obj in s3.list_objects(
                    "copytos3", f"test/{size}/csv/{table}/", recursive=True
                ):
                    assert obj.object_name is not None
                    response = s3.get_object("copytos3", obj.object_name)
                    f = StringIO(response.data.decode("utf-8"))
                    response.close()
                    response.release_conn()
                    del response
                    actual_csv.extend([row for row in csv.reader(f)])
                    del f
                actual_csv.sort()
                assert (
                    actual_csv == expected
                ), f"Table {table}\nActual:\n{actual_csv[0:3]}\n...\n\nExpected:\n{expected[0:3]}\n..."
                del actual_csv

                actual_parquet = []
                for obj in s3.list_objects(
                    "copytos3", f"test/{size}/parquet/{table}/", recursive=True
                ):
                    assert obj.object_name is not None
                    response = s3.get_object("copytos3", obj.object_name)
                    f = BytesIO(response.data)
                    response.close()
                    response.release_conn()
                    del response
                    actual_parquet.extend(
                        [
                            [str(c) for c in row.values()]
                            for row in pyarrow.parquet.read_table(f).to_pylist()
                        ]
                    )
                    del f
                actual_parquet.sort()
                assert (
                    actual_parquet == expected
                ), f"Table {table}\nActual:\n{actual_parquet[0:3]}\n...\n\nExpected:\n{expected[0:3]}\n..."
                del actual_parquet
