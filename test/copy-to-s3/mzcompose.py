# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional tests for the COPY TO S3 command against a local minio service
instead of a real AWS S3.
"""

import csv
import json
import random
import string
from io import BytesIO, StringIO

import pyarrow.parquet  #
from minio import Minio

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc
from materialize.mzcompose.services.minio import Minio as MinioService
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    MinioService(
        additional_directories=["copytos3"],
        ports=["9000:9000", "9001:9001"],
        allow_host_ports=True,
        in_memory=None,
    ),
    Mz(app_password=""),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage_operators::s3_oneshot_sink=trace,debug,info,warn"
        },
    ),
    Testdrive(),
    Mc(),
]


def workflow_default(c: Composition) -> None:
    """
    Run all workflows (including nightly and CI workflows)
    """

    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_nightly(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Run only during the nightly
    """
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
        no_reset=True,
    )

    materialized = Materialized(
        default_size=args.default_size,
        additional_system_parameter_defaults={
            "log_filter": "mz_storage_operators::s3_oneshot_sink=trace,debug,info,warn"
        },
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
        c.run_testdrive_files("nightly/setup.td")

        for size in [
            "tpch10mb",
            "tpch1gb",
            # "tpch10gb",  # SELECT(*) in Mz is way too slow
        ]:
            c.run_testdrive_files(f"nightly/{size}.td")
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


def workflow_ci(c: Composition, _parser: WorkflowArgumentParser) -> None:
    """
    Workflows to run during CI
    """
    for name in ["auth", "http"]:
        with c.test_case(name):
            c.workflow(name)


def workflow_auth(c: Composition) -> None:
    c.up({"name": "mc", "persistent": True})
    c.up("materialized", "minio")

    # Set up IAM credentials for 3 users with different permissions levels to S3:
    # User 'readwritedelete': PutObject, ListBucket, DeleteObject
    # User 'nodelete': PutObject, ListBucket
    # User 'read': GetObject, ListBucket

    def make_random_key(n: int):
        return "".join(
            random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(n)
        )

    def make_user(username: str, actions: list[str]):
        return (
            username,
            make_random_key(10),
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": actions,
                        "Resource": ["arn:aws:s3:::*"],
                    }
                ],
            },
        )

    users = [
        make_user(username, actions)
        for username, actions in [
            ("readwritedelete", ["s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]),
            ("nodelete", ["s3:PutObject", "s3:ListBucket"]),
            ("read", ["s3:GetObject", "s3:ListBucket"]),
        ]
    ]

    minio_alias = "s3test"
    c.exec(
        "mc",
        "mc",
        "alias",
        "set",
        minio_alias,
        "http://minio:9000/",
        "minioadmin",
        "minioadmin",
    )
    # create a user, policy, and policy attachment for each user
    testdrive_args = []
    for user in users:
        c.exec(
            "mc",
            "mc",
            "admin",
            "user",
            "add",
            minio_alias,
            user[0],
            user[1],
        )
        c.exec("mc", "cp", "/dev/stdin", f"/tmp/{user[0]}", stdin=json.dumps(user[2]))
        c.exec(
            "mc",
            "mc",
            "admin",
            "policy",
            "create",
            minio_alias,
            user[0],
            f"/tmp/{user[0]}",
        )
        c.exec(
            "mc",
            "mc",
            "admin",
            "policy",
            "attach",
            minio_alias,
            user[0],
            "--user",
            user[0],
        )
        testdrive_args.append(f"--var=s3-user-{user[0]}-secret-key={user[1]}")

    c.run_testdrive_files(
        *testdrive_args,
        "s3-auth-checks.td",
    )


def workflow_http(c: Composition) -> None:
    """Test http endpoint allows COPY TO S3 but not COPY TO STDOUT."""
    c.up("materialized", "minio")

    with c.override(Testdrive(no_reset=True)):
        c.run_testdrive_files("http/setup.td")

        result = c.exec(
            "materialized",
            "curl",
            "http://localhost:6878/api/sql",
            "-k",
            "-s",
            "--header",
            "Content-Type: application/json",
            "--data",
            "{\"query\": \"COPY (SELECT 1) TO 's3://copytos3/test/http/' WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv');\"}",
            capture=True,
        )
        assert result.returncode == 0
        assert (
            json.loads(result.stdout)["results"][0]["error"]["message"]
            == 'permission denied for CONNECTION "materialize.public.aws_conn"'
            and json.loads(result.stdout)["results"][0]["error"]["detail"]
            == "The 'anonymous_http_user' role needs USAGE privileges on CONNECTION \"materialize.public.aws_conn\""
        )

        c.run_testdrive_files("http/grant.td")
        result = c.exec(
            "materialized",
            "curl",
            "http://localhost:6878/api/sql",
            "-k",
            "-s",
            "--header",
            "Content-Type: application/json",
            "--data",
            "{\"query\": \"COPY (SELECT 1) TO 's3://copytos3/test/http/' WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv');\"}",
            capture=True,
        )
        assert result.returncode == 0
        assert json.loads(result.stdout)["results"][0]["ok"] == "COPY 1"

        result = c.exec(
            "materialized",
            "curl",
            "http://localhost:6878/api/sql",
            "-k",
            "-s",
            "--header",
            "Content-Type: application/json",
            "--data",
            '{"query": "COPY (SELECT 1) TO STDOUT"}',
            capture=True,
        )
        assert result.returncode == 0
        assert "unsupported via this API" in result.stdout
