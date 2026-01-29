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
import os
import random
import string
import time
from io import BytesIO, StringIO
from textwrap import dedent

import boto3
import pyarrow.parquet
from minio import Minio

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
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
        help="Use SIZE 'scale=N,workers=N' for replicas and SIZE 'scale=N,workers=1' for sources",
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


def workflow_gcs(c: Composition) -> None:
    """
    Test COPY TO S3 with GCS using HMAC authentication.  This test requires access to GCS.
    """
    # We can't use the current GCP_SERVICE_ACCOUNT_JSON as that is a credentials file used by GCP
    # SDK. See <https://cloud.google.com/docs/authentication/application-default-credentials>
    #
    # We need HMAC keys, which contain access and secret keys (these don't exist in CI today)
    # see <https://cloud.google.com/storage/docs/authentication/hmackeys>

    def env_get_or_fail(key: str) -> str:
        value = os.environ.get(key)
        assert value is not None, f"{key} environment variable must be set"
        return value

    gcs_bucket = env_get_or_fail("GCS_BUCKET")
    gcs_region = env_get_or_fail("GCS_REGION")
    gcs_access_key = env_get_or_fail("GCS_ACCESS_KEY")
    gcs_secret_key = env_get_or_fail("GCS_SECRET_KEY")
    gcs_endpoint = os.environ.get("GCS_ENDPOINT", "https://storage.googleapis.com")

    key_prefix = f"copy_to/{make_random_key(10)}/"

    s3 = boto3.client(
        "s3",
        endpoint_url=gcs_endpoint,
        region_name=gcs_region,
        aws_access_key_id=gcs_access_key,
        aws_secret_access_key=gcs_secret_key,
    )
    contents = []
    try:
        with c.override(Testdrive(no_reset=True)):
            c.up("materialized")
            c.testdrive(
                dedent(
                    f"""
                    > CREATE SECRET gcs_secret AS '{gcs_secret_key}';
                    > CREATE CONNECTION gcs_conn
                      TO AWS (
                        ACCESS KEY ID = '{gcs_access_key}',
                        SECRET ACCESS KEY = SECRET gcs_secret,
                        ENDPOINT = '{gcs_endpoint}',
                        REGION = '{gcs_region}'
                      );
                    > CREATE TABLE t (a int, b text, c float);
                    > INSERT INTO t VALUES (1, 'a', 1.1), (2, 'b', 2.2);

                    > COPY (SELECT * FROM t)
                      TO 's3://{gcs_bucket}/{key_prefix}'
                      WITH (
                        AWS CONNECTION = gcs_conn, FORMAT = 'csv', HEADER
                      );
                    """
                )
            )
        # list should contain a single file (we won't know the name ahead of time, just the prefix).
        listing = s3.list_objects_v2(Bucket=gcs_bucket, Prefix=key_prefix)

        contents = listing["Contents"]
        assert len(contents) == 1, f"expected 1 file, got {contents}"

        key = contents[0]["Key"]
        assert key.endswith(".csv"), f"expected .csv suffix, got {key}"

        object_response = s3.get_object(Bucket=gcs_bucket, Key=key)
        body = object_response["Body"].read().decode("utf-8")
        csv_reader = csv.DictReader(StringIO(body))
        actual = [row for row in csv_reader]

        # all fields are read in as strings
        expected = [{"a": "1", "b": "a", "c": "1.1"}, {"a": "2", "b": "b", "c": "2.2"}]
        assert actual == expected, f"actual: {actual} != expected: {expected}"

    finally:
        # This is a best effort cleanup, as the process may exit before reaching this point.
        for obj in contents:
            key = obj["Key"]
            s3.delete_object(Bucket=gcs_bucket, Key=key)


def workflow_ci(c: Composition, _parser: WorkflowArgumentParser) -> None:
    """
    Workflows to run during CI
    """
    for name in ["auth", "http", "gcs"]:
        with c.test_case(name):
            c.workflow(name)


def make_random_key(n: int):
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(n)
    )


def workflow_auth(c: Composition) -> None:
    c.up(Service("mc", idle=True), "materialized", "minio")

    # Set up IAM credentials for 3 users with different permissions levels to S3:
    # User 'readwritedelete': PutObject, ListBucket, DeleteObject
    # User 'nodelete': PutObject, ListBucket
    # User 'read': GetObject, ListBucket

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


def workflow_test_github_9627(c: Composition):
    """
    Regression test for database-issues#9627, in which per-replica read holds
    for copy-to dataflows weren't correctly cleaned up, which led to compaction
    being disabled indefinitely for inputs to these dataflows.
    """

    with c.override(Testdrive(no_reset=True)):
        c.up("materialized", "minio")

        c.testdrive(
            dedent(
                """
                > CREATE TABLE t (a int)
                > INSERT INTO t VALUES (1)

                > CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'
                > CREATE CONNECTION aws_conn
                  TO AWS (
                    ACCESS KEY ID = '${arg.aws-access-key-id}',
                    SECRET ACCESS KEY = SECRET aws_secret,
                    ENDPOINT = '${arg.aws-endpoint}',
                    REGION = 'us-east-1'
                  )

                > COPY (SELECT * FROM t) TO 's3://copytos3/test/github_9627/'
                  WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv');
                """
            )
        )

        # Check that the table's read frontier still advances.

        query = """
            SELECT f.read_frontier
            FROM mz_internal.mz_frontiers f
            JOIN mz_tables t ON t.id = f.object_id
            WHERE t.name = 't'
            """

        # Because `mz_frontiers` isn't a linearizable relation it's possible that
        # we need to wait a bit for the object's frontier to show up.
        result = c.sql_query(query)
        tries = 1
        while not result and tries < 3:
            time.sleep(1)
            result = c.sql_query(query)
            tries += 1

        before = int(result[0][0])
        time.sleep(3)

        result = c.sql_query(query)
        after = int(result[0][0])

        assert before < after, f"read frontier is stuck, {before} >= {after}"
