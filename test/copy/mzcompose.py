# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional tests for COPY commands:
- COPY TO S3 against a local minio service instead of real AWS S3
- COPY FROM STDIN
"""

import csv
import json
import random
import string
import threading
import time
from decimal import Decimal
from io import BytesIO, StringIO
from textwrap import dedent

import pyarrow.parquet  #
from minio import Minio

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service as DockerService
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
            "log_filter": "mz_storage_operators::s3_oneshot_sink=trace,info"
        },
    ),
    Testdrive(),
    Mc(),
    DockerService(
        name="redirect-server",
        config={
            "image": "python:3.12-slim",
            "volumes": ["./redirect_server.py:/app/redirect_server.py"],
            "command": ["python3", "-u", "/app/redirect_server.py"],
            "ports": [8080],
            "healthcheck": {
                "test": [
                    "CMD",
                    "python3",
                    "-c",
                    "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')",
                ],
                "interval": "2s",
                "start_period": "30s",
            },
        },
    ),
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
            "log_filter": "mz_storage_operators::s3_oneshot_sink=trace,info"
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
    Run all workflows during CI.

    Every workflow is run except for the exceptions below, so that a newly
    added regression test gets CI coverage automatically instead of silently
    needing to be added to a hand-maintained allowlist:
      - "default": meta-workflow that runs everything (would recurse).
      - "ci": this workflow itself (would recurse).
      - "nightly": heavy TPC-H suite run separately via the `nightly` pipeline
        step (`run: nightly`), not here.
    """
    excluded = {"default", "ci", "nightly"}

    def process(name: str) -> None:
        with c.test_case(name):
            c.workflow(name)

    c.test_parts([name for name in c.workflows.keys() if name not in excluded], process)


def workflow_auth(c: Composition) -> None:
    c.up(Service("mc", idle=True), "materialized", "minio")

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


def workflow_test_column_dedup(c: Composition):
    """
    Regression test: column name deduplication can produce duplicate names.
    For columns ["a", "a2", "a"], dedup produces ["a", "a2", "a2"] instead of
    unique names, causing data corruption in parquet output.
    """

    with c.override(Testdrive(no_reset=True)):
        c.up("materialized", "minio")

        c.testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

                > CREATE SECRET aws_secret_column_dedup AS '${arg.aws-secret-access-key}'
                > CREATE CONNECTION aws_conn_column_dedup
                  TO AWS (
                    ACCESS KEY ID = '${arg.aws-access-key-id}',
                    SECRET ACCESS KEY = SECRET aws_secret_column_dedup,
                    ENDPOINT = '${arg.aws-endpoint}',
                    REGION = 'us-east-1'
                  )

                > COPY (SELECT 1::int4 AS a, 2::int4 AS a, 3::int4 AS a2, 4::int4 AS a)
                  TO 's3://copytos3/test/column_dedup/'
                  WITH (AWS CONNECTION = aws_conn_column_dedup, FORMAT = 'parquet');

                $ s3-verify-data bucket=copytos3 key=test/column_dedup
                1 2 3 4
                """))


def workflow_test_github_9627(c: Composition):
    """
    Regression test for database-issues#9627, in which per-replica read holds
    for copy-to dataflows weren't correctly cleaned up, which led to compaction
    being disabled indefinitely for inputs to these dataflows.
    """

    with c.override(Testdrive(no_reset=True)):
        c.up("materialized", "minio")

        c.testdrive(dedent("""
                > CREATE TABLE t (a int)
                > INSERT INTO t VALUES (1)

                > CREATE SECRET aws_secret_github_9627 AS '${arg.aws-secret-access-key}'
                > CREATE CONNECTION aws_conn_github_9627
                  TO AWS (
                    ACCESS KEY ID = '${arg.aws-access-key-id}',
                    SECRET ACCESS KEY = SECRET aws_secret_github_9627,
                    ENDPOINT = '${arg.aws-endpoint}',
                    REGION = 'us-east-1'
                  )

                > COPY (SELECT * FROM t) TO 's3://copytos3/test/github_9627/'
                  WITH (AWS CONNECTION = aws_conn_github_9627, FORMAT = 'csv');
                """))

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


def workflow_test_ss_193(c: Composition):
    """
    Regression test for SS-193 where COPYing to a table from STDIN would
    store values without rounding them to the destination column's scale.

    The same rounding must also apply to the COPY FROM S3 paths, so we
    additionally round-trip scale-3 values through both a CSV and a parquet
    file on S3 and assert they are rounded to the destination column's scale
    on read.
    """
    c.up("materialized", "minio")
    conn = c.sql_connection()
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE numbers_with_precision (a DECIMAL(10, 2), b NUMERIC(10, 2))"
        )
        with cur.copy("COPY numbers_with_precision FROM STDIN") as copy:
            copy.write("10.447\t10.447\n")
        with cur.copy("COPY numbers_with_precision FROM STDIN (FORMAT CSV)") as copy:
            copy.write("10.447,10.447\n")

        cur.execute("SELECT a, b FROM numbers_with_precision")
        rows = cur.fetchall()
        assert rows == [
            (Decimal("10.45"), Decimal("10.45")),
            (Decimal("10.45"), Decimal("10.45")),
        ], f"COPY FROM STDIN did not round values to the column scale: {rows}"

        # Round-trip scale-3 values through CSV and parquet files on S3 and
        # assert COPY FROM rounds them to the destination column's scale on read.
        cur.execute("CREATE SECRET minio_secret AS 'minioadmin'")
        cur.execute("""
            CREATE CONNECTION aws_conn TO AWS (
                ACCESS KEY ID = 'minioadmin',
                SECRET ACCESS KEY = SECRET minio_secret,
                ENDPOINT = 'http://minio:9000/',
                REGION = 'us-east-1'
            )
            """)

        # Source carries scale-3 values that don't round evenly to scale 2.
        cur.execute("CREATE TABLE numbers_scale3 (a DECIMAL(10, 3), b NUMERIC(10, 3))")
        cur.execute("INSERT INTO numbers_scale3 VALUES (10.447, 10.447)")

        for format in ["csv", "parquet"]:
            cur.execute(
                f"COPY (SELECT a, b FROM numbers_scale3) "
                f"TO 's3://copytos3/test/ss_193/{format}' "
                f"WITH (AWS CONNECTION = aws_conn, FORMAT = '{format}')"
            )
            cur.execute(
                f"CREATE TABLE numbers_from_{format} (a DECIMAL(10, 2), b NUMERIC(10, 2))"
            )
            cur.execute(
                f"COPY INTO numbers_from_{format} "
                f"FROM 's3://copytos3/test/ss_193/{format}' "
                f"(FORMAT {format.upper()}, AWS CONNECTION = aws_conn)"
            )
            cur.execute(f"SELECT a, b FROM numbers_from_{format}")
            rows = cur.fetchall()
            assert rows == [
                (Decimal("10.45"), Decimal("10.45"))
            ], f"COPY FROM {format} did not round values to the column scale: {rows}"


def workflow_copy_from_ssrf_redirect(c: Composition) -> None:
    """Regression: COPY FROM must not follow redirects to private IPs."""
    c.up("materialized", "redirect-server")
    c.sql("CREATE TABLE ssrf_target (a text)")

    copy_succeeded = True
    try:
        c.sql(
            "COPY INTO ssrf_target FROM "
            "'http://redirect-server:8080/redirect-to-docker-ip' (FORMAT CSV)"
        )
    except Exception as e:
        err = e
        print(f"--- Err: {e}")
        copy_succeeded = False

    if copy_succeeded:
        rows = c.sql_query("SELECT a FROM ssrf_target")
        assert rows != [
            ("ssrf_bypass_confirmed",)
        ], "SSRF: redirect to private IP was followed and data was exfiltrated"
    else:
        assert (
            "302" in str(err) or "redirect" in str(err).lower()
        ), f"unexpected error: {err}"


def workflow_copy_from_csv_header(c: Composition) -> None:
    """Regression test: CSV COPY FROM STDIN with HEADER must not drop rows
    at chunk boundaries when input exceeds 32MB.

    The pgwire handler splits data into ~32MB chunks distributed round-robin
    to parallel workers. Previously each worker's CSV reader was configured
    with has_headers(true), causing the first data row of every chunk (after
    the first) to be silently dropped.
    """
    c.up("materialized")

    num_rows = 1_000_000
    chunk_size = 50_000
    # Pad value so each row is ~100 bytes total.
    pad = "a" * 80

    conn = c.sql_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE csv_header_test (id INT, val TEXT)")

        with cur.copy(
            "COPY csv_header_test FROM STDIN WITH (FORMAT CSV, HEADER true)"
        ) as copy:
            copy.write("id,val\n")
            for start in range(0, num_rows, chunk_size):
                end = min(start + chunk_size, num_rows)
                batch = "".join(f"{i},{pad}_{i:010d}\n" for i in range(start, end))
                copy.write(batch)

        cur.execute("SELECT count(*) FROM csv_header_test")
        row = cur.fetchone()
        assert row is not None
        count = row[0]
        assert count == num_rows, (
            f"Expected {num_rows} rows but got {count} — "
            f"lost {num_rows - count} rows (likely at 32MB chunk boundaries)"
        )

        cur.execute("SELECT val FROM csv_header_test WHERE id = 0")
        row = cur.fetchone()
        assert row is not None and row[0] == f"{pad}_0000000000"
        cur.execute("SELECT val FROM csv_header_test WHERE id = 999999")
        row = cur.fetchone()
        assert row is not None and row[0] == f"{pad}_0000999999"


def workflow_copy_from_csv_quoted_null(c: Composition) -> None:
    """Regression test: CSV COPY FROM STDIN must distinguish quoted from
    unquoted NULL markers per PostgreSQL semantics. An unquoted occurrence of
    the NULL marker is SQL NULL; a quoted occurrence is the literal string.
    """
    c.up("materialized")

    conn = c.sql_connection()
    with conn.cursor() as cur:
        # Default NULL marker (empty string): unquoted empty -> NULL,
        # quoted empty -> "".
        cur.execute("CREATE TABLE csv_null_default (a TEXT, b TEXT)")
        with cur.copy("COPY csv_null_default FROM STDIN WITH (FORMAT CSV)") as copy:
            copy.write('a,\nb,""\n"",c\n')

        cur.execute("SELECT a, b FROM csv_null_default ORDER BY a IS NULL, a = '', a")
        rows = cur.fetchall()
        assert rows == [
            ("a", None),
            ("b", ""),
            ("", "c"),
        ], f"unexpected rows with default NULL marker: {rows}"

        # Custom NULL marker "NULL": unquoted NULL -> SQL NULL,
        # quoted "NULL" -> the literal string "NULL".
        cur.execute("CREATE TABLE csv_null_custom (a TEXT, b TEXT)")
        with cur.copy(
            "COPY csv_null_custom FROM STDIN WITH (FORMAT CSV, NULL 'NULL')"
        ) as copy:
            copy.write('a,NULL\nb,"NULL"\nNULL,c\n')

        cur.execute("SELECT a, b FROM csv_null_custom ORDER BY a IS NULL, a = '', a")
        rows = cur.fetchall()
        assert rows == [
            ("a", None),
            ("b", "NULL"),
            (None, "c"),
        ], f"unexpected rows with custom NULL marker: {rows}"


def workflow_copy_from_csv_end_marker(c: Composition) -> None:
    """Regression test: CSV COPY FROM STDIN must distinguish a quoted "\\."
    data row from the bare \\. end-of-copy marker.

    The marker is recognized in the raw input bytes only; a CSV value whose
    *decoded* form is \\. (i.e. the quoted field "\\.") is data. Previously
    both the pgwire row scanner and the CSV decoder compared the decoded
    field bytes, so a quoted "\\." row would stop the import and silently
    drop any subsequent rows.
    """
    c.up("materialized")

    conn = c.sql_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE csv_end_marker_test (val TEXT)")

        # A quoted "\." row in the middle, followed by another row. With the
        # bug, the "\." row and "after" row are both silently dropped.
        with cur.copy("COPY csv_end_marker_test FROM STDIN WITH (FORMAT CSV)") as copy:
            copy.write('before\n"\\."\nafter\n')

        cur.execute("SELECT val FROM csv_end_marker_test ORDER BY val")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == ["\\.", "after", "before"], (
            f'quoted "\\." CSV row was misread as end-of-copy marker; '
            f"got {rows!r}, expected ['\\\\.', 'after', 'before']"
        )

        # Bare \. on its own line still terminates the COPY and discards
        # everything after it, matching PostgreSQL behavior.
        cur.execute("DELETE FROM csv_end_marker_test")
        with cur.copy("COPY csv_end_marker_test FROM STDIN WITH (FORMAT CSV)") as copy:
            copy.write("first\n\\.\nignored\n")

        cur.execute("SELECT val FROM csv_end_marker_test")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == [
            "first"
        ], f"bare \\. did not terminate COPY; got {rows!r}, expected ['first']"


def workflow_copy_from_csv_crlf(c: Composition) -> None:
    """Regression test: CSV COPY FROM STDIN must handle CRLF and CR line
    endings, not just LF.

    csv-core places the record boundary between \\r and \\n, so for CRLF input
    every non-first record's bytes begin with an orphaned \\n. Both the
    decoder's per-field quote probe and the pgwire scanner's end-of-copy marker
    check inspect raw bytes and were fooled by this: a quoted NULL marker
    corrupted to SQL NULL, and a quoted "\\." (plus every row after it) was
    silently dropped. RFC 4180 mandates CRLF and Windows/Excel exports use it,
    so the most standard CSV form was untested and broken.
    """
    c.up("materialized")

    conn = c.sql_connection()
    with conn.cursor() as cur:
        for label, eol in [("crlf", "\r\n"), ("cr", "\r")]:
            # Quoted vs unquoted NULL marker, including a quoted empty leading
            # field on an orphan-led (non-first) record. The dynamic SQL is
            # encoded to bytes so it satisfies psycopg's LiteralString-typed
            # query parameter.
            cur.execute(f"CREATE TABLE csv_{label}_null (a TEXT, b TEXT)".encode())
            with cur.copy(
                f"COPY csv_{label}_null FROM STDIN WITH (FORMAT CSV)".encode()
            ) as copy:
                copy.write(f'a,{eol}b,""{eol}"",c{eol}')
            cur.execute(
                f"SELECT a, b FROM csv_{label}_null "
                "ORDER BY a IS NULL, a = '', a".encode()
            )
            rows = cur.fetchall()
            assert rows == [
                ("a", None),
                ("b", ""),
                ("", "c"),
            ], f"{label}: quoted/unquoted NULL markers misread on CRLF/CR: {rows}"

            # Quoted "\." is data; a bare \. terminates and drops the rest.
            cur.execute(f"CREATE TABLE csv_{label}_marker (val TEXT)".encode())
            with cur.copy(
                f"COPY csv_{label}_marker FROM STDIN WITH (FORMAT CSV)".encode()
            ) as copy:
                copy.write(f'before{eol}"\\."{eol}after{eol}\\.{eol}ignored{eol}')
            cur.execute(f"SELECT val FROM csv_{label}_marker ORDER BY val".encode())
            rows = [r[0] for r in cur.fetchall()]
            assert rows == ["\\.", "after", "before"], (
                f"{label}: quoted \\. misread as marker or bare \\. not honored "
                f"on CRLF/CR; got {rows!r}"
            )


def workflow_copy_from_csv_crlf_large_end_marker(c: Composition) -> None:
    """Regression test: a bare \\. end-of-copy marker in CRLF input must drop
    every row after it, even when the COPY exceeds the 32MB batch size and is
    split across parallel workers.

    The pgwire scanner locates the marker and truncates the stream there. For
    CRLF the record boundary sits between \\r and \\n, so the raw marker span
    is "\\n\\.\\r"; a trailing-only strip missed it, leaving the stream
    untruncated. Below 32MB the single-chunk decoder still catches the marker,
    but past 32MB the buffer is split at row boundaries and round-robined to
    workers, so post-marker rows in chunks other than the marker's are imported
    anyway. Pre- and post-marker data here both exceed 32MB to force splits on
    each side of the marker; post-marker ids are >= rows_each_side so any leak
    is detectable via max(id).
    """
    c.up("materialized")

    # ~95-byte rows; 400k per side ~= 38MB, comfortably over the 32MB batch.
    rows_each_side = 400_000
    chunk_rows = 50_000
    pad = "a" * 80

    conn = c.sql_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE csv_crlf_marker_large (id INT, val TEXT)")

        with cur.copy(
            "COPY csv_crlf_marker_large FROM STDIN WITH (FORMAT CSV)"
        ) as copy:
            # Pre-marker rows: ids [0, rows_each_side).
            for start in range(0, rows_each_side, chunk_rows):
                end = min(start + chunk_rows, rows_each_side)
                copy.write("".join(f"{i},{pad}\r\n" for i in range(start, end)))
            # Bare \. end-of-copy marker (CRLF terminated).
            copy.write("\\.\r\n")
            # Post-marker rows: ids [rows_each_side, 2*rows_each_side). These
            # must all be discarded.
            for start in range(rows_each_side, 2 * rows_each_side, chunk_rows):
                end = min(start + chunk_rows, 2 * rows_each_side)
                copy.write("".join(f"{i},{pad}\r\n" for i in range(start, end)))

        cur.execute("SELECT count(*), max(id) FROM csv_crlf_marker_large")
        row = cur.fetchone()
        assert row is not None
        count, max_id = row
        assert count == rows_each_side and max_id == rows_each_side - 1, (
            "CRLF end-of-copy marker not honored across 32MB chunk boundaries: "
            f"got count={count}, max_id={max_id}; "
            f"expected count={rows_each_side}, max_id={rows_each_side - 1} "
            "(rows after the bare \\. leaked through parallel workers)"
        )


# Must satisfy _NUM_IDLE_SESSIONS * effective_cores >= 512 (blocking-pool cap) to
# re-starve SELECT 1 on a regression; 256 holds margin below the 4-core agent.
_NUM_IDLE_SESSIONS = 256
_SELECT_TIMEOUT_S = 30.0


def _select_1_responsive(c: Composition, timeout_s: float) -> bool:
    box: dict[str, object] = {}

    def run() -> None:
        try:
            conn = c.sql_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchall()
                box["ok"] = True
            finally:
                conn.close()
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout_s)
    if t.is_alive():
        return False
    if box.get("ok"):
        return True
    raise AssertionError(f"SELECT 1 probe errored unexpectedly: {box.get('err')!r}")


def _open_idle_copy(c: Composition) -> tuple:
    conn = c.sql_connection()
    cur = conn.cursor()
    cm = cur.copy("COPY copy_idle_target FROM STDIN")
    cm.__enter__()
    return (conn, cur, cm)


def _close_idle_copies(held: list) -> None:
    for conn, _cur, cm in held:
        try:
            conn.close()
        except Exception:
            pass
        gen = getattr(cm, "gen", None)
        if gen is not None:
            try:
                gen.close()
            except Exception:
                pass
    held.clear()


def workflow_copy_from_stdin_many_idle_sessions(c: Composition) -> None:
    """Many idle COPY FROM STDIN sessions must not prevent other queries from
    running."""
    c.up("materialized")

    setup_conn = c.sql_connection()
    with setup_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS copy_idle_target")
        cur.execute("CREATE TABLE copy_idle_target (a int4)")
    setup_conn.close()

    held: list[tuple] = []
    try:
        for _ in range(_NUM_IDLE_SESSIONS):
            held.append(_open_idle_copy(c))
        assert _select_1_responsive(c, _SELECT_TIMEOUT_S), (
            f"SELECT 1 did not return within {_SELECT_TIMEOUT_S}s while "
            f"{len(held)} idle COPY FROM STDIN sessions were open"
        )
    finally:
        _close_idle_copies(held)
