# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import datetime
import os
import ssl
import time
import urllib.parse
from textwrap import dedent

import pg8000
import requests
from pg8000 import Connection, Cursor
from pg8000.exceptions import InterfaceError

from materialize.cloudtest.util.jwt_key import fetch_jwt
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)

SERVICES = [
    Testdrive(),  # Overridden below
    Mz(app_password=""),  # Overridden below
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    REGION = "aws/us-east-1"
    ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
    USERNAME = os.getenv(
        "CANARY_LOADTEST_USERNAME", "infra+qacanaryload@materialize.io"
    )
    PASSWORD = os.environ["CANARY_LOADTEST_PASSWORD"]
    APP_PASSWORD = os.environ["CANARY_LOADTEST_APP_PASSWORD"]

    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    args = parser.parse_args()

    start_time = time.time()

    with c.override(
        Mz(region=REGION, environment=ENVIRONMENT, app_password=APP_PASSWORD)
    ):
        host = c.cloud_hostname()

        with c.override(
            Testdrive(
                no_reset=True,
                no_consistency_checks=True,  # No access to HTTP for coordinator check
                materialize_url=f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{host}:6875/materialize",
                default_timeout="600s",
            ),
        ):
            c.up("testdrive", persistent=True)

            failures = []

            while time.time() - start_time < args.runtime:
                try:
                    c.testdrive(
                        dedent(
                            """
                                > DELETE FROM qa_canary_environment.public_table.table
                            """
                        )
                    )

                    conn1, cursor_on_table = create_connection_and_cursor(
                        host,
                        USERNAME,
                        APP_PASSWORD,
                        "DECLARE subscribe_table CURSOR FOR SUBSCRIBE (SELECT * FROM qa_canary_environment.public_table.table)",
                    )
                    conn2, cursor_on_mv = create_connection_and_cursor(
                        host,
                        USERNAME,
                        APP_PASSWORD,
                        "DECLARE subscribe_mv CURSOR FOR SUBSCRIBE (SELECT * FROM qa_canary_environment.public_table.table_mv)",
                    )

                    i = 0
                    while time.time() - start_time < args.runtime:
                        print(f"Running iteration {i}")
                        c.override_current_testcase_name(
                            f"iteration {i} of workflow_default"
                        )
                        perform_test(
                            c,
                            host,
                            USERNAME,
                            PASSWORD,
                            cursor_on_table,
                            cursor_on_mv,
                            i,
                        )
                        i += 1

                    close_connection_and_cursor(
                        conn1, cursor_on_table, "subscribe_table"
                    )
                    close_connection_and_cursor(conn2, cursor_on_mv, "subscribe_mv")

                except InterfaceError as e:
                    if "network error" in str(e):
                        print(
                            "Network error received, probably a cloud downtime, retrying in 1 min"
                        )
                        time.sleep(60)
                    else:
                        raise
                except FailedTestExecutionError as e:
                    assert len(e.errors) > 0, "Exception contains not errors"
                    for error in e.errors:
                        print(
                            f"Test failure occurred ('{error.message}'), collecting it, and continuing."
                        )
                    # collect, continue, and rethrow at the end
                    failures.extend(e.errors)

            if len(failures) > 0:
                raise FailedTestExecutionError(
                    error_summary="SQL failures occurred",
                    errors=failures,
                )


def fetch_token(user_name: str, password: str) -> str:
    return fetch_jwt(
        email=user_name,
        password=password,
        host="admin.cloud.materialize.com/frontegg",
        scheme="https",
        max_tries=1,
    )


def http_sql_query(host: str, query: str, token: str) -> list[list[str]]:
    try:
        r = requests.post(
            f'https://{host}/api/sql?options={{"application_name":"canary-load","cluster":"qa_canary_environment_compute"}}',
            headers={"authorization": f"Bearer {token}"},
            json={"queries": [{"params": [], "query": query}]},
        )
    except requests.exceptions.HTTPError as e:
        res = e.response
        print(f"{e}\n{res}\n{res.text}")
        raise
    assert r.status_code == 200, f"{r}\n{r.text}"
    results = r.json()["results"]
    assert len(results) == 1, results

    if "rows" not in results[0].keys():
        assert "error" in results[0].keys()
        error = results[0]["error"]

        details = f"Occurred at {datetime.datetime.now()}."
        if "notices" in results[0].keys():
            notices = results[0]["notices"]
            if not (type(notices) == list and len(notices) == 0):
                details = f"{details} Notices: {notices}"

        raise FailedTestExecutionError(
            error_summary="SQL query failed",
            errors=[TestFailureDetails(message=error, details=details)],
        )

    return results[0]["rows"]


def create_connection_and_cursor(
    host: str, user_name: str, app_password: str, cursor_statement: str
) -> tuple[Connection, Cursor]:
    conn = pg8000.connect(
        host=host,
        user=user_name,
        password=app_password,
        port=6875,
        ssl_context=ssl.create_default_context(),
    )
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    cursor.execute(cursor_statement)

    return conn, cursor


def close_connection_and_cursor(
    connection: Connection, cursor: Cursor, object_to_close: str
) -> None:
    cursor.execute(f"CLOSE {object_to_close}")
    cursor.execute("ROLLBACK")
    cursor.close()
    connection.close()


def perform_test(
    c: Composition,
    host: str,
    user_name: str,
    password: str,
    cursor_on_table: Cursor,
    cursor_on_mv: Cursor,
    i: int,
) -> None:
    current_time = time.time()
    update_data(c, i)

    validate_cursor_on_table(cursor_on_table, current_time, i)
    validate_cursor_on_mv(cursor_on_mv, current_time, i)

    # Token can run out, so refresh it occasionally
    token = fetch_token(user_name, password)

    validate_data(
        host,
        token,
        i,
    )


def update_data(c: Composition, i: int) -> None:
    c.testdrive(
        dedent(
            f"""
                > SELECT 1
                1

                > INSERT INTO qa_canary_environment.public_table.table VALUES {", ".join(f"({i*100+j})" for j in range(100))}

                > SELECT COUNT(DISTINCT l_returnflag) FROM qa_canary_environment.public_tpch.tpch_q01 WHERE sum_charge < 0
                0

                > SELECT COUNT(DISTINCT c_name) FROM qa_canary_environment.public_tpch.tpch_q18 WHERE o_orderdate >= '2023-01-01'
                0

                > SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_pg_cdc.wmr WHERE degree > 10
                0

                > SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_mysql_cdc.mysql_wmr WHERE degree > 10
                0

                > SELECT COUNT(DISTINCT count_star) FROM qa_canary_environment.public_loadgen.sales_product_product_category WHERE count_distinct_product_id < 0
                0

                > SELECT * FROM qa_canary_environment.public_table.table_mv
                {i * 100 + 99}

                > SELECT min(c), max(c), count(*) FROM qa_canary_environment.public_table.table
                0 {i * 100 + 99} {(i + 1) * 100}
            """
        )
    )


def validate_cursor_on_table(
    cursor_on_table: Cursor,
    current_time: float,
    i: int,
) -> None:
    cursor_on_table.execute("FETCH ALL subscribe_table WITH (timeout='5s')")
    results = cursor_on_table.fetchall()
    assert len(results) == 100, f"Unexpected results: {results}"
    for result in results:
        assert int(result[0]) >= current_time, f"Unexpected results: {results}"
        assert int(result[1]) == 1, f"Unexpected results: {results}"
        assert (
            i * 100 <= int(result[2]) < (i + 1) * 100
        ), f"Unexpected results: {results}"


def validate_cursor_on_mv(
    cursor_on_mv: Cursor,
    current_time: float,
    i: int,
) -> None:
    cursor_on_mv.execute("FETCH ALL subscribe_mv WITH (timeout='5s')")
    results = cursor_on_mv.fetchall()
    # First the removal, then the addition if it happens at the same timestamp
    r = sorted(list(results))  # type: ignore
    if i == 0:
        assert len(r) == 1, f"Unexpected results: {r}"
    else:
        assert len(r) % 2 == 0, f"Unexpected results: {r}"
        assert int(r[-2][0]) >= current_time, f"Unexpected results: {r}"
        assert int(r[-2][1]) == -1, f"Unexpected results: {r}"
        assert int(r[-2][2]) == i * 100 - 1, f"Unexpected results: {r}"
    assert int(r[-1][0]) >= current_time, f"Unexpected results: {r}"
    assert int(r[-1][1]) == 1, f"Unexpected results: {r}"
    assert int(r[-1][2]) == (i + 1) * 100 - 1, f"Unexpected results: {r}"


def validate_data(
    host: str,
    token: str,
    i: int,
) -> None:
    result = http_sql_query(host, "SELECT 1", token)
    assert result == [["1"]]

    result = http_sql_query(
        host,
        "SELECT COUNT(DISTINCT l_returnflag) FROM qa_canary_environment.public_tpch.tpch_q01 WHERE sum_charge < 0",
        token,
    )
    assert result == [["0"]]

    result = http_sql_query(
        host,
        "SELECT COUNT(DISTINCT c_name) FROM qa_canary_environment.public_tpch.tpch_q18 WHERE o_orderdate >= '2023-01-01'",
        token,
    )
    assert result == [["0"]]

    result = http_sql_query(
        host,
        "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_pg_cdc.wmr WHERE degree > 10",
        token,
    )
    assert result == [["0"]]

    result = http_sql_query(
        host,
        "SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_mysql_cdc.mysql_wmr WHERE degree > 10",
        token,
    )
    assert result == [["0"]]

    result = http_sql_query(
        host,
        "SELECT COUNT(DISTINCT count_star) FROM qa_canary_environment.public_loadgen.sales_product_product_category WHERE count_distinct_product_id < 0",
        token,
    )
    assert result == [["0"]]

    result = http_sql_query(
        host,
        "SELECT * FROM qa_canary_environment.public_table.table_mv",
        token,
    )
    assert result == [[f"{i * 100 + 99}"]]

    result = http_sql_query(
        host,
        "SELECT min(c), max(c), count(*) FROM qa_canary_environment.public_table.table",
        token,
    )
    assert result == [["0", f"{i * 100 + 99}", f"{(i + 1) * 100}"]]
