# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import os
import ssl
import time
import urllib.parse
from textwrap import dedent

import pg8000
from pg8000.exceptions import InterfaceError

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Testdrive(),  # Overriden below
    Mz(app_password=""),  # Overridden below
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    REGION = "aws/us-east-1"
    ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
    USERNAME = os.getenv("PRODUCTION_SANDBOX_USERNAME", "infra+bot@materialize.com")
    APP_PASSWORD = os.environ["PRODUCTION_SANDBOX_APP_PASSWORD"]

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
                materialize_url=f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{host}:6875/materialize",
            ),
        ):
            c.up("testdrive", persistent=True)

            while time.time() - start_time < args.runtime:
                try:
                    c.testdrive(
                        dedent(
                            """
                                > DELETE FROM qa_canary_environment.public_table.table
                            """
                        )
                    )

                    conn1 = pg8000.connect(
                        host=host,
                        user=USERNAME,
                        password=APP_PASSWORD,
                        port=6875,
                        ssl_context=ssl.create_default_context(),
                    )
                    cursor_table = conn1.cursor()
                    cursor_table.execute("BEGIN")
                    cursor_table.execute(
                        "DECLARE subscribe_table CURSOR FOR SUBSCRIBE (SELECT * FROM qa_canary_environment.public_table.table)"
                    )

                    conn2 = pg8000.connect(
                        host=host,
                        user=USERNAME,
                        password=APP_PASSWORD,
                        port=6875,
                        ssl_context=ssl.create_default_context(),
                    )
                    cursor_mv = conn2.cursor()
                    cursor_mv.execute("BEGIN")
                    cursor_mv.execute(
                        "DECLARE subscribe_mv CURSOR FOR SUBSCRIBE (SELECT * FROM qa_canary_environment.public_table.table_mv)"
                    )

                    i = 0

                    while time.time() - start_time < args.runtime:
                        current_time = time.time()

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

                                    > SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_pg_cdc.wmr WHERE degree > 3
                                    0

                                    > SELECT COUNT(DISTINCT a_name) FROM qa_canary_environment.public_mysql_cdc.mysql_wmr WHERE degree > 3
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

                        cursor_table.execute(
                            "FETCH ALL subscribe_table WITH (timeout='5s')"
                        )
                        results = cursor_table.fetchall()
                        assert len(results) == 100, f"Unexpected results: {results}"
                        for result in results:
                            assert (
                                int(result[0]) >= current_time
                            ), f"Unexpected results: {results}"
                            assert int(result[1]) == 1, f"Unexpected results: {results}"
                            assert (
                                i * 100 <= int(result[2]) < (i + 1) * 100
                            ), f"Unexpected results: {results}"

                        cursor_mv.execute("FETCH ALL subscribe_mv WITH (timeout='5s')")
                        results = cursor_mv.fetchall()
                        # First the removal, then the addition if it happens at the same timestamp
                        r = sorted(list(results))  # type: ignore
                        if i == 0:
                            assert len(r) == 1, f"Unexpected results: {r}"
                        else:
                            assert len(r) == 2, f"Unexpected results: {r}"
                            assert (
                                int(r[0][0]) >= current_time
                            ), f"Unexpected results: {r}"
                            assert int(r[0][1]) == -1, f"Unexpected results: {r}"
                            assert (
                                int(r[0][2]) == i * 100 - 1
                            ), f"Unexpected results: {r}"
                        assert int(r[-1][0]) >= current_time, f"Unexpected results: {r}"
                        assert int(r[-1][1]) == 1, f"Unexpected results: {r}"
                        assert (
                            int(r[-1][2]) == (i + 1) * 100 - 1
                        ), f"Unexpected results: {r}"

                        i += 1

                    cursor_table.execute("CLOSE subscribe_table")
                    cursor_table.execute("ROLLBACK")
                    cursor_table.close()
                    conn1.close()

                    cursor_mv.execute("CLOSE subscribe_mv")
                    cursor_mv.execute("ROLLBACK")
                    cursor_mv.close()
                    conn2.close()

                except InterfaceError as e:
                    if "network error" in str(e):
                        print(
                            "Network error received, probably a cloud downtime, retrying in 1 min"
                        )
                        time.sleep(60)
                    else:
                        raise
