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

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive

REGION = "aws/us-east-1"
ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
USERNAME = os.getenv("PRODUCTION_SANDBOX_USERNAME", "infra+bot@materialize.com")
APP_PASSWORD = os.environ["PRODUCTION_SANDBOX_APP_PASSWORD"]


def hostname(c: Composition) -> str:
    print("Obtaining hostname of staging instance ...")
    region_status = c.run("mz", "region", "show", capture=True)
    sql_line = region_status.stdout.split("\n")[2]
    cloud_url = sql_line.split("\t")[1].strip()
    # It is necessary to append the 'https://' protocol; otherwise, urllib can't parse it correctly.
    cloud_hostname = urllib.parse.urlparse("https://" + cloud_url).hostname
    return str(cloud_hostname)


SERVICES = [
    Testdrive(),  # Overriden below
    Mz(region=REGION, environment=ENVIRONMENT, app_password=APP_PASSWORD),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    args = parser.parse_args()

    start_time = time.time()

    host = hostname(c)

    with c.override(
        Testdrive(
            no_reset=True,
            materialize_url=f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{host}:6875/materialize",
        )
    ):
        c.up("testdrive", persistent=True)

        c.testdrive(
            dedent(
                """
        > DELETE FROM qa_canary_environment.public_table.table
        """
            )
        )

        conn = pg8000.connect(
            host=host,
            user=USERNAME,
            password=APP_PASSWORD,
            port=6875,
            ssl_context=ssl.create_default_context(),
        )
        cursor = conn.cursor()

        cursor.execute("BEGIN")
        cursor.execute(
            "DECLARE subscribe CURSOR FOR SUBSCRIBE (SELECT * FROM qa_canary_environment.public_table.table)"
        )

        i = 0

        while time.time() - start_time < args.runtime:
            current_time = time.time()

            c.testdrive(
                dedent(
                    f"""
            > SELECT 1
            1

            > INSERT INTO qa_canary_environment.public_table.table VALUES ({i})

            > SELECT * FROM qa_canary_environment.public_tpch.tpch_q01 WHERE sum_charge < 0

            > SELECT * FROM qa_canary_environment.public_tpch.tpch_q18 WHERE o_orderdate >= '2023-01-01'

            > SELECT * FROM qa_canary_environment.public_pg_cdc.wmr WHERE degree > 2

            > SELECT * FROM qa_canary_environment.public_loadgen.sales_product_product_category WHERE count_distinct_product_id < 0

            > SELECT max(c) FROM qa_canary_environment.public_table.table
            {i}
            """
                )
            )

            cursor.execute("FETCH ALL subscribe WITH (timeout='10s')")

            results = cursor.fetchall()
            assert len(results) == 1, f"Unexpected results: {results}"
            assert int(results[0][0]) >= current_time, f"Unexpected results: {results}"
            assert int(results[0][1]) == 1, f"Unexpected results: {results}"
            assert int(results[0][2]) == i, f"Unexpected results: {results}"

            i += 1

        cursor.execute("CLOSE subscribe")
        cursor.execute("ROLLBACK")
        cursor.close()
        conn.close()
