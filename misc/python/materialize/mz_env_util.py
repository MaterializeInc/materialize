# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""mz env util"""

import psycopg

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mz import Mz


def get_cloud_hostname(
    c: Composition,
    app_password: str,
    region: str = "aws/us-east-1",
    environment: str = "production",
    quiet: bool = False,
) -> str:
    with c.override(
        Mz(region=region, environment=environment, app_password=app_password)
    ):
        return c.cloud_hostname(quiet=quiet)


def print_environment_id(conn: psycopg.Connection, indent: str = "") -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT mz_environment_id()")
            row = cur.fetchone()
        env_id = row[0] if row else "<unknown>"
        print(f"{indent}mz_environment_id() = {env_id}")
    except Exception as e:
        print(f"{indent}WARNING: failed to read mz_environment_id(): {e}")


def connect_and_print_environment_id(
    host: str,
    user: str | None,
    password: str | None,
    port: int = 6875,
    dbname: str = "materialize",
) -> None:
    try:
        with psycopg.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            dbname=dbname,
            sslmode="require",
        ) as conn:
            print_environment_id(conn)
    except Exception as e:
        print(f"WARNING: failed to connect to {host}: {e}")
