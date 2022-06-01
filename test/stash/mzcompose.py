# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Postgres, Testdrive

SERVICES = [
    Materialized(),
    Postgres(),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    materialized = Materialized(
        options=["--catalog-postgres-stash", "postgres://postgres:postgres@postgres"],
    )
    postgres = Postgres(image="postgres:13.6")
    testdrive = Testdrive()

    with c.override(materialized, testdrive, postgres):
        c.up("postgres")
        c.wait_for_postgres()
        c.start_and_wait_for_tcp(services=["materialized"])
        c.wait_for_materialized("materialized")

        c.sql("CREATE TABLE a (i INT)")

        c.stop("postgres")
        c.up("postgres")
        c.wait_for_postgres()

        c.sql("CREATE TABLE b (i INT)")

        c.rm("postgres", stop=True, destroy_volumes=True)
        c.up("postgres")
        c.wait_for_postgres()

        # Postgres cleared its database, so this should fail.
        try:
            c.sql("CREATE TABLE c (i INT)")
            raise Exception("expected unreachable")
        except Exception as e:
            # Depending on timing, either of these errors can occur. The stash error comes
            # from the stash complaining. The network error comes from pg8000 complaining
            # because materialize panic'd.
            if "stash error: postgres: db error" not in str(
                e
            ) and "network error" not in str(e):
                raise e
