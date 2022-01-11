# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import sys

import sqlparse
from pg8000.dbapi import Connection  # type: ignore

from materialize.query_fitness.all_parts_essential import AllPartsEssential

threshold = 0.5


def main() -> None:
    conn = Connection("pstoev", password="pstoev")
    fitness_func = AllPartsEssential(conn=conn)

    for query in sys.stdin:
        fitness = fitness_func.fitness(query)

        if fitness > 0.5:
            dump_slt(conn, query)


def dump_slt(conn: Connection, query: str) -> None:
    query = sqlparse.format(query.rstrip(), reindent=True, keyword_case="upper")  # type: ignore
    cursor = conn.cursor()
    cursor.execute("ROLLBACK")
    cursor.execute(query)
    row = cursor.fetchone()
    cols = len(row)
    colspec = "I" * cols
    print(
        f"""

query {colspec}
{query}
----
9999999999 values hashing to YY

query T multiline
EXPLAIN {query}
----
EOF"""
    )


if __name__ == "__main__":
    main()
