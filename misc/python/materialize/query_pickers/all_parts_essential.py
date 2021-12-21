# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


"""
Test that all parts of the query are important. This is done by
commenting out parts of the query -- if any part of the query
can be commented out without this affecting the result of the query
this means that the query contains constructs and predicates that
do not contribute to the final result in any way.

On the other hand, if all parts of the query are deemed essential,
the query is such that if any part of it is lost during optimization
or execution, the entire query will start producing a different result.
Such queries are suitable for inclusion in regression tests
"""

import sys
from typing import Optional

import psycopg2

conn = psycopg2.connect("dbname=pstoev user=pstoev")

cur = conn.cursor()


def result_checksum(query: str) -> Optional[str]:
    """Execute the query and return a 'checksum' of the result.
    In this implementation, the checksum is simply the serialization of the entire result
    """
    try:
        cur.execute("COMMIT")  # type: ignore
        cur.execute(query)  # type: ignore
        all = cur.fetchall()
        return str(all)
    except:
        return None


def test_query(query: str) -> bool:
    """Test if all parts of a query are essential to producing the same result. This is done
    by removing parts of the query and checking if the result is the same. If it is, then
    the query contains a non-essential part and is thus rejected.
    """
    orig_result = result_checksum(query)

    tokens = query.split(" ")
    tokens = [x for x in tokens if x]
    l = len(tokens)

    for start_token in reversed(range(0, l)):
        for end_token in reversed(range(start_token, l)):
            new_tokens = [*tokens]
            new_tokens.insert(end_token + 1, " */ ")
            new_tokens.insert(start_token, " /* ")

            new_query = " ".join(new_tokens)
            new_result = result_checksum(new_query)

            if new_result == orig_result:
                return False

    print(query)
    return True


def run() -> None:
    for query in map(str.rstrip, sys.stdin):
        test_query(query.rstrip(" ;"))
