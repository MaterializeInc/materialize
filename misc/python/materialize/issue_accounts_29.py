#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# pyactivate — runs a script in the Materialize Python virtualenv.

import sys
from textwrap import dedent

SCHEMA = "accounts_29"


def init_schema(num_cols: int) -> str:
    return dedent(
        f"""
        DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
        CREATE SCHEMA {SCHEMA};
        SET SCHEMA = {SCHEMA};

        CREATE TABLE t1({", ".join((f"f{i:03} int" for i in range(num_cols)))}, read_time int not null);
        """
    ).lstrip("\n")


def init_view_def(num_exprs: int) -> str:
    expressions = ",\n            ".join(
        (
            f"COALESCE(f{i:03}, lag(f{i:03}, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f{i:03}_filled"
            for i in range(num_exprs)
        )
    )

    return dedent(
        f"""
        CREATE OR REPLACE MATERIALIZED VIEW materialize.{SCHEMA}.test_view AS
        SELECT
            {expressions}
        FROM 
            materialize.{SCHEMA}.t1;
        """
    ).lstrip("\n")


def main(args: list[str]) -> int:
    if len(args) != 2:
        print("USAGE: <prog> [schema|query] n", file=sys.stderr)
        return 1

    if str(args[0]) == "schema":
        print(init_schema(int(args[1])))
        return 0
    elif str(args[0]) == "query":
        print(init_view_def(int(args[1])))
        return 0
    else:
        print("USAGE: <prog> [schema|query] n", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
