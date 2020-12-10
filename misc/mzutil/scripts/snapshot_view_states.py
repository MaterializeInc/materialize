#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""snapshot_view_states

Script to query all materialized views in a Materialize database and persist a point in time copy
of each view to files in the snapshots directory.

Best used when no data is running through the system.
"""

import argparse
import os
import typing

import psycopg2  # type: ignore
import psycopg2.extensions  # type: ignore


def view_names(
    conn: psycopg2.extensions.connection,
) -> typing.Generator[str, None, None]:
    """Return a generator containing all view names in Materialize."""
    with conn.cursor() as cursor:
        cursor.execute("SHOW VIEWS")
        for row in cursor:
            yield row[0]


def snapshot_materialize_views(args: argparse.Namespace) -> None:
    """Record the current table status of all views installed in Materialize."""

    with psycopg2.connect(f"postgresql://{args.host}:{args.port}/materialize") as conn:
        for view in view_names(conn):
            with conn.cursor() as cursor:
                viewfile = os.path.join(args.snapshot_dir, f"{view}.sql")
                with open(viewfile, "w") as outfile:
                    query = f"COPY (SELECT * FROM {view}) TO STDOUT"
                    cursor.copy_expert(query, outfile)


def main() -> None:
    """Parse arguments and snapshot materialized views."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", help="materialized hostname", default="materialized", type=str
    )
    parser.add_argument(
        "-p", "--port", help="materialized port number", default=6875, type=int
    )

    parser.add_argument(
        "-d",
        "--snapshot-dir",
        help="Directory containing view snapshots",
        type=str,
        default="/snapshot",
    )

    args = parser.parse_args()
    snapshot_materialize_views(args)


if __name__ == "__main__":
    main()
