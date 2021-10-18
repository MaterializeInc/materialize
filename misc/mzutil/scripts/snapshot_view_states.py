#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
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
import sys
import typing

import psycopg2
import psycopg2.extensions


def view_names(
    conn: psycopg2.extensions.connection,
) -> typing.Generator[str, None, None]:
    """Return a generator containing all view names in Materialize."""
    with conn.cursor() as cursor:
        cursor.execute("SHOW VIEWS")
        for row in cursor:
            yield row[0]


def source_names(
    conn: psycopg2.extensions.connection,
) -> typing.Generator[str, None, None]:
    """Return a generator containing all sources in Materialize."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT source_name FROM mz_source_info")
        for row in cursor:
            yield row[0]


def snapshot_materialize_views(args: argparse.Namespace) -> None:
    """Record the current table status of all views installed in Materialize."""

    with psycopg2.connect(
        f"postgresql://materialize@{args.host}:{args.port}/materialize"
    ) as conn:
        conn.autocommit = True
        for view in view_names(conn):
            with conn.cursor() as cursor:
                viewfile = os.path.join(args.snapshot_dir, f"{view}.sql")
                with open(viewfile, "w") as outfile:
                    query = f"COPY (SELECT * FROM {view}) TO STDOUT"
                    cursor.copy_expert(query, outfile)


def snapshot_source_offsets(args: argparse.Namespace) -> None:
    """Record the current topic offset of all sources installed in Materialize."""

    with psycopg2.connect(
        f"postgresql://materialize@{args.host}:{args.port}/materialize"
    ) as conn:
        conn.autocommit = True
        for source in source_names(conn):
            with conn.cursor() as cursor:
                query = "SELECT mz_source_info.offset as offset FROM mz_source_info WHERE source_name = %s"
                cursor.execute(query, (source,))

                if cursor.rowcount != 1:
                    print(f"ERROR: Expected one row for {source}: {cursor.fetchall()}")
                    sys.exit(1)

                viewfile = os.path.join(args.snapshot_dir, f"{source}.offset")
                with open(viewfile, "w") as outfile:
                    offset = cursor.fetchone()[0]
                    outfile.write(f"{offset}")


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
    snapshot_source_offsets(args)


if __name__ == "__main__":
    main()
