#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""wait_for_view_states

Script to query all materialized views in a Materialize database and wait until each view matches
the exact output as captured in the snapshot files.

Prints timing information to indicate how long each view took to reach the desired state.
"""

import argparse
import glob
import io
import os
import pathlib
import time
import typing

import psycopg2  # type: ignore
import psycopg2.errors  # type: ignore
import psycopg2.extensions  # type: ignore


def view_names(
    conn: psycopg2.extensions.connection,
) -> typing.Generator[str, None, None]:
    """Return a generator containing all view names in Materialize."""
    with conn.cursor() as cursor:
        cursor.execute("SHOW VIEWS")
        for row in cursor:
            yield row[0]


def view_matches(cursor: psycopg2.extensions.cursor, view: str, expected: str) -> bool:
    """Return True if a SELECT from the VIEW matches the expected string."""
    stream = io.StringIO()
    try:
        cursor.copy_expert(f"COPY (SELECT * FROM {view}) TO STDOUT", stream)
    except psycopg2.errors.InternalError_:
        # The view is not yet ready to be queried
        return False
    return stream.getvalue() == expected


def wait_for_materialize_views(args: argparse.Namespace) -> None:
    """Record the current table status of all views installed in Materialize."""

    start_time = time.time()

    # Create a dictionary mapping view names (as calculated from the filename) to expected contents
    view_snapshots = {
        p.stem: p.read_text() for p in pathlib.Path(args.snapshot_dir).glob("*.sql")
    }

    with psycopg2.connect(f"postgresql://{args.host}:{args.port}/materialize") as conn:
        installed_views = list(view_names(conn))

    assert sorted(view_snapshots.keys()) == sorted(
        installed_views
    ), "Installed views do not match snapshot views"
    print("Recording time required until each view matches its snapshot")

    with psycopg2.connect(f"postgresql://{args.host}:{args.port}/materialize") as conn:
        while view_snapshots:
            views_to_remove = []
            for (view, contents) in view_snapshots.items():
                with conn.cursor() as cursor:
                    if view_matches(cursor, view, contents):
                        time_taken = time.time() - start_time
                        print(f"{time_taken:>6.1f}s: {view}")
                        views_to_remove.append(view)

            for view in views_to_remove:
                del view_snapshots[view]

            if view_snapshots:
                # Our queries should be very fast, use a fast timer
                time.sleep(0.1)


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
    wait_for_materialize_views(args)


if __name__ == "__main__":
    main()
