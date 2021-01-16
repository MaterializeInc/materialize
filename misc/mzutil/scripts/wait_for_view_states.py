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
import json
import os
import sys
import pathlib
import time
import toml
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


class ViewNotReady(Exception):
    pass


def view_contents(cursor: psycopg2.extensions.cursor, view: str, timestamp: int) -> str:
    """Return True if a SELECT from the VIEW matches the expected string."""
    stream = io.StringIO()
    query = f"COPY (SELECT * FROM {view} WHERE mz_logical_timestamp() > {timestamp}) TO STDOUT"
    try:
        cursor.copy_expert(query, stream)
    except psycopg2.errors.InternalError_:
        # The view is not yet ready to be queried
        raise ViewNotReady()
    return stream.getvalue().strip()


class SourceInfo:
    """Container class containing information about a source."""

    def __init__(self, topic_name: str, offset: int):
        self.topic_name = topic_name
        self.offset = offset


def source_at_offset(
    cursor: psycopg2.extensions.cursor, source_info: SourceInfo
) -> typing.Union[None, int]:
    """Return the mz timestamp from a source if it has reached the desired offset."""
    query = (
        'SELECT timestamp FROM mz_source_info WHERE source_name = %s and "offset" = %s'
    )
    try:
        cursor.execute(query, (source_info.topic_name, source_info.offset))
        if cursor.rowcount > 1:
            print("ERROR: More than one row returned when querying source offsets:")
            for row in cursor:
                print(f"\t{row}")
            sys.exit(1)
        if not cursor.rowcount:
            return None

        return int(cursor.fetchone()[0])
    except psycopg2.errors.InternalError_:
        # The view is not yet ready to be queried
        return None


def wait_for_materialize_views(args: argparse.Namespace) -> None:
    """Record the current table status of all views installed in Materialize."""

    start_time = time.monotonic()

    # Create a dictionary mapping view names (as calculated from the filename) to expected contents
    view_snapshots: typing.Dict[str, str] = {
        p.stem: p.read_text().strip()
        for p in pathlib.Path(args.snapshot_dir).glob("*.sql")
    }

    source_offsets: typing.Dict[str, int] = {
        p.stem: int(p.read_text().strip())
        for p in pathlib.Path(args.snapshot_dir).glob("*.offset")
    }

    # Create a dictionary mapping view names to source name and offset
    view_sources: typing.Dict[str, SourceInfo] = {}
    with open(os.path.join(args.snapshot_dir, "config.toml")) as fd:
        conf = toml.load(fd)

        if len(conf["sources"]) != 1:
            print(f"ERROR: Expected just one source block: {conf['sources']}")
            sys.exit(1)

        source_info = conf["sources"][0]
        topic_prefix: str = source_info["topic_namespace"]
        source_names: typing.List[str] = source_info["names"]

        for query_info in conf["queries"]:

            # Ignore views not in this snapshot (they likely have multiple sources...)
            view: str = query_info["name"]
            if view not in view_snapshots:
                continue

            sources: typing.List[str] = query_info["sources"]
            if len(query_info["sources"]) != 1:
                print(
                    f"ERROR: Expected just one source for view {view}: {query_info['sources']}"
                )
                sys.exit(1)

            source_name: str = query_info["sources"][0]
            if source_name not in source_name:
                print(
                    f"ERROR: No matching source {source_name} for view {view}: {source_names}"
                )
                sys.exit(1)

            topic_name = f"{topic_prefix}{source_name}"
            if topic_name not in source_offsets:
                print(
                    f"ERROR: Missing offset information for source {topic_name}: {source_offsets}"
                )
                sys.exit(1)

            view_sources[view] = SourceInfo(topic_name, source_offsets[topic_name])

    with psycopg2.connect(f"postgresql://{args.host}:{args.port}/materialize") as conn:
        installed_views = set(view_names(conn))

    # Verify that we have snapshots for all views installed
    captured_views = set(view_snapshots.keys())
    if not captured_views.issuperset(installed_views):
        missing_views = installed_views.difference(captured_views)
        print(f"ERROR: Missing final state for views: {missing_views}")
        print(f"       Have: {captured_views}")
        sys.exit(1)

    print("Recording time required until each view matches its snapshot")

    pending_views = installed_views
    with psycopg2.connect(f"postgresql://{args.host}:{args.port}/materialize") as conn:
        while pending_views:
            views_to_remove = []
            time_taken = time.monotonic() - start_time
            for view in pending_views:
                with conn.cursor() as cursor:

                    # Determine if the source is at the desired offset and identify the
                    # mz_logical_timestamp associated with the offset
                    timestamp = source_at_offset(cursor, view_sources[view])
                    if not timestamp:
                        continue

                    # Get the contents of the view at the desired timestamp, where an empty result
                    # implies that the desired timestamp is not yet incorporated into the view
                    try:
                        contents = view_contents(cursor, view, timestamp)
                        if not contents:
                            continue
                    except ViewNotReady:
                        continue

                    views_to_remove.append(view)
                    expected = view_snapshots[view]
                    if contents == expected:
                        print(
                            f"PASSED: {time_taken:>6.1f}s: {view} (result={contents})"
                        )
                    else:
                        print(
                            f"FAILED: {time_taken:>6.1f}s: {view} ({contents} != {expected})"
                        )

            for view in views_to_remove:
                pending_views.remove(view)

            if pending_views:
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
