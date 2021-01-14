#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""capture_scheduling_info

Script to periodically query internal scheduling information from Materialize and write out the
results to an AVRO OCF file, keyed by timestamp. From these metrics, we can turn scheduling
information into timeseries data.
"""

import argparse
import functools
import multiprocessing
import pathlib
import sys
import time
import typing
import uuid

import avro.datafile
import avro.io
import avro.schema
import psycopg2
import psycopg2.extras

def main() -> None:
    """Parse arguments and start polling performance data from materialized."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", help="materialized hostname", default="materialized", type=str
    )

    parser.add_argument(
        "-p", "--port", help="materialized port number", default=6875, type=int
    )

    parser.add_argument(
        "-q",
        "--queries-dir",
        help="Directory that contains the definition of queries to run",
        type=str,
        default="queries",
    )

    parser.add_argument(
        "-o",
        "--out-directory",
        help="Directory to write scheduling metrics into",
        type=str,
        default="metrics",
    )

    args = parser.parse_args()
    capture_scheduling_info(args)

def capture_scheduling_info(args: argparse.Namespace) -> None:
    """Loop forever, recording materialized scheduling statistics to local files."""

    trace_directory = pathlib.Path(args.out_directory)
    trace_directory.mkdir(parents=True, exist_ok=True)

    # TODO: Make this an argument to the script
    queries = ["time_per_operator", "time_per_worker", "time_per_operator_per_worker"]
    with multiprocessing.Pool(len(queries)) as pool:
        capture = functools.partial(capture_query, args, trace_directory)
        pool.map(capture, queries)


def capture_query(args: argparse.Namespace, trace_directory: pathlib.Path, query_name: str) -> None:
    """Periodically run the desired query and write the results to the AVRO OCF."""
    try:
        capture_query_inner(args, trace_directory, query_name)
    except Exception as e:
        print(f"Oh no! Failed to query {query_name}: {e}")
        raise


def capture_query_inner(args: argparse.Namespace, trace_directory: pathlib.Path, query_name: str) -> None:
    """Periodically run the desired query and write the results to the AVRO OCF.

    Sample code to read the results:

        with open(outfile, "rb") as ocf_file:
            with avro.datafile.DataFileReader(ocf_file, avro.io.DatumReader()) as reader:
                for row in reader:
                    print(row)
    """

    outfile = pathlib.Path(trace_directory, f"{query_name}.avro")
    query = pathlib.Path(args.queries_dir, f"{query_name}.sql").read_text().strip()

    if not outfile.exists():
        schema = avro.schema.parse(pathlib.Path(args.queries_dir, f"{query_name}.avsc").read_text())
    else:
        schema = None ## Use existing schema from file

    print(f"Running {query_name} and writing to {outfile}")
    dsn = f"postgresql://{args.host}:{args.port}/materialize"

    while 1:
        try:
            capture_query_loop(args, dsn, query, outfile, schema)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            schema = None
            time.sleep(1)


def capture_query_loop(args: argparse.Namespace, dsn: str, query: str, outfile: pathlib.Path,
        schema: typing.Union[None, avro.schema.Schema]) -> None:
    """Periodically run the desired query and write the results to the AVRO OCF.

    Raises an error if the connection is dropped.
    """
    with psycopg2.connect(dsn) as conn, open(outfile, "ab+") as ocf_file:
        with avro.datafile.DataFileWriter(ocf_file, avro.io.DatumWriter(), schema) as writer:
            while 1:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(query)
                    now = time.time()
                    for row in cursor:
                        writer.append(row)
                    writer.flush()
                time.sleep(1)


if __name__ == '__main__':
    main()
