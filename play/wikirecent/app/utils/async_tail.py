#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""async_tail

Example utility that streams a VIEW using the TAIL command provided by materialized. Useful for
debugging / viewing the data transferred between materialized and the Python web server.

Can also be used as a very simple example for how to use psycopg3 in an asynchronous context.
"""

import argparse
import asyncio

import psycopg3


async def tail_view(args):
    """Continuously print changes to a Materialize View."""
    dsn = f"postgresql://{args.host}:{args.port}/materialize"
    async with await psycopg3.AsyncConnection.connect(dsn) as conn:
        async with await conn.cursor() as cursor:
            cursor_name = f"{args.view}_tail_cursor"
            query = f"DECLARE {cursor_name} CURSOR FOR TAIL {args.view} WITH (PROGRESS)"
            await cursor.execute(query)
            await cursor.execute(f"FETCH ALL {cursor_name}")

            while 1:

                row = await cursor.fetchone()
                if not row:
                    await cursor.execute(f"FETCH ALL {cursor_name}")
                    continue

                (timestamp, progressed, diff, *columns) = row
                print(f"{timestamp} {progressed} {diff} {columns}")


def main():
    """Parse arguments and run tail query."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", help="Materialize hostname", default="materialized", type=str
    )
    parser.add_argument(
        "-p", "--port", help="Materialize port number", default=6875, type=int
    )

    parser.add_argument("view", help="Name of the view to TAIL", type=str)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tail_view(args))
    loop.close()


if __name__ == "__main__":
    main()
