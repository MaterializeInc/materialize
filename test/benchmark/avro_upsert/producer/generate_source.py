#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
import random


def generate_rows(args: argparse.Namespace) -> None:
    """Generate the random dataset, writing to the desired output file."""

    for _ in range(0, args.num_rows):
        book_id = random.randint(1, args.num_book_ids)
        security_id = random.randint(1, args.num_security_ids)
        exposure1 = book_id + security_id + 100000 + random.randint(0, 32767)
        exposure2 = exposure1 + random.randint(0, 32767)
        exposure3 = exposure1 + random.randint(0, 32767)
        exposure4 = exposure1 + random.randint(0, 32767)

        key = f"{book_id}-{security_id}"

        value = json.dumps(
            {
                "BookId": book_id,
                "SecurityId": security_id,
                "Exposure": {
                    "Current": {
                        "Long2": {"Exposure": exposure1},
                        "Short2": {"Exposure": exposure2},
                    },
                    "Target": {
                        "Long": {"Exposure": exposure3},
                        "Short": {"Exposure": exposure4},
                    },
                },
            }
        )

        args.output.write(f"{key}:{value}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b",
        "--num-book-ids",
        type=int,
        default=100,
        help="Number of unique book IDs to include in the dataset",
    )
    parser.add_argument(
        "-n",
        "--num-rows",
        type=int,
        default=10000000,
        help="Number of rows to generate for the initial dataset",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default="-",
        help="Where to write results to (defaults to stdout)",
    )
    parser.add_argument(
        "-s",
        "--num-security-ids",
        type=int,
        default=32000,
        help="Number of unique security IDs to include in the dataset",
    )
    args = parser.parse_args()
    generate_rows(args)
