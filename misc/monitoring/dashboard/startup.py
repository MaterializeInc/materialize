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
import os
import shlex


def main() -> None:
    args = parse_args()
    mz_url = args.materialized_url
    print(f"namespace: {args}")
    print(f"namespace: {vars(args)}")
    for fname in args.materialize_files:
        with open(fname) as fh:
            contents = fh.read()
        contents = contents.replace("MATERIALIZED_URL", args.materialized_url)
        with open(fname, "w") as fh:
            fh.write(contents)
            if os.getenv("MZ_LOG", "info") in ["debug"]:
                print(f"New file {fname}:\n{contents}")

    cmd = shlex.split(args.exec)
    os.execv(cmd[0], cmd)


def parse_args() -> argparse.Namespace:
    args = argparse.ArgumentParser()
    args.add_argument(
        "-u",
        "--materialized-url",
        default=os.getenv("MATERIALIZED_URL", "materialized:6875"),
    )
    args.add_argument(
        "--exec",
        help="What to execute afterwards",
        default="/bin/supervisord -c /supervisord/supervisord.conf",
    )
    args.add_argument(
        "materialize_files",
        metavar="MATERIALIZE-FILES",
        nargs="+",
        help="Which files to replace MATERIALIZED_URL in",
    )

    return args.parse_args()


if __name__ == "__main__":
    main()
