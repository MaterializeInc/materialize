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
import pathlib
import subprocess


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

    pipes = args.create_pipes.split(",")
    for p_fname in pipes:
        _, fname = p_fname.split("=", 1)
        f = pathlib.Path(fname)
        if f.exists():
            if not f.is_fifo():
                print(
                    f"WARNING: {f} already exists but is not a FIFO, trying to delete it"
                )
                f.unlink()
        if not f.exists():
            os.mkfifo(f)
            print(f"startup: created fifo {f}")

    # Note that unfortunately we can't create use log-prefixer as a supervisord-managed
    # process because supervisord tries to open files for writing I suppose a bit earlier
    # than it tries to start the highest-priority program, and pipes block their host
    # when you try to write to them if there is no reader.
    #
    # So we need to spawn off a thread before supervisord has a chance to get itself
    # messed up. Possibly this is related to
    # https://github.com/Supervisor/supervisor/issues/122
    subprocess.Popen(["/bin/log-prefixer.py"] + pipes)

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
        "--create-pipes", help="comma-separated list of files to create", default=""
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
