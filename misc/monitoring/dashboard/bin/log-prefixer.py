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
import threading
import sys
from typing import Dict


def main() -> None:
    args = parse_args()
    echo_files_with_prefixes({p: f for p, f in args.prefix_file})


def echo_files_with_prefixes(prefixes_files: Dict[str, str]) -> None:
    """Start as many threads as there are files in prefixes_files, writing their output
    """
    max_prefix_len = max(len(prefix) for prefix in prefixes_files)
    threads = []
    for prefix, fname in prefixes_files.items():
        pad = " " * (max_prefix_len - len(prefix))
        thread = threading.Thread(
            target=follow_file, args=(f"{prefix}", f"{pad} >", fname)
        )
        thread.start()
        threads.append(thread)

    for i, thread in enumerate(threads):
        try:
            thread.join()
        except KeyboardInterrupt:
            return


def follow_file(prefix: str, pad: str, fname: str) -> None:
    error_count = 0
    full_pre = prefix + pad
    while True:
        try:
            with open(fname) as fh:
                for line in fh:
                    print(full_pre, line, end="")
            log(f"reached end of input for {prefix} {fname}")
            return
        except FileNotFoundError:
            if error_count % 100 == 0:
                print(f"Error opening {prefix} {fname} for reading")
            time.sleep(0.5)
            error_count += 1
        except KeyboardInterrupt:
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print all lines from files with different prefixes"
    )
    parser.add_argument(
        "prefix_file", help="prefix=/the/file/to/read", nargs="*",
    )
    args = parser.parse_args()
    print(args.prefix_file)
    args.prefix_file = [pf.split("=", 1) for pf in args.prefix_file]
    if any(len(pf) != 2 for pf in args.prefix_file):
        print(
            "Every prefix file must include a prefix, followed by the file, got {}".format(
                " and ".join([pf for pf in args.prefix_file if len(pf) != 2])
            )
        )
    return args


def log(msg: str, *args: str):
    print("prefixer>", msg.format(*args), file=sys.stderr)


if __name__ == "__main__":
    main()
