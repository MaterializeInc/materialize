# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzimage.py — builds Materialize-specific Docker images.

from materialize import mzbuild
from pathlib import Path
from typing import List
import os
import sys


def main(args: List[str]) -> int:
    if len(args) == 0:
        print_usage()
        return 1

    root = Path(os.environ["MZ_ROOT"])
    command = args.pop(0)
    if command == "list":
        repo = mzbuild.Repository(root)
        for image in repo:
            print(image.name)
    elif command in ["build", "run", "acquire", "fingerprint", "describe"]:
        if len(args) < 1:
            print_usage()
            return 1
        image_name = args.pop(0)
        repo = mzbuild.Repository(root)
        if image_name not in repo.images:
            print(f"fatal: unknown image: {image_name}", file=sys.stderr)
            return 1
        deps = repo.resolve_dependencies([repo.images[image_name]])
        rimage = deps[image_name]
        if command == "build":
            deps.acquire(force_build=True)
        elif command == "run":
            deps.acquire()
            rimage.run(args)
        elif command == "acquire":
            deps.acquire()
        elif command == "fingerprint":
            print(rimage.fingerprint())
        elif command == "describe":
            print(f"Image: {rimage.name}")
            print(f"Fingerprint: {rimage.fingerprint()}")
            print("Inputs:")
            for inp in rimage.inputs():
                print(f"    {inp.decode()}")
            print("Dependencies:")
            for d in rimage.dependencies:
                print(f"    {d}")
            if not rimage.dependencies:
                print("    (none)")
    elif command in ["-h", "--help"]:
        print_usage()
        return 0
    else:
        print(f"fatal: unknown command: {command}", file=sys.stderr)
        return 1
    return 0


def print_usage() -> None:
    print(
        "usage: mz-image <build|run|acquire|fingerprint|describe> <image> [<args>...]",
        file=sys.stderr,
    )
    print("   or: mz-image list", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
