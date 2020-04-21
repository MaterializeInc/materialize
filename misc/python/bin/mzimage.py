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
    elif command in ["build", "run", "acquire", "fingerprint"]:
        if len(args) < 1:
            print_usage()
            return 1
        image_name = args.pop(0)
        repo = mzbuild.Repository(root)
        if image_name not in repo.images:
            print(f"fatal: unknown image: {image_name}", file=sys.stderr)
            return 1
        image = repo.images[image_name]
        deps = repo.resolve_dependencies([image])
        if command == "build":
            deps.acquire(force_build=True)
        elif command == "run":
            deps.acquire()
            image.run(args)
        elif command == "acquire":
            deps.acquire()
        elif command == "fingerprint":
            print(deps[-1].fingerprint())
    else:
        print(f"fatal: unknown command: {command}", file=sys.stderr)
        return 1
    return 0


def print_usage() -> None:
    print(
        "usage: mz-image <build|run|acquire|fingerprint> <image> [<args>...]",
        file=sys.stderr,
    )
    print("   or: mz-image list", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
