#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path
import mzbuild
import os

def main() -> None:
    repo = mzbuild.Repository(Path("."))
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire(push=True)

if __name__ == "__main__":
    main()
