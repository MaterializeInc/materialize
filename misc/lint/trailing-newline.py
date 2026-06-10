#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# trailing-newline.py — checks files for missing trailing newlines.

import os
import sys

errors = 0
for path in sys.argv[1:]:
    if not os.path.isfile(path):
        print(
            f"lint: \033[31merror:\033[0m trailing-newline: internal error: {path} is not a file",
            file=sys.stderr,
        )
        sys.exit(1)
    if os.path.getsize(path) == 0:
        continue
    with open(path, "rb") as f:
        f.seek(-1, 2)
        if f.read(1) != b"\n":
            print(
                f"lint: \033[31merror:\033[0m trailing-newline: {path} is missing a trailing newline",
                file=sys.stderr,
            )
            errors += 1

sys.exit(1 if errors else 0)
