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
# test-attribute.py — checks usage of test/tokio::test attributes.
#
# We should favor mz_ore::test instead since it automatically initializes
# logging, see also
# https://github.com/MaterializeInc/materialize/blob/main/doc/developer/style.md

import re
import sys

RED = "\033[31m"
RESET = "\033[0m"

ALLOW_RE = re.compile(r"//\s*allow\(test-attribute\)")

errors = 0
for path in sys.argv[1:]:
    has_test = False
    has_tokio_test = False
    with open(path, errors="replace") as f:
        for line in f:
            if "#[test]" in line and not ALLOW_RE.search(line):
                has_test = True
            if "#[tokio::test" in line and not ALLOW_RE.search(line):
                has_tokio_test = True
            if has_test and has_tokio_test:
                break
    if has_test:
        print(
            f"lint: {RED}error:{RESET} test-attribute: {path}: use of disallowed `#[test]` attribute. Use the `#[mz_ore::test]` attribute instead or add a `// allow(test-attribute)` comment",
            file=sys.stderr,
        )
        errors += 1
    if has_tokio_test:
        print(
            f"lint: {RED}error:{RESET} test-attribute: {path}: use of disallowed `#[tokio::test] attribute. Use the `#[mz_ore::test(tokio::test)]` attribute instead or add a `// allow(test-attribute)` comment",
            file=sys.stderr,
        )
        errors += 1

sys.exit(1 if errors else 0)
