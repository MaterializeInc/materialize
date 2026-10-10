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
# allow-dead-code.py — rejects `allow(dead_code)` and `allow(unused)`
# attributes.
#
# An `allow` silently outlives the dead code it was added for. Use
# `#[expect(dead_code)]` instead, which turns into an
# `unfulfilled_lint_expectations` warning once the code is used, so the attribute
# gets removed. The `unused` lint group includes `dead_code`.
#
# Where an `expect` cannot work, for example in a macro whose expansions differ
# in what they use, add a `// allow(allow-dead-code)` comment on the same line.

import re
import sys

RED = "\033[31m"
RESET = "\033[0m"

# Matches `allow(...)` lint lists, including multi-line ones and the inner
# `allow` of `cfg_attr(..., allow(...))`.
ALLOW_RE = re.compile(r"\ballow\s*\(([^()]*)\)")
DEAD_CODE_RE = re.compile(r"\b(dead_code|unused)\b")
EXEMPT_RE = re.compile(r"//\s*allow\(allow-dead-code\)")

errors = 0
for path in sys.argv[1:]:
    with open(path, errors="replace") as f:
        contents = f.read()
    lines = contents.split("\n")
    for m in ALLOW_RE.finditer(contents):
        if DEAD_CODE_RE.search(m.group(1)):
            line = contents.count("\n", 0, m.start()) + 1
            if EXEMPT_RE.search(lines[line - 1]):
                continue
            print(
                f"lint: {RED}error:{RESET} allow-dead-code: {path}:{line}: use of disallowed `allow(dead_code)` or `allow(unused)`. Use `#[expect(dead_code)]` instead, or add a `// allow(allow-dead-code)` comment",
                file=sys.stderr,
            )
            errors += 1

sys.exit(1 if errors else 0)
