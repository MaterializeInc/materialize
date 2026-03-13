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
# copyright.py — checks files for missing or malformatted copyright headers.

import re
import sys

RED = "\033[31m"
RESET = "\033[0m"

errors = 0
for path in sys.argv[1:]:
    try:
        with open(path, errors="replace") as f:
            copyright_line = None
            copyright_text = None
            found_non_comment = False
            for i, line in enumerate(f, 1):
                # Skip shebang
                if i == 1 and line.startswith("#!") and "/" in line:
                    continue
                # Skip blank lines
                if line.strip() == "":
                    continue
                # Skip lines containing {}[]" chars (JSON, templates)
                if re.search(r'[{}\[\]"]', line):
                    continue
                # Check for copyright line
                if re.match(r'^(//|#|--|\s+"#|;)?.*Copyright', line):
                    copyright_line = i
                    copyright_text = line
                    continue
                # If line is not a comment, we're past the header
                if not re.match(r"^(<!--|<\?xml|//|#|--|;)", line):
                    found_non_comment = True
                    break

            # Match awk behavior: only report errors if we actually
            # encountered a non-comment line (empty files and
            # all-comment files are silently accepted).
            if not found_non_comment:
                continue

            if copyright_text is None:
                print(
                    f"lint: {RED}error:{RESET} copyright: {path} is missing copyright header",
                    file=sys.stderr,
                )
                errors += 1
            elif "Copyright Materialize, Inc. and contributors." not in copyright_text:
                print(
                    f"lint: {RED}error:{RESET} copyright: {path} has malformatted copyright header",
                    file=sys.stderr,
                )
                print(
                    f'hint: line {copyright_line} does not include the exact text "Copyright Materialize, Inc. and contributors."',
                    file=sys.stderr,
                )
                errors += 1
    except Exception as e:
        print(
            f"lint: {RED}error:{RESET} copyright: {path}: {e}",
            file=sys.stderr,
        )
        errors += 1

sys.exit(1 if errors else 0)
