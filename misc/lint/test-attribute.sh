#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# test-attribute.sh - checks usage of test/tokio::test attributes
#
# We should favor mz_ore::test instead since it automatically initializes
# logging, see also
# https://github.com/MaterializeInc/materialize/blob/main/doc/developer/style.md
# Checking this in clippy is unfortunately not possible since the mz_ore::test
# macro's internal usage of test/tokio::test would cause warnings even on
# proper use.

set -euo pipefail

# shellcheck source=misc/shlib/shlib.bash
. "$(dirname "$0")/../shlib/shlib.bash"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <file>" >&2
    exit 1
fi
errors=0

for file in "$@"; do
    if [[ ! -f "$file" ]]; then
        echo "lint: $(red error:) test-attribute: internal error: $file is not a file" >&2
        exit 1
    fi

    if grep "#\[test\]" "$file" | grep -qvE "//\s*allow\(test-attribute\)"; then
        echo "lint: $(red error:) test-attribute: $file: use of disallowed \`#[test]\` attribute. Use the \`#[mz_ore::test]\` attribute instead or add a \`// allow(test-attribute)\` comment" >&2
        ((errors++))
    fi
    if grep "#\[tokio::test" "$file" | grep -qvE "//\s*allow\(test-attribute\)"; then
        echo "lint: $(red error:) test-attribute: $file: use of disallowed \`#[tokio::test] attribute. Use the \`#[mz_ore::test(tokio::test)]\` attribute instead or add a \`// allow(test-attribute)\` comment" >&2
        ((errors++))
    fi
done

if [[ $errors -gt 0 ]]; then
        exit 1
fi
