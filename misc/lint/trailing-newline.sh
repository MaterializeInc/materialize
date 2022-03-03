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
# trailing-newline.sh — checks file for missing trailing newline.

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
        echo "lint: $(red error:) trailing-newline: internal error: $file is not a file" >&2
        exit 1
    fi

    last_byte=$(tail -c1 "$file")
    if [[ "$last_byte" != $'\n' && "$last_byte" != "" ]] &> /dev/null; then
        echo "lint: $(red error:) trailing-newline: $file is missing a trailing newline" >&2
        ((errors++))
    fi
done

if [[ $errors -gt 0 ]]; then
        exit 1
fi
