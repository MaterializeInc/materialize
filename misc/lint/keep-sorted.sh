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
# keep-sorted.sh — Checks that marked regions stay sorted.
# Pass --rewrite to sort marked regions.

set -euo pipefail

# shellcheck source=misc/shlib/shlib.bash
. "$(dirname "$0")/../shlib/shlib.bash"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <file>" >&2
    exit 1
fi

rewrite=false
errors=0

if [[ "${1:-}" = --rewrite ]]; then
    shift
    rewrite=true
fi

awk_prog='
/\/\/ keep-sorted-end/ {
    close("sort")
    p=1
}
p {print}
!p {print | "sort"}
/\/\/ keep-sorted-start/ {p=0}
'

for file in "$@"; do
    if [[ ! -f "$file" ]]; then
        echo "lint: $(red error:) keep-sorted: internal error: $file is not a file" >&2
        exit 1
    fi

    ret=0

    if $rewrite; then
        temp=$(mktemp)
        LC_ALL=C awk -v p=1 "$awk_prog" "$file" > "$temp"
        cmp --silent "$file" "$temp" || ret=$?
        if [ "$ret" -gt 1 ]; then
            echo "lint: $(red error:) cmp internal error $ret"
            exit "$ret"
        elif [ "$ret" -eq 1 ]; then
            echo "lint: Updating $file"
            mv -f "$temp" "$file"
        else
            unlink "$temp"
        fi
    else
        diff "$file" <(LC_ALL=C awk -v p=1 "$awk_prog" "$file") || ret=$?
        if [ "$ret" -gt 1 ]; then
            echo "lint: $(red error:) diff internal error $ret"
            exit "$ret"
        elif [ "$ret" -eq 1 ]; then
            echo "lint: $(red error:) keep-sorted blocks in $file are not correctly sorted"
            ((errors+=1))
        fi
    fi
done

if [ "$errors" -gt 0 ]; then
    exit 1
fi
