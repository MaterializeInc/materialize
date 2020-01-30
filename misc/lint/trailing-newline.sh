#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# trailing-newline.sh — checks file for missing trailing newline.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <file>" >&2
    exit 1
fi

for file in "$@"; do
    if [[ ! -f "$file" ]]; then
        echo "lint: trailing-newline: internal error: $file is not a file" >&2
        exit 1
    fi

    last_byte=$(tail -c1 "$file")
    if [[ "$last_byte" != $'\n' && "$last_byte" != "" ]] &> /dev/null; then
        echo "lint: trailing-newline: $file is missing a trailing newline" >&2
        exit 1
    fi
done
