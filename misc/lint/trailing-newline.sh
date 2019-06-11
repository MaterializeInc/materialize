#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# trailing-newline.sh — checks file for missing trailing newline.

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <file>" >&2
    exit 1
fi

if [[ ! -f "$1" ]]; then
    echo "lint: trailing-newline: internal error: $1 is not a file" >&2
    exit 1
fi

last_byte=$(tail -c1 "$1")
if [[ "$last_byte" != $'\n' && "$last_byte" != "" ]] &> /dev/null; then
    echo "lint: trailing-newline: $1 is missing a trailing newline" >&2
    exit 1
fi
