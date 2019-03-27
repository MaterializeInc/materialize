#!/usr/bin/env bash

# Copyright 2019 Timely Data, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Timely Data, Inc.
#
# test.sh — runs tests and lints in CI.

set -euo pipefail

passed=0
total=0

try() {
    # Start a collapsed-by-default log section.
    echo "--- $@"

    # Try the command.
    if "$@"; then
        ((++passed))
    else
        # The command failed. Tell Buildkite to uncollapse this log section, so
        # that the errors are immediately visible.
        echo "^^^ +++"
    fi
    ((++total))
}

try bin/lint
try bin/check
try cargo fmt -- --check
try cargo test

echo "$passed/$total commands passed"
if ((passed != total)); then
    exit 1
fi
